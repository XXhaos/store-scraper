from __future__ import annotations
import asyncio
import json
import re
from urllib.parse import quote_plus
from dataclasses import dataclass, field
from typing import AsyncIterator, Dict, Any, List, Optional

from catalog.adapters.base import Adapter, AdapterConfig, Capabilities
from catalog.models import GameRecord
from catalog.normalize import (
   clean_title,
   strip_edition_noise,
   price_to_string,
   normalize_platforms,
   normalize_rating,
)
from catalog.http import DomainLimiter

# Nintendo's public surface varies by region and frequently embeds data in the page.
# Strategy:
#  1) (Optional) Use a JSON endpoint if you have a stable one for your region.
#  2) Fallback: Parse listing pages and extract product arrays from:
#        - __NEXT_DATA__ (Next.js)
#        - application/ld+json (JSON-LD)
#
# Provide a handful of "seed" listing pages that enumerate many titles.

NIN_LIMIT = DomainLimiter(2.0)

_JSONLD_RE = re.compile(
   r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
   re.S | re.I
)
_NEXT_RE = re.compile(
   r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
   re.S | re.I
)

_ASSET_HOST = "https://assets.nintendo.com"

def _normalize_asset_url(value: Optional[str]) -> Optional[str]:
   if not value:
      return value
   if isinstance(value, str):
      if value.startswith("http://") or value.startswith("https://"):
         return value
      if value.startswith("//"):
         return f"https:{value}"
      path = value.lstrip("/")
      if path.startswith("image/upload/"):
         return f"{_ASSET_HOST}/{path}"
      return f"{_ASSET_HOST}/image/upload/{path}"
   return value

def _extract_price_components(*values: Any) -> tuple[Optional[float], Optional[str], Optional[str]]:
   amount: Optional[float] = None
   currency: Optional[str] = None
   display: Optional[str] = None

   def visit(node: Any):
      nonlocal amount, currency, display
      if node is None or (amount is not None and currency is not None and display is not None):
         return
      if isinstance(node, bool):
         return
      if isinstance(node, (int, float)) and not isinstance(node, bool):
         if amount is None:
            amt = float(node)
            if amt > 1000:
               amt = amt / 100.0
            amount = amt
         return
      if isinstance(node, str):
         stripped = node.strip()
         if not stripped:
            return
         try:
            amt = float(stripped.replace("$", "").replace(",", ""))
         except ValueError:
            if display is None:
               display = stripped
         else:
            if amount is None:
               if amt > 1000:
                  amt = amt / 100.0
               amount = amt
         return
      if isinstance(node, dict):
         if currency is None:
            currency = node.get("currency") or node.get("currencyCode") or node.get("currency_symbol")
         if display is None:
            display = node.get("display") or node.get("formatted") or node.get("rawValue") or node.get("priceFormatted")
         for key in (
            "finalPrice", "salePrice", "discountPrice", "discounted", "regularPrice", "regPrice",
            "current", "amount", "raw", "value", "price", "msrp", "final", "basePrice", "usdValue",
         ):
            if key in node:
               visit(node.get(key))
         return
      if isinstance(node, (list, tuple, set)):
         for item in node:
            visit(item)

   for v in values:
      visit(v)

   return amount, currency, display

@dataclass(slots=True)
class NintendoEndpoints:
   # Example (fill if you’ve got one for your locale/market):
   # "https://www.nintendo.com/search/api?query={query}&count={count}&country={country}&locale={locale}&page={page}"
   search_api: Optional[str] = None
   algolia_app_id: Optional[str] = None
   algolia_api_key: Optional[str] = None
   algolia_index: Optional[str] = None
   algolia_filters: Optional[str] = None
   algolia_additional_params: Dict[str, Any] = field(default_factory=dict)

   # Listing pages that contain many products (per locale)
   seed_pages: Optional[List[str]] = None

def _default_seed_pages(country: str, locale: str) -> List[str]:
   # nintendo.com often uses paths like /en-us/store/games/
   # normalize locale to "en-us" style
   loc = locale.replace("_", "-").lower()
   # country sometimes appears separately (us, ca, etc.). When not used, locale usually suffices.
   base = f"https://www.nintendo.com/{loc}/store/games"
   # A small set of broad catalogs; add/remove as your region exposes
   return [
      base,                                   # all games
      f"{base}/?f=available-now",             # available now (filter example)
      f"{base}/?f=on-sale",                   # on sale
      f"{base}/?f=new-releases",              # new releases
      f"{base}/?f=coming-soon",               # coming soon
      # (If your region still uses legacy pages, you can add them here)
   ]

class NintendoAdapter(Adapter):
   store = "nintendo"
   capabilities = Capabilities(pagination=True, returns_partial_price=True)

   def __init__(self, *, config: AdapterConfig | None = None,
                endpoints: NintendoEndpoints | None = None, **kw):
      super().__init__(config=config, **kw)
      self.endpoints = endpoints or NintendoEndpoints(
         search_api="https://u3b6gr4ua3-1.algolianet.com/1/indexes/{index_name}/query",
         algolia_app_id="U3B6GR4UA3",
         algolia_api_key="a29c6927638bfd8cee23993e51e721c9",
         algolia_index="store_game_{locale}_{country}_release_des",
         algolia_filters="NOT \"contentDescriptors.label\":\"Partial Nudity\" AND NOT \"contentDescriptors.label\":\"Nudity\"",
         algolia_additional_params={
            "analytics": True,
            "facetingAfterDistinct": True,
            "clickAnalytics": True,
            "highlightPreTag": "^*^^",
            "highlightPostTag": "^*",
            "attributesToHighlight": ["description"],
            "facets": ["*"],
            "maxValuesPerFacet": 100,
         },
         seed_pages=_default_seed_pages(self.config.country, self.config.locale),
      )

   async def iter_games(self) -> AsyncIterator[GameRecord]:
      # Strategy A: JSON search (optional)
      if self.endpoints.search_api:
         for ch in "abcdefghijklmnopqrstuvwxyz":
            async for rec in self._iter_search_api(query=ch, page_size=60):
               if rec:
                  yield rec
            await asyncio.sleep(0.1)

      # Strategy B: Listing pages with embedded JSON
      for url in self.endpoints.seed_pages or []:
         async for rec in self._iter_list_page(url):
            if rec:
               yield rec
         await asyncio.sleep(0.2)

   # ---------- Strategy A: JSON search API (optional) ----------

   async def _iter_search_api(self, *, query: str, page_size: int = 60) -> AsyncIterator[Optional[GameRecord]]:
      assert self.endpoints.search_api, "search_api endpoint template not configured"

      headers = {"Accept": "application/json"}
      if self.endpoints.algolia_app_id:
         headers["X-Algolia-Application-Id"] = self.endpoints.algolia_app_id
      if self.endpoints.algolia_api_key:
         headers["X-Algolia-Api-Key"] = self.endpoints.algolia_api_key

      locale_underscore = self.config.locale.replace("-", "_").lower()
      country_lower = self.config.country.lower()
      index_template = self.endpoints.algolia_index or "ncom_game_en_{country}"
      index_name = index_template.format(
         country=country_lower,
         locale=locale_underscore,
      )
      if locale_underscore.endswith(f"_{country_lower}"):
         dup = f"_{country_lower}_{country_lower}"
         if dup in index_name:
            index_name = index_name.replace(dup, f"_{country_lower}")

      search_api = self.endpoints.search_api or ""
      if "{index_name}" in search_api or "{index}" in search_api:
         search_api = search_api.format(index=index_name, index_name=index_name)
      using_queries = search_api.endswith("/queries")

      page = 0
      while True:
         if using_queries:
            params = {
               "query": query,
               "hitsPerPage": str(page_size),
               "page": str(page),
            }
            payload = {
               "requests": [
                  {
                     "indexName": index_name,
                     "params": "&".join(f"{k}={quote_plus(v)}" for k, v in params.items()),
                  }
               ]
            }
            resp = await self.request("POST", search_api, json=payload, headers=headers)
            js = resp.json()
            results = (js.get("results") or [])
            if results:
               items = results[0].get("hits") or []
               nb_pages = results[0].get("nbPages")
            else:
               items = []
               nb_pages = None
         else:
            payload = {
               "query": query,
               "hitsPerPage": page_size,
               "page": page,
            }
            if self.endpoints.algolia_filters:
               payload["filters"] = self.endpoints.algolia_filters
            if self.endpoints.algolia_additional_params:
               payload.update(self.endpoints.algolia_additional_params)
            resp = await self.request("POST", search_api, json=payload, headers=headers)
            js = resp.json()
            items = js.get("hits") or []
            nb_pages = js.get("nbPages")

         count = 0
         for it in items:
            coalesced = self._coerce_algolia_hit(it)
            rec = self._normalize_api_item(coalesced)
            if rec:
               count += 1
               yield rec

         if nb_pages is not None:
            if page + 1 >= nb_pages:
               break
         if count < page_size:
            break
         page += 1
         await asyncio.sleep(0.05)

   def _extract_items_from_api(self, js: Dict[str, Any]) -> List[Dict[str, Any]]:
      if not isinstance(js, dict):
         return []
      # Common container keys
      for k in ("products", "items", "results"):
         v = js.get(k)
         if isinstance(v, list) and v:
            return v
      data = js.get("data")
      if isinstance(data, dict):
         for k in ("products", "items", "results"):
            v = data.get(k)
            if isinstance(v, list) and v:
               return v
      return []

   def _normalize_api_item(self, it: Dict[str, Any]) -> Optional[GameRecord]:
      # Titles often under "title", "name", or "productTitle"
      name = strip_edition_noise(clean_title(
         it.get("title") or it.get("name") or it.get("productTitle") or ""
      ))
      if not name:
         return None

      # Image fields: hero, boxArt, imageUrl, keyImages[]
      image = (
         it.get("image") or it.get("imageUrl") or it.get("boxArt") or it.get("heroBanner") or
         it.get("productImage") or it.get("productImageSquare")
      )
      if not image:
         imgs = it.get("images") or it.get("keyImages") or []
         if isinstance(imgs, list) and imgs:
            # prefer box art if tagged, else first
            preferred = None
            for img in imgs:
               if not isinstance(img, dict):
                  continue
               kind = (img.get("type") or img.get("purpose") or "").lower()
               if "box" in kind or "pack" in kind or "cover" in kind:
                  preferred = img.get("url")
                  if preferred:
                     break
            image = preferred or (imgs[0].get("url") if isinstance(imgs[0], dict) else imgs[0])
      image = _normalize_asset_url(str(image)) if image else "https://www.nintendo.com/etc.clientlibs/ncom/clientlibs/clientlib-ncom/resources/img/nintendo_red.svg"

      # Href: product page URL (or build from slug/nsuid)
      href = (
         it.get("productUrl") or it.get("url") or it.get("webUrl")
      )
      if not href:
         slug = it.get("slug") or it.get("seoName")
         nsuid = it.get("nsuid") or it.get("id")
         loc = self.config.locale.replace("_", "-").lower()
         if slug:
            href = f"https://www.nintendo.com/{loc}/store/products/{slug}/"
         elif nsuid:
            href = f"https://www.nintendo.com/{loc}/store/products/{nsuid}/"
         else:
            href = f"https://www.nintendo.com/{loc}/store/"

      if href and isinstance(href, str):
         if href.startswith("//"):
            href = f"https:{href}"
         elif href.startswith("/"):
            href = f"https://www.nintendo.com{href}"
         elif href.startswith("store/"):
            href = f"https://www.nintendo.com/{href}"

      # Price normalization
      # We prefer display strings when Nintendo provides them ("Free", "$59.99", etc.).
      display = (
         (it.get("price") or {}).get("display") or
         it.get("displayPrice") or
         it.get("priceDisplay")
      )
      amount = None
      currency = None
      price_obj = it.get("price") or {}
      if isinstance(price_obj, dict):
         # Possible numeric fields: "regular", "discounted", "current", "amount"
         amt = price_obj.get("discounted") or price_obj.get("current") or price_obj.get("regular") or price_obj.get("amount")
         try:
            amount = float(amt) if amt is not None else None
         except Exception:
            amount = None
         currency = price_obj.get("currency") or price_obj.get("currencyCode")
      eshop_details = it.get("eshopDetails") if isinstance(it.get("eshopDetails"), dict) else {}
      if isinstance(price_obj, dict):
         amt_guess, cur_guess, disp_guess = _extract_price_components(price_obj, eshop_details)
         amount = amount or amt_guess
         currency = currency or cur_guess
         if not isinstance(display, string_types()) and disp_guess:
            display = disp_guess
      if not isinstance(display, string_types()) and isinstance(eshop_details, dict):
         maybe_flag = eshop_details.get("goldPointOfferType")
         if maybe_flag:
            display = maybe_flag
      price_str = display if isinstance(display, string_types()) else price_to_string(amount, currency)

      # Platforms: almost always "Switch" for Nintendo store data
      platforms = it.get("platforms") or it.get("platform") or []
      if isinstance(platforms, str):
         platforms = [platforms]
      if not platforms:
         platforms = ["Switch"]
      platforms = normalize_platforms(platforms)

      raw_rating = it.get("rating") or it.get("ratings")
      if isinstance(raw_rating, dict):
         raw_rating = raw_rating.get("display") or raw_rating.get("name")
      elif isinstance(raw_rating, list) and raw_rating:
         cand = raw_rating[0]
         if isinstance(cand, dict):
            raw_rating = cand.get("display") or cand.get("name")
      rating = normalize_rating(raw_rating)

      # UUID: NSUID preferred if present
      uuid = it.get("nsuid") or it.get("id") or it.get("productId")

      return GameRecord(
         store="nintendo",
         name=name,
         price=price_str,
         image=image,
         href=str(href),
         uuid=str(uuid) if uuid else None,
         platforms=platforms,
         rating=rating,
         type="game",
      )

   # ---------- Strategy B: HTML + embedded JSON ----------

   async def _iter_list_page(self, url: str) -> AsyncIterator[Optional[GameRecord]]:
      html = await self.get_text(url, headers={"Accept": "text/html"})
      # 1) __NEXT_DATA__
      for rec in self._parse_next_data(html, base_url=url):
         yield rec
      # 2) JSON-LD
      for rec in self._parse_jsonld(html, base_url=url):
         yield rec

   def _parse_next_data(self, html: str, *, base_url: str) -> List[Optional[GameRecord]]:
      out: List[Optional[GameRecord]] = []
      m = _NEXT_RE.search(html)
      if not m:
         return out
      try:
         js = json.loads(m.group(1))
      except Exception:
         return out

      # Walk the Next.js tree to locate product arrays
      def walk(o: Any):
         if isinstance(o, dict):
            # frequent keys: "products", "items", "results", "tiles"
            for key in ("products", "items", "results", "tiles"):
               v = o.get(key)
               if isinstance(v, list):
                  for it in v:
                     rec = self._normalize_api_item(self._coerce_to_api_like(it, base_url))
                     if rec:
                        out.append(rec)
            for v in o.values():
               walk(v)
         elif isinstance(o, list):
            for v in o:
               walk(v)

      walk(js)
      return out

   def _coerce_to_api_like(self, it: Any, base_url: str) -> Dict[str, Any]:
      """
      Convert a heterogeneous tile/card into a dict compatible with _normalize_api_item.
      """
      if not isinstance(it, dict):
         return {}
      guess: Dict[str, Any] = {}

      # name/title
      guess["title"] = it.get("title") or it.get("name") or it.get("productTitle") or ""

      # image(s)
      if it.get("imageUrl") or it.get("image"):
         guess["imageUrl"] = it.get("imageUrl") or it.get("image")
      else:
         imgs = it.get("images") or it.get("keyImages") or []
         if imgs:
            guess["keyImages"] = imgs

      # link
      link = it.get("url") or it.get("href") or it.get("productUrl")
      guess["productUrl"] = link or base_url

      # price (can be object or display string)
      price = it.get("price") or it.get("displayPrice") or it.get("priceDisplay")
      if isinstance(price, dict):
         guess["price"] = price
      elif isinstance(price, str):
         guess["displayPrice"] = price

      # ids
      guess["nsuid"] = it.get("nsuid") or it.get("id") or it.get("productId")

      # platforms
      plats = it.get("platforms")
      if isinstance(plats, list):
         guess["platforms"] = normalize_platforms(plats)
      elif isinstance(plats, str):
         guess["platforms"] = normalize_platforms([plats])

      return guess

   def _coerce_algolia_hit(self, hit: Any) -> Dict[str, Any]:
      if not isinstance(hit, dict):
         return {}
      guess: Dict[str, Any] = {}

      slug = hit.get("slug") or hit.get("urlKey")
      if slug:
         guess.setdefault("slug", slug)

      guess["title"] = hit.get("title") or hit.get("name") or hit.get("productTitle") or ""
      guess["nsuid"] = hit.get("nsuid") or hit.get("id") or hit.get("productId")

      image = (
         hit.get("boxArt") or hit.get("heroBanner") or hit.get("image") or
         hit.get("productImage") or hit.get("productImageSquare")
      )
      if image:
         guess["image"] = _normalize_asset_url(str(image))

      link = hit.get("url") or hit.get("productUrl")
      if not link and slug:
         loc = self.config.locale.replace("_", "-").lower()
         link = f"https://www.nintendo.com/{loc}/store/products/{slug}/"
      guess["productUrl"] = link or None

      price = hit.get("price") or hit.get("prices") or {}
      eshop_details = hit.get("eshopDetails") if isinstance(hit.get("eshopDetails"), dict) else {}
      amt, cur, disp = _extract_price_components(price, eshop_details)
      currency = cur or (price.get("currency") if isinstance(price, dict) else None) or hit.get("currency") or hit.get("currencyCode")
      if amt is not None or currency:
         guess["price"] = {"amount": amt, "currency": currency}
      if disp:
         guess["displayPrice"] = disp
      elif isinstance(price, str):
         guess["displayPrice"] = price

      display_price = hit.get("priceDisplay") or hit.get("price_display") or hit.get("priceText")
      if display_price and "displayPrice" not in guess:
         guess["displayPrice"] = display_price

      plats = hit.get("platforms") or hit.get("platform") or hit.get("corePlatforms")
      if isinstance(plats, list):
         guess["platforms"] = normalize_platforms(plats)
      elif isinstance(plats, str):
         guess["platforms"] = normalize_platforms([plats])

      rating = hit.get("rating") or hit.get("esrb") or hit.get("ageRating") or hit.get("contentRating")
      if isinstance(rating, dict):
         rating = rating.get("label") or rating.get("code")
      if rating:
         guess["ratings"] = rating

      return guess

   def _parse_jsonld(self, html: str, *, base_url: str) -> List[Optional[GameRecord]]:
      out: List[Optional[GameRecord]] = []
      for m in _JSONLD_RE.finditer(html):
         try:
            block = json.loads(m.group(1))
         except Exception:
            continue
         blocks = block if isinstance(block, list) else [block]
         for b in blocks:
            if not isinstance(b, dict):
               continue
            # Accept Product/VideoGame schemas, or walk @graph
            types = {str(b.get("@type","")).lower()}
            if "@graph" in b and isinstance(b["@graph"], list):
               for g in b["@graph"]:
                  if not isinstance(g, dict):
                     continue
                  t = str(g.get("@type","")).lower()
                  if t in {"product","videogame"}:
                     rec = self._normalize_jsonld_item(g, base_url)
                     if rec:
                        out.append(rec)
            elif types & {"product","videogame"}:
               rec = self._normalize_jsonld_item(b, base_url)
               if rec:
                  out.append(rec)
      return out

   def _normalize_jsonld_item(self, b: Dict[str, Any], base_url: str) -> Optional[GameRecord]:
      name = strip_edition_noise(clean_title(b.get("name") or ""))
      if not name:
         return None

      image = b.get("image")
      if isinstance(image, list):
         image = image[0] if image else None

      offers = b.get("offers") or {}
      if isinstance(offers, list):
         offers = offers[0] if offers else {}
      currency = offers.get("priceCurrency")
      try:
         amt = float(offers.get("price")) if "price" in offers else None
      except Exception:
         amt = None

      price_str = price_to_string(amt, currency)
      href = b.get("url") or base_url

      # Nintendo output is primarily Switch
      platforms: List[str] = normalize_platforms(["Switch"])

      # NSUID is sometimes available in JSON-LD (not guaranteed)
      uuid = b.get("sku") or b.get("productID") or b.get("mpn") or None

      return GameRecord(
         store="nintendo",
         name=name,
         price=price_str,
         image=str(image) if image else "https://www.nintendo.com/etc.clientlibs/ncom/clientlibs/clientlib-ncom/resources/img/nintendo_red.svg",
         href=str(href),
         uuid=str(uuid) if uuid else None,
         platforms=platforms,
         rating=None,
         type="game",
      )

# Small helper to check str-ness in a typed way
def string_types():
   try:
      return (str,)
   except Exception:
      return (str,)
