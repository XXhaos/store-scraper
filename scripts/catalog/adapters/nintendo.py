from __future__ import annotations
import asyncio
import json
import re
from urllib.parse import quote_plus
from dataclasses import dataclass
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

@dataclass(slots=True)
class NintendoEndpoints:
   # Example (fill if you’ve got one for your locale/market):
   # "https://www.nintendo.com/search/api?query={query}&count={count}&country={country}&locale={locale}&page={page}"
   search_api: Optional[str] = None
   algolia_app_id: Optional[str] = None
   algolia_api_key: Optional[str] = None
   algolia_index: Optional[str] = None

   # Listing pages that contain many products (per locale)
   seed_pages: List[str] = None

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
         search_api="https://u3b6gr4ua3-dsn.algolia.net/1/indexes/*/queries",
         algolia_app_id="U3B6GR4UA3",
         algolia_api_key="9a2c7a43f1f6c9616bf1b9d5f3fa0c6e",
         algolia_index="ncom_game_en_{country}",
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
      if self.endpoints.algolia_app_id and self.endpoints.algolia_api_key:
         headers.update({
            "X-Algolia-Application-Id": self.endpoints.algolia_app_id,
            "X-Algolia-API-Key": self.endpoints.algolia_api_key,
         })

      locale = self.config.locale.replace("_", "-").lower()
      index_template = self.endpoints.algolia_index or "ncom_game_en_{country}"
      index_name = index_template.format(country=self.config.country.lower(), locale=locale)

      page = 0
      while True:
         params = {
            "query": quote_plus(query),
            "hitsPerPage": str(page_size),
            "page": str(page),
         }
        
         payload = {
            "requests": [
               {
                  "indexName": index_name,
                  "params": "&".join(f"{k}={v}" for k, v in params.items()),
               }
            ]
         }

         if self.endpoints.search_api.endswith("/queries"):
            resp = await self.request("POST", self.endpoints.search_api, json=payload, headers=headers)
            js = resp.json()
            results = (js.get("results") or [])
            if results:
               items = results[0].get("hits") or []
               nb_pages = results[0].get("nbPages")
            else:
               items = []
               nb_pages = None
         else:
            js = await self.get_json(self.endpoints.search_api.format(
               query=query,
               count=page_size,
               country=self.config.country,
               locale=self.config.locale,
               page=page,
            ), headers=headers)
            items = self._extract_items_from_api(js)
            nb_pages = None

         count = 0
         for it in items:
            coalesced = self._coerce_algolia_hit(it) if self.endpoints.search_api.endswith("/queries") else it
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
         it.get("image") or it.get("imageUrl") or it.get("boxArt") or it.get("heroBanner")
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
      image = str(image) if image else "https://www.nintendo.com/etc.clientlibs/ncom/clientlibs/clientlib-ncom/resources/img/nintendo_red.svg"

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
      price_str = display if isinstance(display, string_types()) else price_to_string(amount, currency)

      # Platforms: almost always "Switch" for Nintendo store data
      platforms = it.get("platforms") or []
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

      guess["title"] = hit.get("title") or hit.get("name") or hit.get("productTitle") or ""
      guess["nsuid"] = hit.get("nsuid") or hit.get("id") or hit.get("productId")
      if hit.get("slug"):
         guess.setdefault("slug", hit.get("slug"))

      image = hit.get("boxArt") or hit.get("heroBanner") or hit.get("image")
      if image:
         guess["image"] = image

      link = hit.get("url") or hit.get("productUrl")
      if not link and hit.get("slug"):
         loc = self.config.locale.replace("_", "-").lower()
         link = f"https://www.nintendo.com/{loc}/store/products/{hit['slug']}/"
      guess["productUrl"] = link or None

      price = hit.get("price") or hit.get("prices") or {}
      if isinstance(price, dict):
         amount = price.get("discounted") or price.get("current") or price.get("regular") or price.get("amount")
         if amount is None and "raw" in price and isinstance(price["raw"], dict):
            raw = price["raw"]
            amount = raw.get("discounted") or raw.get("current") or raw.get("regular")
         currency = price.get("currency") or price.get("currencyCode")
         if isinstance(amount, (int, float)) and amount > 1000:
            amount = float(amount) / 100.0
         guess["price"] = {"amount": amount, "currency": currency}
         display = price.get("display") or price.get("formatted") or price.get("rawValue")
         if display:
            guess["displayPrice"] = display
      elif isinstance(price, (int, float)):
         amt = float(price)
         if amt > 1000:
            amt = amt / 100.0
         guess["price"] = {"amount": amt, "currency": hit.get("currency")}
      elif isinstance(price, str):
         guess["displayPrice"] = price

      display_price = hit.get("priceDisplay") or hit.get("price_display") or hit.get("priceText")
      if display_price and "displayPrice" not in guess:
         guess["displayPrice"] = display_price

      plats = hit.get("platforms") or hit.get("platform")
      if isinstance(plats, list):
         guess["platforms"] = normalize_platforms(plats)
      elif isinstance(plats, str):
         guess["platforms"] = normalize_platforms([plats])

      rating = hit.get("rating") or hit.get("esrb") or hit.get("ageRating")
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
