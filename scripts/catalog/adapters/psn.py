from __future__ import annotations
import asyncio
import json
import re
from urllib.parse import quote_plus, urlparse, parse_qs
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

# Notes:
# - PSN's public surface has changed multiple times (legacy "valkyrie" / "chihiro",
#   Next.js app with __NEXT_DATA__, etc.). This adapter is defensive:
#   1) Try a JSON search API (if present for your region).
#   2) Fallback: fetch HTML listing pages and parse embedded JSON (__NEXT_DATA__ / JSON-LD).
#
# - You should populate SEED_PAGES with a handful of "browse" URLs that list many products
#   for your locale/region (e.g., "All Games" / "Deals" / "Collections").
#
# - Out of the box, the fallback will work for most list pages that embed Next.js data.

# Per-domain limiter (tweak via config.rps if needed)
PSN_LIMIT = DomainLimiter(2.0)

_JSONLD_RE = re.compile(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.S | re.I)
_NEXT_RE   = re.compile(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', re.S | re.I)

@dataclass(slots=True)
class PSNEndpoints:
   # If your region exposes a JSON search API, put it here (format string).
   # Example pattern (adjust as your region allows):
   # "https://store.playstation.com/api/v1/search?query={query}&size={size}&country={country}&language={lang}&page={page}"
   search_api: Optional[str] = None

   # Seed listing pages (Next.js pages) to crawl and parse (__NEXT_DATA__/JSON-LD)
   seed_pages: List[str] = None

def _default_seed_pages(country: str, locale: str) -> List[str]:
   # These are general directory pages that often embed large product lists.
   # You can add/remove based on what your region exposes.
   base = f"https://store.playstation.com/{locale.lower()}-{country.lower()}"
   return [
      f"{base}/pages/browse/ps5-games",
      f"{base}/pages/browse/ps4-games",
      f"{base}/pages/browse/ps5-deals",
      f"{base}/pages/browse/ps4-deals",
   ]

class PSNAdapter(Adapter):
   store = "psn"
   capabilities = Capabilities(pagination=True, returns_partial_price=True)

   def __init__(self, *, config: AdapterConfig | None = None,
                endpoints: PSNEndpoints | None = None, **kw):
      super().__init__(config=config, **kw)
      self.endpoints = endpoints or PSNEndpoints(
         search_api=(
            "https://store.playstation.com/api/productsearch/v2"
            "?query={query}&size={size}&country={country}&language={language}&lang={lang}&offset={offset}"
         ),
         seed_pages=_default_seed_pages(self.config.country, self.config.locale),
      )

   # ---------- public contract ----------

   async def iter_games(self) -> AsyncIterator[GameRecord]:
      # Strategy A: Search API (optional). Demonstrated with A-Z seeds if provided.
      if self.endpoints.search_api:
         # paginate letters 'a'..'z' (tweak to your needs)
         for ch in "abcdefghijklmnopqrstuvwxyz":
            async for rec in self._iter_search_api(query=ch, page_size=50):
               if rec:  # could be None if malformed
                  yield rec
            # brief polite pause between seed letters
            await asyncio.sleep(0.1)

      # Strategy B: Fallback to HTML pages with embedded JSON
      for url in self.endpoints.seed_pages or []:
         async for rec in self._iter_list_page(url):
            if rec:
               yield rec
         # be nice between heavy pages
         await asyncio.sleep(0.2)

   # ---------- Strategy A: JSON search API (optional) ----------

   async def _iter_search_api(self, *, query: str, page_size: int = 50) -> AsyncIterator[Optional[GameRecord]]:
      """
      Iterate products via a JSON search endpoint (if your region exposes one).
      This uses the base.paginate helper-like flow.
      """
      assert self.endpoints.search_api, "search_api endpoint template not configured"

      headers = {
         "Accept": "application/json",
         "X-PSN-Store-Locale": f"{self.config.locale.lower()}-{self.config.country.lower()}",
         "Referer": f"https://store.playstation.com/{self.config.locale.lower()}-{self.config.country.lower()}",
      }

      locale = self.config.locale.replace("_", "-")
      language = locale.split("-")[0]
      offset = 0
      while True:
         url = self.endpoints.search_api.format(
            query=quote_plus(query),
            size=page_size,
            country=self.config.country.upper(),
            language=language,
            lang=locale,
            offset=offset,
         )
         js = await self.get_json(url, headers=headers)
         items = js.get("products") or js.get("results") or js.get("items") or []
         if isinstance(items, dict):
            items = items.get("products") or []
         count = 0
         for it in items or []:
            rec = self._normalize_api_item(it)
            if rec:
               count += 1
               yield rec
         next_offset = None
         links = js.get("links") or {}
         if isinstance(links, dict):
            for key in ("next", "nextPage", "nextPageUrl"):
               href = links.get(key)
               if not isinstance(href, str):
                  continue
               try:
                  parsed = urlparse(href)
               except Exception:
                  parsed = None
               if not parsed:
                  continue
               qs = parse_qs(parsed.query)
               for qk in ("offset", "start"):
                  if qk in qs and qs[qk]:
                     try:
                        next_offset = int(qs[qk][0])
                        break
                     except Exception:
                        next_offset = None
               if next_offset is None and "page" in qs and qs["page"]:
                  try:
                     next_offset = int(qs["page"][0]) * page_size
                  except Exception:
                     next_offset = None
               if next_offset is not None:
                  break
            if next_offset is not None:
               offset = next_offset
               await asyncio.sleep(0.05)
               continue
         total = js.get("total_results") or js.get("totalResults") or js.get("total")
         if total is not None:
            try:
               total = int(total)
            except Exception:
               total = None
         if total is not None and (offset + count) < total:
            offset += count or page_size
            await asyncio.sleep(0.05)
            continue
         if count >= page_size:
            offset += count
            await asyncio.sleep(0.05)
            continue
         break

   def _normalize_api_item(self, it: Dict[str, Any]) -> Optional[GameRecord]:
      """
      Normalize an item returned by a JSON search API variant.
      Adjust keys to match your region’s actual payload.
      """
      # Common-ish fields seen across PSN JSON variants:
      name = strip_edition_noise(clean_title(it.get("name") or it.get("title") or ""))
      if not name:
         return None

      # image (prefer hero/cover)
      image = (
         it.get("image") or
         it.get("media", {}).get("thumbnailUrl") or
         (it.get("keyImages") or [{}])[0].get("url") or
         ""
      )

      # url / href
      href = (
         it.get("url") or
         it.get("productUrl") or
         it.get("webUrl") or
         ""
      )

      # price normalization (display prices are often present)
      display = (
         (it.get("price") or {}).get("display") or
         it.get("displayPrice") or
         None
      )
      # fallback to amount+currency if present
      amount = None
      currency = None
      price_obj = it.get("price") or {}
      if isinstance(price_obj, dict):
         amount = price_obj.get("discounted") or price_obj.get("current") or price_obj.get("amount")
         currency = price_obj.get("currency")

      if display and isinstance(display, str):
         price_str = display
      else:
         price_str = price_to_string(amount, currency)

      # platforms
      platforms: List[str] = []
      for p in (it.get("platforms") or it.get("playablePlatforms") or []):
         if isinstance(p, dict):
            platforms.append(p.get("name") or p.get("platform") or "")
         else:
            platforms.append(str(p))
      platforms = normalize_platforms(platforms)

      # rating (ESRB-like)
      rating = None
      ratings = it.get("rating") or it.get("ratings") or {}
      if isinstance(ratings, dict):
         rating = ratings.get("display") or ratings.get("ageRating")
      elif isinstance(ratings, list) and ratings:
         rating = (ratings[0].get("display") or ratings[0].get("ageRating"))

      rating = normalize_rating(rating)

      uuid = (
         it.get("id") or it.get("skuId") or it.get("productId") or
         it.get("contentId") or None
      )

      image = str(image) if image else "https://store.playstation.com/assets/cover-placeholder.png"
      href = str(href) if href else f"https://store.playstation.com/{self.config.locale.lower()}-{self.config.country.lower()}"

      return GameRecord(
         store="psn",
         name=name,
         price=price_str,
         image=image,
         href=href,
         uuid=str(uuid) if uuid else None,
         platforms=platforms,
         rating=rating,
         type="game",
      )

   # ---------- Strategy B: HTML + embedded JSON ----------

   async def _iter_list_page(self, url: str) -> AsyncIterator[Optional[GameRecord]]:
      """
      Fetch a Next.js listing page and parse embedded product data.
      We look for __NEXT_DATA__ first, then JSON-LD as a fallback.
      """
      html = await self.get_text(url, headers={"Accept": "text/html"}, params=None)
      # First: parse __NEXT_DATA__
      for rec in self._parse_next_data(html, base_url=url):
         yield rec
      # Then: parse JSON-LD (often individual product cards include ld+json)
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

      # Next.js trees vary; walk for plausible product arrays
      def walk(o: Any):
         if isinstance(o, dict):
            # common product-ish keys
            if "products" in o and isinstance(o["products"], list):
               for it in o["products"]:
                  rec = self._normalize_next_item(it, base_url)
                  if rec:
                     out.append(rec)
            # sometimes "results" holds products
            if "results" in o and isinstance(o["results"], list):
               for it in o["results"]:
                  rec = self._normalize_next_item(it, base_url)
                  if rec:
                     out.append(rec)
            for v in o.values():
               walk(v)
         elif isinstance(o, list):
            for v in o:
               walk(v)

      walk(js)
      return out

   def _normalize_next_item(self, it: Dict[str, Any], base_url: str) -> Optional[GameRecord]:
      # Many Next.js props mirror API objects; reuse the API normalizer when possible.
      # Map fields into a simpler dict and pass through _normalize_api_item.
      guess: Dict[str, Any] = {}

      # name / title
      guess["name"] = (
         it.get("name") or it.get("title") or
         it.get("productName") or ""
      )

      # urls
      guess["url"] = (
         it.get("url") or it.get("productUrl") or
         (base_url if base_url else None)
      )

      # image / media
      img = (
         it.get("image") or
         it.get("thumbnail") or
         it.get("media", {}).get("thumbnailUrl") or
         (it.get("keyImages") or [{}])[0].get("url")
      )
      if img:
         guess["image"] = img

      # platforms (PS4/PS5)
      plats = []
      psrc = it.get("platforms") or it.get("playablePlatforms") or it.get("platform") or []
      if isinstance(psrc, list):
         for p in psrc:
            if isinstance(p, dict):
               plats.append(p.get("name") or p.get("platform") or "")
            else:
               plats.append(str(p))
      elif isinstance(psrc, str):
         plats = [psrc]
      guess["platforms"] = normalize_platforms(plats)

      # price
      price = it.get("price") or {}
      if isinstance(price, dict):
         # Prefer display string if available
         if "display" in price:
            guess["displayPrice"] = price["display"]
         else:
            if "amount" in price:
               guess["price"] = {"amount": price.get("amount"), "currency": price.get("currency")}
            elif "discounted" in price or "current" in price:
               amt = price.get("discounted") or price.get("current")
               guess["price"] = {"amount": amt, "currency": price.get("currency")}

      # rating
      rating = it.get("rating") or it.get("ratings")
      if rating:
         guess["rating"] = rating

      # ids
      guess["id"] = it.get("id") or it.get("skuId") or it.get("productId") or it.get("contentId")

      return self._normalize_api_item(guess)

   def _parse_jsonld(self, html: str, *, base_url: str) -> List[Optional[GameRecord]]:
      out: List[Optional[GameRecord]] = []
      for m in _JSONLD_RE.finditer(html):
         try:
            block = json.loads(m.group(1))
         except Exception:
            continue
         # JSON-LD may be a dict or a list of dicts
         blocks = block if isinstance(block, list) else [block]
         for b in blocks:
            # Only consider Product/Game-like schemas
            if not isinstance(b, dict):
               continue
            if (b.get("@type") or "").lower() not in {"product", "videogame"}:
               # Sometimes an array under "@graph"
               for g in (b.get("@graph") or []):
                  if isinstance(g, dict) and (g.get("@type") or "").lower() in {"product", "videogame"}:
                     rec = self._normalize_jsonld_item(g, base_url)
                     if rec:
                        out.append(rec)
               continue
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

      # Platforms: JSON-LD often lacks detailed platform info; leave empty or infer from url
      platforms: List[str] = []
      if "PlayStation 5" in (b.get("name") or ""):
         platforms.append("PS5")
      if "PlayStation 4" in (b.get("name") or ""):
         platforms.append("PS4")
      platforms = normalize_platforms(platforms)

      return GameRecord(
         store="psn",
         name=name,
         price=price_str,
         image=str(image) if image else "https://store.playstation.com/assets/cover-placeholder.png",
         href=str(href),
         uuid=None,
         platforms=platforms,
         rating=None,
         type="game",
      )
