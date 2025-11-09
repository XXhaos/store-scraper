from __future__ import annotations
import asyncio
import os
from typing import AsyncIterator, Dict, Any, List, Optional

from catalog.adapters.base import Adapter, AdapterConfig, Capabilities
from catalog.models import GameRecord
from catalog.normalize import clean_title, strip_edition_noise, price_to_string

API_FEATURED  = "https://store.steampowered.com/api/featuredcategories"
API_DETAILS   = "https://store.steampowered.com/api/appdetails"
API_APP_LIST  = "http://api.steampowered.com/ISteamApps/GetAppList/v0002/"

class SteamAdapter(Adapter):
   """
   Steam adapter (no auth required).

   Strategy:
     1) Hit 'featuredcategories' to get a broad, fresh set of appids (top_sellers, specials, new_releases, coming_soon).
     2) Hydrate each appid via 'appdetails' and normalize into GameRecord.

   Notes:
     - Skips non-'game' types by default (DLC/soundtracks/tools). Toggle with include_types if desired.
     - Price strings: prefers Steam's display via price_overview; falls back to 'Free' or 'Unavailable'.
     - Platforms: maps Windows/Mac/Linux flags to canonical names.
   """
   store = "steam"
   capabilities = Capabilities(pagination=False, returns_partial_price=False, yields_dlc=False)

   def __init__(self, *, config: AdapterConfig | None = None,
                include_types: Optional[List[str]] = None,  # e.g., ["game","dlc"]
                buckets: Optional[List[str]] = None,        # override featured buckets
                app_list_url: Optional[str] = None,
                api_key: Optional[str] = None,
                **kw):
      super().__init__(config=config, **kw)
      self.include_types = [t.lower() for t in (include_types or ["game"])]
      self.buckets = buckets or ["coming_soon", "specials", "top_sellers", "new_releases"]
      self._app_list_url = app_list_url or API_APP_LIST
      # allow passing via ctor or environment; empty string treated as absent
      self._api_key = (api_key if api_key is not None else os.getenv("STEAM_API_KEY")) or None

   async def iter_games(self) -> AsyncIterator[GameRecord]:
      # Step 1: seed appids from the global Steam app list, fallback to featured categories
      appids = await self._fetch_app_list_ids()
      if not appids:
         featured = await self.get_json(API_FEATURED, params={"l": "english"})
         appids = self._extract_featured_appids(featured, self.buckets)

      # Step 2: hydrate via appdetails (region-aware pricing via cc)
      for appid in appids:
         data = await self._fetch_appdetails(appid)
         if not data:
            continue

         rec = self._normalize_app(appid, data)
         if rec:
            yield rec
         await asyncio.sleep(0.05)  # polite jitter between app calls

   # ---------------- helpers ----------------

   async def _fetch_app_list_ids(self) -> List[str]:
      params = {"format": "json"}
      if self._api_key:
         params["key"] = self._api_key

      try:
         js = await self.get_json(self._app_list_url, params=params)
      except Exception:
         return []

      apps = (((js.get("applist") or {}).get("apps")) or [])
      ids: List[str] = []
      seen: set[str] = set()
      for entry in apps:
         appid = entry.get("appid")
         if not isinstance(appid, int):
            continue
         appid_str = str(appid)
         if appid_str in seen:
            continue
         seen.add(appid_str)
         ids.append(appid_str)
      return ids

   def _extract_featured_appids(self, featured: Dict[str, Any], buckets: List[str]) -> List[str]:
      ids: List[str] = []
      for b in buckets:
         items = (featured.get(b) or {}).get("items") or []
         for it in items:
            try:
               ids.append(str(it["id"]))
            except Exception:
               continue
      # de-dup while preserving order
      seen = set()
      return [a for a in ids if not (a in seen or seen.add(a))]

   async def _fetch_appdetails(self, appid: str) -> Optional[Dict[str, Any]]:
      js = await self.get_json(
         API_DETAILS,
         params={"appids": appid, "l": "english", "cc": self.config.country}
      )
      payload = js.get(str(appid))
      if not payload or not payload.get("success"):
         return None
      return payload.get("data") or None

   def _normalize_app(self, appid: str, app: Dict[str, Any]) -> Optional[GameRecord]:
      # Filter by type
      app_type = (app.get("type") or "").lower()
      if self.include_types and app_type and app_type not in self.include_types:
         return None

      # Title
      name_raw = app.get("name") or ""
      name = strip_edition_noise(clean_title(name_raw))
      if not name:
         return None

      # Price string
      p = app.get("price_overview")
      if isinstance(p, dict):
         # price_overview.final is in cents
         try:
            amount = float(p["final"]) / 100.0
         except Exception:
            amount = None
         currency = p.get("currency")
         # If discount present, we still output the discounted display (no strike-through in schema)
         price_str = price_to_string(amount, currency)
      else:
         price_str = "Free" if app.get("is_free") else "Unavailable"

      # Image / href
      image = app.get("header_image") or ""
      href = f"https://store.steampowered.com/app/{appid}"

      # Platforms
      platforms: List[str] = []
      plat = app.get("platforms") or {}
      if plat.get("windows"): platforms.append("Windows")
      if plat.get("mac"):     platforms.append("Mac")
      if plat.get("linux"):   platforms.append("Linux")

      # UUID: use appid (string)
      uuid = str(appid)

      return GameRecord(
         store="steam",
         name=name,
         price=price_str,
         image=str(image) if image else "https://store.steampowered.com/public/shared/images/header/globalheader_logo.png",
         href=href,
         uuid=uuid,
         platforms=platforms,
         rating=None,
         type="game" if app_type == "game" else app_type or None,
         extra={"steam_type": app.get("type")}
      )
