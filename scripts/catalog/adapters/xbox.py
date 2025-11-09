from __future__ import annotations
import asyncio
import base64
import json
import re
from urllib.parse import quote_plus, urlparse, parse_qs
from dataclasses import dataclass
from typing import AsyncIterator, Dict, Any, Iterable, List, Optional, Set

import httpx

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

# Microsoft/Xbox pages are SPA-driven and often embed large JSON blobs.
# This adapter:
#  1) Uses a JSON search/browse endpoint if you have one (region-dependent).
#  2) Falls back to scraping listing pages and extracting either:
#     - __NEXT_DATA__/React hydration data
#     - application/ld+json (JSON-LD)
#
# You provide a few "seed" listing pages that enumerate lots of titles.
# Example seed: https://www.xbox.com/en-us/games/all-games

XBOX_LIMIT = DomainLimiter(2.0)

XBL_EXPERIMENTS_HEADER = (
   "enableuhfcache,forcerefreshexp,forceservernav,enableaamscript,"
   "enableserversideuserfeatureassignments,enableserverauthv3,aatest9010,"
   "aatestdevice9010,aatest_50_50,aa_test_device_50_50,autofullscreenpersist,"
   "disablexgp,enableaapaddfriendstream,enableaapmultiplayertitle,"
   "enableaapscreentimestream,enableabsolutemouse,enableaccountlinking,"
   "enableachievements,enableanontelemetry,enableaskaparentaddfriend,"
   "enableaskaparentcontent,enableaskaparentscreentime,enableauthv2ew,"
   "enableauthv2ewtizen,enablebundlebuilderadobelaunch,enablebuynowdynamicmeparams,"
   "enablebuynowxboxuiexp,enablebyog,enablebyogpurchase,"
   "enablecartcheckoutdynamicparams,enablecartmoraystyling,"
   "enablecelestiaxboxcom,enablechangegamertag,enablechatimageupload,"
   "enableclientauthv3,enableclientguide,enableclientguideinstream,"
   "enableclientrenderedcursor,enablecomingsoonupsell,enableconsoles,"
   "enablecontextualstorebrowse,enablecontrollerstatusv2,"
   "enabledefaultultimateupsell,enabledisconnectwarning,"
   "enableenhancedreportareview,enablefeedbacksdk,enablefriendlinksharing,"
   "enablefriendsandfollowers,enablegameinvites,enablegarrisoninlineredeem,"
   "enablegtaplus,enableguestcheckout,enableguidechattab,enableguidehometab,"
   "enableguidenotifications,enableguideprofiletab,enableiapbrowseexperience,"
   "enableinternalcookielist,enablejanus,enablejanusxboxcom,enablelaunchpad,"
   "enableleavingdate,enableloginsagafix,enablemaunaloa,enablemecontrolgamerscore,"
   "enablemecontrolpresence,enablemediaplayonweb,enablemessagesafetysettings,"
   "enablemiconmacsafari,enableminipdprefreshexp,enablemouseandkeyboard,"
   "enablemulticloudplaybutton,enablemultiupsellbutton,"
   "enablemutualfriendsprivacysettings,enablenakatomi,enablenakatomiu,"
   "enablenetqualityindicator,enablenewsearchexperience,enablenewsearchgeneraltab,"
   "enableoffersandcredits,enableopenendedgameinvites,enableoverridedevsettings,"
   "enableparties,enablepdpgallery,enablepidlstandarizedforms,"
   "enableplaypathnavigation,enableplaywithfriendsrow,"
   "enablepresenceheartbeat,enableprivacycontrols,enableprovisioningupsell,"
   "enablereactcheckout,enablereactgiftflow,enablereactredeem,"
   "enablerealnamesharing,enableredeemcodemodal,enableremoteplay,"
   "enablesearchpagev2,enablesearchpromo,enablesenerchia,enablesessiontime,"
   "enablesiglv3,enableskipactivatestoreexp,enablestorebyog,"
   "enablestreamstatistics,enabletakcontrolresizing,"
   "enabletakhighcontrastmode,enabletitanautorenewtoggle,"
   "enabletitanredeemsubs,enabletitleactivation,enabletvautosignout,"
   "enabletvgamepassupsellv2,enabletvlayerhint,enableubisoftpcversionlegaltext,"
   "enableubisoftplusdata,enableusbguidance,enableuseretryafterheader,"
   "enableuserprofilepage,enableuserstoragemenu,enablewishlistgifting,"
   "enablexboxapponmobilegooglepay,enablexboxcomnewui,"
   "enablexboxcomredeemhostnor,enablexboxgamepadlite,"
   "enablexboxonerfaccountsettings,enablexboxonerfsetupredirectpage,"
   "enablexboxsmallpurchasedialog,enablexcloudxboxcardredeem,"
   "enablexesurveys,enablexsearch,enablextracetelemetry,hidebetareferences,"
   "purchasesdkcartcheckout,randomizeentitlementquery,showmousekeyboardsetting,"
   "skipredirectcounter,testautomaticxboxwebexpscorecard,testf2paaforeverrunnig,"
   "test_flight_7_11_752,uselocalorigininvitelinks,usepostmessagehelper,"
   "usetizenh264mainsdphack,usev2msaxblauth,xwsrmdevaa90"
)

_JSONLD_RE = re.compile(
   r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
   re.S | re.I
)
_NEXT_RE = re.compile(
   r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
   re.S | re.I
)

_PRELOADED_STATE_RE = re.compile(
   r'window\.__PRELOADED_STATE__\s*=\s*(\{.*?\})\s*;?\s*</script>',
   re.S | re.I
)

# Some Xbox pages hydrate with "data-state" or window.__INITIAL_DATA__ style blocks
_STATE_RE = re.compile(
   r'<script[^>]+data-state[^>]*>(.*?)</script>',
   re.S | re.I
)
_WININIT_RE = re.compile(
   r'window\.__INITIAL_DATA__\s*=\s*(\{.*?\})\s*;?',
   re.S | re.I
)

@dataclass(slots=True)
class XboxEndpoints:
   # Public browse endpoint surfaced by emerald.xboxservices.com. Accepts POST with
   # optional continuation tokens.
   browse_api: Optional[str] = None

   # If you have an internal/regionally-available search API, put a template here:
   # e.g., "https://www.xbox.com/api/search?query={query}&count={count}&market={country}&locale={locale}&page={page}"
   search_api: Optional[str] = None

   # Listing pages to parse (these should list many products)
   seed_pages: List[str] | None = None

def _default_seed_pages(country: str, locale: str) -> List[str]:
   # xbox.com uses 'en-us' style; normalize locale to that
   # locale like "en-US" -> "en-us"
   loc = locale.replace("_", "-").lower()
   return [
      f"https://www.xbox.com/{loc}/games/browse",
   ]

class XboxAdapter(Adapter):
   store = "xbox"
   capabilities = Capabilities(pagination=True, returns_partial_price=True)

   def __init__(self, *, config: AdapterConfig | None = None,
                endpoints: XboxEndpoints | None = None, **kw):
      super().__init__(config=config, **kw)
      self.endpoints = endpoints or XboxEndpoints(
         browse_api="https://emerald.xboxservices.com/xboxcomfd/browse",
         search_api=(
            "https://www.xbox.com/xwebapp/UnifiedSearch"
            "?Locale={locale}&Market={country}&Query={query}&Skip={skip}&Take={count}"
         ),
         seed_pages=_default_seed_pages(self.config.country, self.config.locale),
      )
      self._ms_cv_base = "4jJHCSTOdoFi3I6HIa4VZs"
      self._ms_cv_counter = 20
      self._initial_encoded_ct: Optional[str] = None
      self._initial_total_items: Optional[int] = None
      self._initial_state_loaded = False

   async def iter_games(self) -> AsyncIterator[GameRecord]:
      seen: Set[str] = set()

      # Strategy A: emerald browse API (preferred)
      if self.endpoints.browse_api:
         async for rec in self._iter_browse_api():
            if rec and self._mark_seen(rec, seen):
               yield rec
         await asyncio.sleep(0.1)

      # Strategy A: JSON API (optional)
      if self.endpoints.search_api:
         for ch in "abcdefghijklmnopqrstuvwxyz":
            async for rec in self._iter_search_api(query=ch, page_size=60):
               if rec and self._mark_seen(rec, seen):
                  yield rec
            await asyncio.sleep(0.1)

      # Strategy B: Listing pages with embedded JSON
      for url in self.endpoints.seed_pages or []:
         async for rec in self._iter_list_page(url):
            if rec and self._mark_seen(rec, seen):
               yield rec
         await asyncio.sleep(0.2)

   def _mark_seen(self, rec: GameRecord, seen: Set[str]) -> bool:
      key = rec.uuid or rec.href or rec.name
      if not key:
         return True
      if key in seen:
         return False
      seen.add(key)
      return True

   # ---------- Strategy A: emerald browse API ----------

   async def _iter_browse_api(self) -> AsyncIterator[Optional[GameRecord]]:
      assert self.endpoints.browse_api, "browse_api endpoint not configured"

      await self._ensure_initial_browse_state()

      locale = self.config.locale.replace("_", "-")
      headers = {
         "Accept": "application/json",
         "Content-Type": "application/json",
         "Origin": "https://www.xbox.com",
         "Referer": f"https://www.xbox.com/{locale.lower()}/",
         "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
         ),
         "x-ms-api-version": "1.1",
      }

      empty_filters = base64.b64encode(json.dumps({}).encode("utf-8")).decode("utf-8")

      base_body = {
         "Filters": empty_filters,
         "ReturnFilters": False,
         "ChannelKeyToBeUsedInResponse": "BROWSE_CHANNELID=_FILTERS=",
         "ChannelId": "",
      }

      continuation: Optional[str] = None
      continuation_key: Optional[str] = "EncodedCT" if self._initial_encoded_ct else None
      if continuation_key:
         continuation = self._initial_encoded_ct
      seen_ids: Set[str] = set()

      while True:
         payload = dict(base_body)
         if continuation_key and continuation:
            payload[continuation_key] = continuation

         headers["Ms-Cv"] = self._next_ms_cv()
         try:
            resp = await self.request(
               "POST",
               self.endpoints.browse_api,
               params={"locale": locale},
               headers=headers,
               json=payload,
            )
         except httpx.HTTPStatusError as exc:
            if exc.response is not None and exc.response.status_code == 403:
               self.log.warning(
                  "xbox: browse API returned HTTP 403; falling back to HTML parsing"
               )
               break
            raise
         js = resp.json()

         produced = 0
         for item in self._extract_browse_items(js, seen_ids):
            rec = self._normalize_browse_item(item)
            if rec:
               produced += 1
               yield rec

         cont_info = self._extract_browse_continuation(js)
         if cont_info:
            continuation_key, continuation = cont_info
         else:
            continuation_key, continuation = None, None

         if self._initial_total_items is None:
            total = self._extract_total_items(js)
            if total is not None:
               self._initial_total_items = total

         has_more = self._extract_browse_has_more(js)
         if has_more is None and continuation:
            decoded = self._decode_encoded_ct(continuation)
            if isinstance(decoded, dict) and isinstance(decoded.get("HasMore"), bool):
               has_more = decoded["HasMore"]
         if (
            not continuation
            or produced == 0
            or has_more is False
            or (
               isinstance(self._initial_total_items, int)
               and self._initial_total_items > 0
               and len(seen_ids) >= self._initial_total_items
            )
         ):
            break
         await asyncio.sleep(0.05)

   def _next_ms_cv(self) -> str:
      value = f"{self._ms_cv_base}.{self._ms_cv_counter}"
      self._ms_cv_counter += 1
      return value

   def _extract_browse_items(self, js: Any, seen_ids: Set[str]) -> List[Dict[str, Any]]:
      items: List[Dict[str, Any]] = []

      def walk(node: Any) -> None:
         if isinstance(node, dict):
            pid = node.get("productId") or node.get("ProductId")
            if pid and (node.get("title") or node.get("Title") or node.get("productFamily")):
               key = str(pid)
               if key not in seen_ids:
                  seen_ids.add(key)
                  items.append(node)
            for value in node.values():
               walk(value)
         elif isinstance(node, list):
            for value in node:
               walk(value)

      walk(js)
      return items

   def _extract_total_items(self, js: Any) -> Optional[int]:
      if isinstance(js, dict):
         for key in ("totalItems", "TotalItems", "totalResults", "TotalResults", "totalCount", "TotalCount", "total"):
            if key in js:
               try:
                  return int(js[key])
               except Exception:
                  return None
         for value in js.values():
            total = self._extract_total_items(value)
            if total is not None:
               return total
      elif isinstance(js, list):
         for value in js:
            total = self._extract_total_items(value)
            if total is not None:
               return total
      return None

   def _extract_browse_continuation(self, js: Any) -> Optional[tuple[str, str]]:
      def walk(node: Any) -> Optional[tuple[str, str]]:
         if isinstance(node, dict):
            for key in ("continuationToken", "ContinuationToken", "nextContinuationToken"):
               token = node.get(key)
               if isinstance(token, str) and token:
                  return key, token
            for key in ("encodedContinuationToken", "EncodedContinuationToken", "EncodedCT", "encodedCT"):
               token = node.get(key)
               if isinstance(token, str) and token:
                  if key.lower().endswith("encodedct"):
                     return "EncodedCT", token
                  return key, token
            for value in node.values():
               token = walk(value)
               if token:
                  return token
         elif isinstance(node, list):
            for value in node:
               token = walk(value)
               if token:
                  return token
         return None

      return walk(js)

   async def _ensure_initial_browse_state(self) -> None:
      if self._initial_state_loaded:
         return
      self._initial_state_loaded = True

      locale = self.config.locale.replace("_", "-").lower()
      url = f"https://www.xbox.com/{locale}/games/browse"

      try:
         html = await self.get_text(url, headers={"Accept": "text/html"})
      except Exception:
         return

      encoded_ct, total_items = self._extract_initial_browse_state(html)
      if encoded_ct:
         self._initial_encoded_ct = encoded_ct
      if total_items is not None:
         self._initial_total_items = total_items

   def _extract_initial_browse_state(self, html: str) -> tuple[Optional[str], Optional[int]]:
      m = _PRELOADED_STATE_RE.search(html)
      if not m:
         return None, None
      try:
         state = json.loads(m.group(1))
      except Exception:
         return None, None

      data: Any = state
      for key in ("core2", "channels", "channelData"):
         if not isinstance(data, dict):
            return None, None
         data = data.get(key)
      if not isinstance(data, dict):
         return None, None

      channel = data.get("BROWSE_CHANNELID=_FILTERS=")
      if not isinstance(channel, dict):
         return None, None

      payload = channel.get("data") or channel.get("Data")
      if not isinstance(payload, dict):
         return None, None

      encoded_ct = (
         payload.get("encodedCT")
         or payload.get("EncodedCT")
         or payload.get("EncodeddCT")
      )
      if isinstance(encoded_ct, str):
         encoded_ct = encoded_ct.strip()
      else:
         encoded_ct = None

      total_items_val = None
      for key in ("totalItems", "TotalItems", "total"):
         if key in payload:
            try:
               total_items_val = int(payload[key])
            except Exception:
               total_items_val = None
            break

      return encoded_ct, total_items_val

   def _extract_browse_has_more(self, js: Any) -> Optional[bool]:
      def walk(node: Any) -> Optional[bool]:
         if isinstance(node, dict):
            for key in ("hasMore", "HasMore", "hasMoreItems"):
               if key in node and isinstance(node[key], bool):
                  return node[key]
            for value in node.values():
               flag = walk(value)
               if flag is not None:
                  return flag
         elif isinstance(node, list):
            for value in node:
               flag = walk(value)
               if flag is not None:
                  return flag
         return None

      return walk(js)

   def _decode_encoded_ct(self, token: str) -> Optional[Dict[str, Any]]:
      if not token:
         return None
      padding = "=" * (-len(token) % 4)
      try:
         raw = base64.b64decode(token + padding)
      except Exception:
         return None
      try:
         return json.loads(raw.decode("utf-8"))
      except Exception:
         return None

   def _normalize_browse_item(self, item: Dict[str, Any]) -> Optional[GameRecord]:
      name = strip_edition_noise(clean_title(
         item.get("title")
         or item.get("Title")
         or item.get("productTitle")
         or ""
      ))
      if not name:
         return None

      images = item.get("images") or {}
      image_url = None
      if isinstance(images, dict):
         for key in ("superHeroArt", "poster", "boxArt"):
            img = images.get(key)
            if isinstance(img, dict) and img.get("url"):
               image_url = img["url"]
               break
      if not image_url:
         image_url = item.get("imageUrl") or item.get("ImageUrl")
      if not image_url:
         image_url = "https://www.xbox.com/etc.clientlibs/settings/wcm/designs/xbox/glyphs/xbox-glyph.png"

      pid = item.get("productId") or item.get("ProductId") or item.get("legacyId")
      loc = self.config.locale.replace("_", "-").lower()
      href = item.get("url") or item.get("Url") or (
         f"https://www.xbox.com/{loc}/games/store/{pid}" if pid else "https://www.xbox.com/"
      )

      price_obj = item.get("specificPrices") or {}
      price_entries: Iterable[Dict[str, Any]] = []
      if isinstance(price_obj, dict):
         for key in ("purchaseable", "giftable", "price"):
            if key in price_obj and isinstance(price_obj[key], list) and price_obj[key]:
               price_entries = price_obj[key]
               break

      amount = None
      currency = None
      price_str: Optional[str] = None
      for entry in price_entries:
         currency = entry.get("currency") or entry.get("Currency")
         raw_amount = entry.get("listPrice") or entry.get("msrp") or entry.get("ListPrice")
         if raw_amount is None:
            continue
         try:
            amount = float(raw_amount)
         except Exception:
            amount = None
         price_str = price_to_string(amount, currency)
         if price_str:
            break

      if not price_str:
         msrp = item.get("msrp") or item.get("msrpPrice")
         try:
            amount = float(msrp) if msrp is not None else None
         except Exception:
            amount = None
         price_str = price_to_string(amount, currency)

      platforms = item.get("availableOn") or []
      if not isinstance(platforms, list):
         platforms = [platforms]
      platforms = normalize_platforms([str(p) for p in platforms if p])

      rating_info = item.get("contentRating") or {}
      raw_rating = None
      if isinstance(rating_info, dict):
         raw_rating = (
            rating_info.get("rating")
            or rating_info.get("ratingDescription")
            or rating_info.get("description")
         )
      rating = normalize_rating(raw_rating)

      product_kind = item.get("productKind") or item.get("productFamily") or "game"
      product_type = str(product_kind).lower() if product_kind else "game"

      return GameRecord(
         store="xbox",
         name=name,
         price=price_str,
         image=str(image_url),
         href=str(href),
         uuid=str(pid) if pid else None,
         platforms=platforms,
         rating=rating,
         type=product_type,
      )

   # ---------- Strategy A: JSON search API (optional) ----------

   async def _iter_search_api(self, *, query: str, page_size: int = 60) -> AsyncIterator[Optional[GameRecord]]:
      assert self.endpoints.search_api, "search_api endpoint template not configured"

      headers = {"Accept": "application/json"}
      locale = self.config.locale.replace("_", "-").lower()
      skip = 0
      while True:
         url = self.endpoints.search_api.format(
            query=quote_plus(query),
            count=page_size,
            country=self.config.country.upper(),
            locale=locale,
            skip=skip,
            page=skip // max(1, page_size),
         )
         js = await self.get_json(url, headers=headers)
         items = self._extract_items_from_api(js)
         count = 0
         for it in items:
            rec = self._normalize_api_item(it)
            if rec:
               count += 1
               yield rec
         next_skip = self._next_skip(js, skip, count, page_size)
         if next_skip is None:
            break
         skip = next_skip
         await asyncio.sleep(0.05)

   def _extract_items_from_api(self, js: Dict[str, Any]) -> List[Dict[str, Any]]:
      # Heuristics for common fields used by Microsoft/Xbox discovery responses
      # Examples: { "products": [...] } or { "Items": [...] } etc.
      if not isinstance(js, dict):
         return []
      for k in ("products", "Products", "items", "Items", "results", "Results"):
         v = js.get(k)
         if isinstance(v, list) and v:
            return v
      # Some APIs nest under "data"
      data = js.get("data")
      if isinstance(data, dict):
         for k in ("products", "items", "results"):
            v = data.get(k)
            if isinstance(v, list) and v:
               return v
      return []

   def _next_skip(self, js: Dict[str, Any], skip: int, produced: int, page_size: int) -> Optional[int]:
      paging = js.get("paging") or js.get("Paging")
      total = None
      if isinstance(paging, dict):
         for key in ("totalItems", "TotalItems", "totalResults", "TotalResults", "totalCount", "TotalCount", "total"):
            if key in paging:
               try:
                  total = int(paging[key])
               except Exception:
                  total = None
               break
         for key in ("nextOffset", "nextSkip"):
            if key in paging:
               try:
                  return int(paging[key])
               except Exception:
                  return None
         for key in ("skip", "Skip"):
            if key in paging:
               try:
                  base = int(paging[key])
               except Exception:
                  base = skip
               if total is not None and (base + produced) < total:
                  return base + max(produced, page_size)
      links = js.get("links") or js.get("Links") or {}
      if isinstance(links, dict):
         for key in ("next", "Next", "nextLink", "NextLink"):
            href = links.get(key)
            if not isinstance(href, str):
               continue
            try:
               parsed = urlparse(href)
            except Exception:
               continue
            qs = parse_qs(parsed.query)
            for candidate in ("skip", "Skip", "start", "Start"):
               if candidate in qs and qs[candidate]:
                  try:
                     return int(qs[candidate][0])
                  except Exception:
                     continue
      if total is not None and (skip + produced) < total:
         return skip + max(produced, page_size)
      if produced >= page_size:
         return skip + produced
      return None

   def _normalize_api_item(self, it: Dict[str, Any]) -> Optional[GameRecord]:
      # Titles can come from "Title", "Name", or "displayName"
      name = strip_edition_noise(clean_title(
         it.get("Title") or it.get("Name") or it.get("displayName") or it.get("title") or ""
      ))
      if not name:
         return None

      # Images are often arrays of { Url, Purpose } or simple strings
      image = (
         it.get("Image") or it.get("image") or
         it.get("ImageUrl") or it.get("imageUrl")
      )
      if not image:
         imgs = it.get("Images") or it.get("images") or []
         if imgs and isinstance(imgs, list):
            # prefer hero/box art
            preferred = None
            for img in imgs:
               purpose = (img.get("Purpose") or img.get("purpose") or "").lower()
               if purpose in {"boxart", "poster", "tile", "branded"}:
                  preferred = img.get("Url") or img.get("url")
                  if preferred:
                     break
            image = preferred or (imgs[0].get("Url") or imgs[0].get("url") if isinstance(imgs[0], dict) else imgs[0])
      image = str(image) if image else "https://www.xbox.com/etc.clientlibs/settings/wcm/designs/xbox/glyphs/xbox-glyph.png"

      # Href: "ProductPageUrl" or build from productId/slug if needed
      href = (
         it.get("ProductPageUrl") or it.get("productPageUrl") or
         it.get("Url") or it.get("url")
      )
      if not href:
         pid = it.get("ProductId") or it.get("productId") or it.get("id")
         if pid:
            loc = self.config.locale.replace("_", "-").lower()
            href = f"https://www.xbox.com/{loc}/games/store/{pid}"
         else:
            href = "https://www.xbox.com/"

      # Price normalization: sometimes we get display string, other times amount/currency.
      display = (
         it.get("displayPrice") or it.get("DisplayPrice") or
         (it.get("price") or {}).get("display")
      )
      amount = None
      currency = None
      price_obj = it.get("price") or it.get("Price") or {}
      if isinstance(price_obj, dict):
         currency = price_obj.get("Currency") or price_obj.get("currency")
         # Amount may be in micros/cents; here we accept a float if provided
         if "Amount" in price_obj:
            try:
               amount = float(price_obj["Amount"])
            except Exception:
               amount = None
         elif "value" in price_obj:
            try:
               amount = float(price_obj["value"])
            except Exception:
               amount = None

      price_str = display if isinstance(display, str) else price_to_string(amount, currency)

      # Platforms: Xbox One, Series X|S, PC
      platforms: List[str] = []
      plats = it.get("Platforms") or it.get("platforms") or it.get("PlayableOn") or it.get("playableOn") or []
      if isinstance(plats, list):
         for p in plats:
            if isinstance(p, dict):
               platforms.append(p.get("Name") or p.get("name") or "")
            else:
               platforms.append(str(p))
      elif isinstance(plats, str):
         platforms = [plats]
      platforms = normalize_platforms(platforms)

      raw_rating = it.get("ContentRating") or it.get("contentRating") or it.get("Rating") or it.get("rating")
      if isinstance(raw_rating, dict):
         raw_rating = raw_rating.get("Name") or raw_rating.get("name") or raw_rating.get("value")
      rating = normalize_rating(raw_rating)

      # UUID: productId/legacyId
      uuid = it.get("ProductId") or it.get("productId") or it.get("legacyId") or it.get("id")

      return GameRecord(
         store="xbox",
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
      # Try multiple embedded-data strategies:
      # 1) __NEXT_DATA__
      for rec in self._parse_next_data(html, base_url=url):
         yield rec
      # 2) data-state script blocks
      for rec in self._parse_data_state(html, base_url=url):
         yield rec
      # 3) window.__INITIAL_DATA__
      for rec in self._parse_window_initial(html, base_url=url):
         yield rec
      # 4) JSON-LD
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
      self._walk_and_collect_products(js, out, base_url)
      return out

   def _parse_data_state(self, html: str, *, base_url: str) -> List[Optional[GameRecord]]:
      out: List[Optional[GameRecord]] = []
      for m in _STATE_RE.finditer(html):
         try:
            js = json.loads(m.group(1))
         except Exception:
            continue
         self._walk_and_collect_products(js, out, base_url)
      return out

   def _parse_window_initial(self, html: str, *, base_url: str) -> List[Optional[GameRecord]]:
      out: List[Optional[GameRecord]] = []
      m = _WININIT_RE.search(html)
      if not m:
         return out
      try:
         js = json.loads(m.group(1))
      except Exception:
         return out
      self._walk_and_collect_products(js, out, base_url)
      return out

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

   def _walk_and_collect_products(self, js: Any, out: List[Optional[GameRecord]], base_url: str) -> None:
      def walk(o: Any):
         if isinstance(o, dict):
            # Common keys: "products", "items", "tiles"
            for key in ("products","Products","items","Items","tiles","Tiles","results","Results"):
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

   def _coerce_to_api_like(self, it: Any, base_url: str) -> Dict[str, Any]:
      """
      Map a heterogeneous tile/card object into a common shape used by _normalize_api_item.
      """
      if not isinstance(it, dict):
         return {}
      guess: Dict[str, Any] = {}

      # Name/title
      guess["Title"] = (
         it.get("title") or it.get("Title") or
         it.get("displayName") or it.get("name") or ""
      )

      # Images
      img = (
         it.get("imageUrl") or it.get("ImageUrl") or
         it.get("image") or it.get("Image")
      )
      if not img:
         imgs = it.get("images") or it.get("Images") or []
         if isinstance(imgs, list) and imgs:
            guess["Images"] = imgs
      else:
         guess["ImageUrl"] = img

      # Links
      link = (
         it.get("url") or it.get("Url") or
         it.get("href") or it.get("link")
      )
      if link:
         guess["ProductPageUrl"] = link
      else:
         guess["ProductPageUrl"] = base_url

      # Price
      price = it.get("price") or it.get("Price") or {}
      if isinstance(price, dict):
         guess["price"] = price
      else:
         # Sometimes price surfaces as a string "Included", "$19.99", etc.
         if isinstance(price, str):
            guess["displayPrice"] = price

      # Platforms (if present)
      plats = it.get("platforms") or it.get("Platforms") or it.get("badges") or []
      if isinstance(plats, list):
         guess["Platforms"] = normalize_platforms(plats)
      elif isinstance(plats, str):
         guess["Platforms"] = normalize_platforms([plats])

      # Id
      guess["ProductId"] = it.get("productId") or it.get("id") or it.get("legacyId")

      return guess

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

      platforms: List[str] = []
      # JSON-LD rarely lists Xbox variants explicitly; leave empty unless recognizable
      if "Xbox" in (b.get("name") or ""):
         platforms.append("Xbox")
      platforms = normalize_platforms(platforms)

      return GameRecord(
         store="xbox",
         name=name,
         price=price_str,
         image=str(image) if image else "https://www.xbox.com/etc.clientlibs/settings/wcm/designs/xbox/glyphs/xbox-glyph.png",
         href=str(href),
         uuid=None,
         platforms=platforms,
         rating=None,
         type="game",
      )
