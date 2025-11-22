from __future__ import annotations
import abc, asyncio, logging
from dataclasses import dataclass
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Protocol

import httpx

from catalog.http import fetch, DomainLimiter, make_client
from catalog.models import GameRecord

@dataclass(slots=True)
class AdapterConfig:
   country: str = "US"
   locale: str = "en-US"
   rps: float = 2.5              # requests per second for this domain
   timeout: float = 30.0         # seconds

@dataclass(slots=True)
class Capabilities:
   pagination: bool = False
   needs_headless: bool = False
   returns_partial_price: bool = False  # e.g., region-gated
   yields_dlc: bool = False             # if True, adapters should filter or tag

@dataclass(slots=True)
class FetchContext:
   client: httpx.AsyncClient
   limiter: DomainLimiter

@dataclass(slots=True)
class RecordResult:
   ok: bool
   record: Optional[GameRecord] = None
   error: Optional[str] = None
   raw: Optional[Dict[str, Any]] = None  # stash raw for quarantine/debug

class SupportsAdapter(Protocol):
   store: str
   config: AdapterConfig
   capabilities: Capabilities

class Adapter(abc.ABC):
   """
   Base class for all store adapters.

   Usage:
      async with SteamAdapter() as a:
         async for rec in a.iter_games():
            ...
   """
   store: str = "unknown"
   capabilities: Capabilities = Capabilities()

   def __init__(self, *, config: AdapterConfig | None = None,
                http: httpx.AsyncClient | None = None,
                limiter: DomainLimiter | None = None,
                logger: logging.Logger | None = None):
      self.config = config or AdapterConfig()
      self._external_http = http
      self._http = http
      self._limiter = limiter or DomainLimiter(self.config.rps)
      self.log = logger or logging.getLogger(f"catalog.{self.store}")
      # lightweight counters
      self.metrics: Dict[str, int] = {"fetched": 0, "parsed": 0, "quarantined": 0}

   # -------- lifecycle ------------------------------------------------------

   async def __aenter__(self) -> "Adapter":
      if self._http is None:
         # create managed client
         self._client_cm = make_client(timeout=self.config.timeout)
         self._http = await self._client_cm.__aenter__()
      return self

   async def __aexit__(self, exc_type, exc, tb):
      if self._http is not None and self._external_http is None:
         # close managed client
         await self._client_cm.__aexit__(exc_type, exc, tb)
         self._http = None

   # -------- HTTP helpers ---------------------------------------------------

   async def request(self, method: str, url: str, **kw) -> httpx.Response:
      """All network I/O goes through here (rate limit + retries)."""
      assert self._http is not None, "Adapter must be used inside 'async with' or injected with a client"
      r = await fetch(self._http, method, url, limiter=self._limiter, **kw)
      self.metrics["fetched"] += 1
      return r

   async def get_json(self, url: str, **kw) -> Dict[str, Any]:
      r = await self.request("GET", url, **kw)
      return r.json()

   async def get_text(self, url: str, **kw) -> str:
      r = await self.request("GET", url, **kw)
      return r.text

   # -------- iterator contract ---------------------------------------------

   @abc.abstractmethod
   async def iter_games(self) -> AsyncIterator[GameRecord]:
      """
      Yield normalized GameRecord objects.

      Notes:
         - Apply country/locale from self.config
         - Avoid raising on per-item parse errors; quarantine with self.quarantine(...)
      """
      ...

   def resume(self, records: List[GameRecord]) -> None:
      """Adapters can override to optimize when resuming from cached records."""
      return None

   # -------- optional child catalog support -------------------------------

   def child_catalogs(self, rows: List[GameRecord]) -> Dict[str, List[GameRecord]]:
      """
      Optionally return additional catalogs derived from the adapter's output.

      The default implementation returns an empty mapping; adapters can
      override this to build platform-specific (or other derived) catalogs.
      Keys should correspond to the store directory names to be written.
      """
      return {}

   # -------- utilities for adapters ----------------------------------------

   def quarantine(self, *, error: str, raw: Dict[str, Any] | None = None) -> RecordResult:
      self.metrics["quarantined"] += 1
      # keep log terse but actionable
      self.log.warning("%s: quarantine: %s", self.store, error)
      return RecordResult(ok=False, error=error, raw=raw)

   def ok(self, record: GameRecord, raw: Dict[str, Any] | None = None) -> RecordResult:
      self.metrics["parsed"] += 1
      return RecordResult(ok=True, record=record, raw=raw)

   async def paginate(
      self,
      *,
      start: int = 0,
      page_size: int = 50,
      fetch_page: Callable[[int, int], "asyncio.Future[Dict[str, Any]]"],
      has_more: Callable[[Dict[str, Any]], bool],
   ):
      """
      Generic pagination helper:
         async for page in self.paginate(start=0, page_size=100, fetch_page=..., has_more=...):
            ...
      """
      cursor = start
      while True:
         js = await fetch_page(cursor, page_size)
         yield js
         if not has_more(js):
            break
         cursor += page_size
