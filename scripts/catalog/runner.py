from __future__ import annotations
from typing import List

from catalog.adapters.base import Adapter
from catalog.io_writer import write_catalog
from catalog.models import GameRecord

async def run_adapter(adapter: Adapter, out_dir: str) -> None:
   async with adapter as a:
      log = a.log
      log.info("[%s] starting scrape", a.store)
      buf: List[GameRecord] = []
      count = 0
      async for rec in a.iter_games():
         buf.append(rec)
         count += 1
         if count % 100 == 0:
            log.info("[%s] collected %d records", a.store, count)
      log.info("[%s] writing %d records", a.store, len(buf))
      write_catalog(out_dir, a.store, buf)
      log.info(
         "[%s] complete (fetched=%d parsed=%d quarantined=%d)",
         a.store,
         a.metrics.get("fetched", 0),
         a.metrics.get("parsed", 0),
         a.metrics.get("quarantined", 0),
      )
