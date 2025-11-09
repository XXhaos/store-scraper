from __future__ import annotations
from typing import List, Optional

from rich.progress import Progress

from catalog.adapters.base import Adapter
from catalog.io_writer import write_catalog
from catalog.models import GameRecord

async def run_adapter(
   adapter: Adapter,
   out_dir: str,
   progress: Optional[Progress] = None,
   task_id: Optional[int] = None,
) -> None:
   async with adapter as a:
      log = a.log
      log.info("[%s] starting scrape", a.store)
      buf: List[GameRecord] = []
      count = 0
      if progress is not None and task_id is not None:
         progress.update(task_id, description=f"{a.store}: starting scrape")
         progress.start_task(task_id)
      async for rec in a.iter_games():
         buf.append(rec)
         count += 1
         if count % 100 == 0:
            log.info("[%s] collected %d records", a.store, count)
         if progress is not None and task_id is not None:
            progress.update(task_id, description=f"{a.store}: collected {count} records")
      log.info("[%s] writing %d records", a.store, len(buf))
      if progress is not None and task_id is not None:
         progress.update(task_id, description=f"{a.store}: writing {len(buf)} records")
      write_catalog(out_dir, a.store, buf)
      log.info(
         "[%s] complete (fetched=%d parsed=%d quarantined=%d)",
         a.store,
         a.metrics.get("fetched", 0),
         a.metrics.get("parsed", 0),
         a.metrics.get("quarantined", 0),
      )
      if progress is not None and task_id is not None:
         progress.update(
            task_id,
            description=(
               f"{a.store}: fetched {a.metrics.get('fetched', 0)} "
               f"parsed {a.metrics.get('parsed', 0)} "
               f"quarantined {a.metrics.get('quarantined', 0)}"
            ),
         )
         progress.stop_task(task_id)
