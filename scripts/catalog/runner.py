from __future__ import annotations
from typing import Dict, List, Optional

from rich.progress import Progress

from catalog.adapters.base import Adapter
from catalog.db import CatalogCache, cache_key_for_record
from catalog.io_writer import write_catalog
from catalog.models import GameRecord

async def run_adapter(
   adapter: Adapter,
   out_dir: str,
   progress: Optional[Progress] = None,
   task_id: Optional[int] = None,
   *,
   cache: CatalogCache | None = None,
   resume: bool = True,
) -> None:
   try:
      async with adapter as a:
         log = a.log
         log.info("[%s] starting scrape", a.store)
         buf: List[GameRecord] = []
         index_by_key: Dict[str, int] = {}
         count = 0

         if cache is not None and resume:
            cached_records = cache.load(a.store)
            if cached_records:
               log.info("[%s] resuming with %d cached records", a.store, len(cached_records))
               buf.extend(cached_records)
               for idx, rec in enumerate(buf):
                  index_by_key[cache_key_for_record(rec)] = idx
               count = len(buf)
               try:
                  a.resume(cached_records)
               except Exception as exc:
                  log.warning("[%s] adapter resume hook failed: %s", a.store, exc)
               if progress is not None and task_id is not None:
                  progress.update(task_id, description=f"{a.store}: resumed {count} cached records")

         if progress is not None and task_id is not None:
            progress.update(task_id, description=f"{a.store}: starting scrape")
            progress.start_task(task_id)
         async for rec in a.iter_games():
            key = cache_key_for_record(rec)
            existing_idx = index_by_key.get(key)
            if existing_idx is not None:
               buf[existing_idx] = rec
            else:
               index_by_key[key] = len(buf)
               buf.append(rec)
               count += 1
            if cache is not None:
               cache.store_record(rec)
            if count % 100 == 0:
               log.info("[%s] collected %d records", a.store, count)
            if progress is not None and task_id is not None:
               progress.update(task_id, description=f"{a.store}: collected {count} records")
         log.info("[%s] writing %d records", a.store, len(buf))
         if progress is not None and task_id is not None:
            progress.update(task_id, description=f"{a.store}: writing {len(buf)} records")
         write_catalog(out_dir, a.store, buf)
         for child_store, child_rows in (a.child_catalogs(buf) or {}).items():
            if not child_rows:
               continue
            log.info(
               "[%s] writing %d records to child catalog %s",
               a.store,
               len(child_rows),
               child_store,
            )
            write_catalog(out_dir, child_store, child_rows)
         log.info(
            "[%s] complete (fetched=%d parsed=%d quarantined=%d)",
            a.store,
            a.metrics.get("fetched", 0),
            a.metrics.get("parsed", 0),
            a.metrics.get("quarantined", 0),
         )
         if cache is not None:
            cache.sync_keys(a.store, index_by_key.keys())
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
   finally:
      if cache is not None:
         cache.close()
