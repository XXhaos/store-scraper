from __future__ import annotations
import argparse
import asyncio
import logging

from catalog.adapters.base import AdapterConfig
from catalog.adapters.steam import SteamAdapter
from catalog.adapters.psn import PSNAdapter
from catalog.adapters.xbox import XboxAdapter
from catalog.adapters.nintendo import NintendoAdapter
from catalog.db import CatalogCache, make_session
from catalog.runner import run_adapter

from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

log = logging.getLogger("catalog.crawl")

FACTORY = {
   "steam": lambda c: SteamAdapter(config=c),
   "psn": lambda c: PSNAdapter(config=c),
   "xbox": lambda c: XboxAdapter(config=c),
   "nintendo": lambda c: NintendoAdapter(config=c),
}

async def main():
   ap = argparse.ArgumentParser(description="Crawl game stores and write JSON outputs.")
   ap.add_argument("--stores", type=str, default="psn,xbox,steam,nintendo", help="Comma-separated list of stores: steam,psn,xbox,nintendo")
   ap.add_argument("--out", type=str, default="./out", help="Output directory")
   ap.add_argument("--country", type=str, default="US", help="Region country code (e.g., US)")
   ap.add_argument("--locale", type=str, default="en-US", help="Locale (e.g., en-US)")
   ap.add_argument("--log-level", type=str, default="INFO", help="Logging level (e.g., INFO, DEBUG)")
   ap.add_argument("--cache-db", type=str, default="catalog-cache.db", help="Path or SQLAlchemy URL for the resume cache database")
   ap.add_argument("--no-cache", action="store_true", help="Disable database caching (useful for one-off crawls)")
   ap.add_argument("--no-resume-cache", action="store_true", help="Do not load cached records before crawling")
   ap.add_argument("--cache-commit-interval", type=int, default=50, help="How many records to buffer before committing cache writes")
   args = ap.parse_args()

   logging.basicConfig(
      level=getattr(logging, args.log_level.upper(), logging.INFO),
      format="%(message)s",
      datefmt="[%X]",
      handlers=[RichHandler(rich_tracebacks=True, markup=True)],
   )

   cfg = AdapterConfig(country=args.country, locale=args.locale)
   stores = [s.strip().lower() for s in args.stores.split(",") if s.strip()]

   configured_stores = []
   for s in stores:
      ctor = FACTORY.get(s)
      if not ctor:
         log.warning("Unknown store requested: %s", s)
         continue
      configured_stores.append((s, ctor))

   progress_columns = (
      SpinnerColumn(),
      TextColumn("[progress.description]{task.description}", justify="left"),
      TimeElapsedColumn(),
   )

   cache_db_url = None
   resume_cache = False
   if not args.no_cache and args.cache_db:
      cache_db_url = args.cache_db
      if "://" not in cache_db_url:
         cache_db_url = f"sqlite:///{cache_db_url}"
      resume_cache = not args.no_resume_cache
      log.info("Caching catalogs to %s (resume=%s)", cache_db_url, "yes" if resume_cache else "no")

   if configured_stores:
      log.info("Starting crawl for %d store(s)", len(configured_stores))
      tasks = []
      with Progress(*progress_columns, transient=False) as progress:
         for s, ctor in configured_stores:
            log.info("Scheduling crawl for %s", s)
            task_id = progress.add_task(f"{s}: pending", start=False, total=None)
            cache_obj = None
            if cache_db_url:
               cache_session = make_session(cache_db_url)
               cache_obj = CatalogCache(cache_session, commit_interval=args.cache_commit_interval)
            tasks.append(run_adapter(ctor(cfg), args.out, progress, task_id, cache=cache_obj, resume=resume_cache))

         await asyncio.gather(*tasks)
      log.info("All requested stores completed")
   else:
      log.warning("No valid stores requested")

if __name__ == "__main__":
   asyncio.run(main())
