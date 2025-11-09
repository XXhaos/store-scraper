from __future__ import annotations
import argparse
import asyncio
import logging

from catalog.adapters.base import AdapterConfig
from catalog.adapters.steam import SteamAdapter
from catalog.adapters.psn import PSNAdapter
from catalog.adapters.xbox import XboxAdapter
from catalog.adapters.nintendo import NintendoAdapter
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
   ap.add_argument("--stores", type=str, default="steam",
                   help="Comma-separated list of stores: steam,psn,xbox,nintendo")
   ap.add_argument("--out", type=str, default="./out", help="Output directory")
   ap.add_argument("--country", type=str, default="US", help="Region country code (e.g., US)")
   ap.add_argument("--locale", type=str, default="en-US", help="Locale (e.g., en-US)")
   ap.add_argument("--log-level", type=str, default="INFO", help="Logging level (e.g., INFO, DEBUG)")
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

   if configured_stores:
      log.info("Starting crawl for %d store(s)", len(configured_stores))
      tasks = []
      with Progress(*progress_columns, transient=False) as progress:
         for s, ctor in configured_stores:
            log.info("Scheduling crawl for %s", s)
            task_id = progress.add_task(f"{s}: pending", start=False, total=None)
            tasks.append(run_adapter(ctor(cfg), args.out, progress, task_id))

         await asyncio.gather(*tasks)
      log.info("All requested stores completed")
   else:
      log.warning("No valid stores requested")

if __name__ == "__main__":
   asyncio.run(main())
