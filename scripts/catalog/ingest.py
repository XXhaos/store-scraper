from __future__ import annotations

import argparse
import json
import logging
import os
from collections import defaultdict
from typing import Dict, Iterable, List

from catalog.dedupe import canonical_key
from catalog.io_writer import write_catalog
from catalog.models import GameRecord
from catalog.normalize import (
   clean_title,
   strip_edition_noise,
   normalize_platforms,
   normalize_rating,
   parse_price_string,
)


log = logging.getLogger("catalog.ingest")


def load_store_records(root: str, store_dir: str) -> List[GameRecord]:
   bang_path = os.path.join(root, store_dir, "!.json")
   if not os.path.exists(bang_path):
      log.debug("skip %s (no !.json)", store_dir)
      return []

   with open(bang_path, "r", encoding="utf-8") as fp:
      bang = json.load(fp)

   base_store = store_dir.split("-", 1)[0]
   records: List[GameRecord] = []
   for entry in bang:
      if not isinstance(entry, list) or len(entry) != 2:
         continue
      name, payload = entry
      if not isinstance(payload, dict):
         continue
      data = dict(payload)
      data.setdefault("name", name)
      data.setdefault("platforms", [])
      data.setdefault("rating", None)
      data.setdefault("type", None)
      data.setdefault("uuid", payload.get("uuid"))
      data["store"] = base_store
      try:
         record = GameRecord(**data)
      except Exception as exc:
         log.warning("Unable to coerce record from %s/%s: %s", store_dir, name, exc)
         continue
      record.extra.setdefault("source_store", store_dir)
      records.append(record)
   return records


def group_by_canonical(records: Iterable[GameRecord]) -> Dict[str, List[GameRecord]]:
   buckets: Dict[str, List[GameRecord]] = defaultdict(list)
   for record in records:
      buckets[canonical_key(record.name)].append(record)
   return buckets


def merge_cluster(records: List[GameRecord]) -> GameRecord:
   records = list(records)
   records.sort(key=lambda r: (r.extra.get("source_store", r.store), r.name.lower()))
   base = records[0].model_copy(deep=True)
   base_name = max(records, key=lambda r: len(r.name or ""))
   base.name = strip_edition_noise(clean_title(base_name.name))

   platforms = []
   rating = base.rating
   best_price = base.price
   best_price_value = parse_price_string(best_price)
   seen_platforms = set()
   sources: List[Dict[str, object]] = []
   price_map: Dict[str, str] = {}
   uuid_list: List[str] = []

   def _merge_platforms(items: Iterable[str]) -> None:
      nonlocal platforms, seen_platforms
      for plat in normalize_platforms(items):
         key = plat.lower()
         if key in seen_platforms:
            continue
         seen_platforms.add(key)
         platforms.append(plat)

   placeholder_tokens = {"placeholder", "generic"}
   for record in records:
      source_name = record.extra.get("source_store", record.store)
      sources.append(
         {
            "store": source_name,
            "href": str(record.href),
            "price": record.price,
            "platforms": list(record.platforms),
            "uuid": record.uuid,
         }
      )
      price_map[source_name] = record.price
      if record.uuid and record.uuid not in uuid_list:
         uuid_list.append(record.uuid)

      _merge_platforms(record.platforms)

      if record.rating and not rating:
         rating = normalize_rating(record.rating) or rating

      value = parse_price_string(record.price)
      if best_price_value is None or (value is not None and (best_price_value is None or value < best_price_value)):
         best_price = record.price
         best_price_value = value

      # Prefer the first non-placeholder image/href/type we encounter
      if base.image and isinstance(base.image, str):
         low = base.image.lower()
      else:
         low = ""
      if (not base.image) or any(tok in low for tok in placeholder_tokens):
         base.image = record.image
      if not base.href or base.href == "":
         base.href = record.href
      if not base.type and record.type:
         base.type = record.type

   base.platforms = platforms
   base.rating = rating or None
   base.price = best_price
   if uuid_list and not base.uuid:
      base.uuid = uuid_list[0]

   base.extra = {
      "sources": sources,
      "prices": price_map,
   }
   if uuid_list:
      base.extra["uuids"] = uuid_list

   return base


def merge_catalog(records: Iterable[GameRecord]) -> List[GameRecord]:
   merged: List[GameRecord] = []
   for _, cluster_records in group_by_canonical(records).items():
      merged.append(merge_cluster(cluster_records))
   merged.sort(key=lambda r: r.name.lower())
   return merged


def main() -> None:
   ap = argparse.ArgumentParser(description="Merge store-specific catalogs into unified outputs.")
   ap.add_argument("--input", type=str, default="./out", help="Directory containing per-store outputs")
   ap.add_argument("--output", type=str, default=None, help="Destination directory for merged catalogs")
   ap.add_argument(
      "--stores",
      type=str,
      default="psn,xbox,nintendo,steam",
      help="Comma-separated list of base store names to merge (use 'all' for every store)",
   )
   args = ap.parse_args()

   logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

   out_dir = args.output or os.path.join(args.input, "merged")
   os.makedirs(out_dir, exist_ok=True)

   requested = {s.strip().lower() for s in args.stores.split(",") if s.strip()}
   include_all = "all" in requested or "*" in requested

   groups: Dict[str, List[GameRecord]] = defaultdict(list)

   for entry in sorted(os.listdir(args.input)):
      if entry in {".", ".."}:
         continue
      full_path = os.path.join(args.input, entry)
      if not os.path.isdir(full_path):
         continue
      records = load_store_records(args.input, entry)
      if not records:
         continue
      base = entry.split("-", 1)[0]
      if not include_all and base not in requested:
         log.debug("Skipping %s (base %s not requested)", entry, base)
         continue
      log.info("Loaded %d records from %s", len(records), entry)
      groups[base].extend(records)

   if not groups:
      log.warning("No catalogs matched input criteria")
      return

   for base, records in groups.items():
      merged = merge_catalog(records)
      log.info("Writing merged catalog for %s (%d titles)", base, len(merged))
      write_catalog(out_dir, base, merged)


if __name__ == "__main__":
   main()
