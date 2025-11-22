#!/usr/bin/env python3
import json
from pathlib import Path
import argparse


def load_json(path: Path):
   with path.open("r", encoding="utf-8-sig") as f:
      return json.load(f)


def save_json(path: Path, data):
   with path.open("w", encoding="utf-8") as f:
      json.dump(data, f, ensure_ascii=False, indent=3)
      f.write("\n")


def main():
   p = argparse.ArgumentParser(description="Merge !.json into steam.json applist.apps")
   p.add_argument("--map", default="!.json", help="The map file (default: !.json)")
   p.add_argument("--cache", default="steam.json", help="The cache file (default: steam.json)")
   p.add_argument("--output", default=None, help="Output file (default: overwrite cache)")
   args = p.parse_args()

   map_path = Path(args.map)
   cache_path = Path(args.cache)
   out_path = Path(args.output) if args.output else cache_path

   # Load !.json (array of [name, details])
   entries = load_json(map_path)

   # Load steam.json
   cache = load_json(cache_path)
   apps = cache.get("applist", {}).get("apps", [])

   # Build quick lookup
   existing = {int(app["appid"]) for app in apps if "appid" in app}

   added = 0
   for pair in entries:
      if not isinstance(pair, list) or len(pair) != 2:
         continue

      _, details = pair
      if not isinstance(details, dict):
         continue

      # Required fields
      if "uuid" not in details or "name" not in details:
         continue

      try:
         appid = int(details["uuid"])
      except ValueError:
         continue

      name = details["name"]

      # Skip if already present
      if appid in existing:
         continue

      # Add new entry
      apps.append({
         "appid": appid,
         "name": name
      })
      existing.add(appid)
      added += 1

   # Sort for consistency
   apps.sort(key=lambda x: x["appid"])

   save_json(out_path, cache)

   print(f"Added {added} new entries.")
   print(f"Written: {out_path}")


if __name__ == "__main__":
   main()
