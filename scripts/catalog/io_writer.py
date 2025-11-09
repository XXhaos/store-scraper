from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterable, List, Tuple

from catalog.models import GameRecord, LetterItem
from catalog.normalize import letter_bucket

def write_catalog(out_dir: str, store: str, rows: Iterable[GameRecord]) -> None:
   base = os.path.join(out_dir, store)
   os.makedirs(base, exist_ok=True)

   # Build per-letter arrays and the bang Map-as-array
   buckets: dict[str, List[LetterItem]] = defaultdict(list)
   bang: List[Tuple[str, dict]] = []

   for rec in rows:
      item = LetterItem(
         name=rec.name,
         type=rec.type,
         price=rec.price,
         image=str(rec.image),
         href=str(rec.href),
         uuid=rec.uuid,
         platforms=rec.platforms,
         rating=rec.rating if rec.rating else None
      )
      buckets[letter_bucket(rec.name)].append(item)
      bang.append((rec.name, item.model_dump(mode="json")))

   # stable-ish ordering per-letter
   for k in buckets:
      buckets[k].sort(key=lambda i: i.name.lower())

   # ensure global bang list is sorted for deterministic output
   bang.sort(key=lambda item: item[0].lower())

   # Write per-letter
   for k in sorted(buckets):
      arr = buckets[k]
      with open(os.path.join(base, f"{k}.json"), "w", encoding="utf-8") as fp:
         json.dump([i.model_dump(mode="json") for i in arr], fp, ensure_ascii=False, indent=4)

   # Write metadata and bang files
   metadata = {
      "size": len(bang),
      "date": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
   }
   with open(os.path.join(base, "$.json"), "w", encoding="utf-8") as fp:
      json.dump(metadata, fp, ensure_ascii=False, indent=4)

   with open(os.path.join(base, "!.json"), "w", encoding="utf-8") as fp:
      json.dump(bang, fp, ensure_ascii=False, indent=4)
