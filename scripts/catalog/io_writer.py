from __future__ import annotations
import json, os
from collections import defaultdict
from typing import Iterable, Tuple, List
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
      bang.append([rec.name, item.model_dump()])  # 2-item list

   # stable-ish ordering per-letter
   for k in buckets:
      buckets[k].sort(key=lambda i: i.name.lower())

   # Write per-letter
   for k, arr in buckets.items():
      with open(os.path.join(base, f"{k}.json"), "w", encoding="utf-8") as fp:
         json.dump([i.model_dump() for i in arr], fp, ensure_ascii=False, indent=2)

   # Write bang file
   with open(os.path.join(base, "!.json"), "w", encoding="utf-8") as fp:
      json.dump(bang, fp, ensure_ascii=False, indent=2)
