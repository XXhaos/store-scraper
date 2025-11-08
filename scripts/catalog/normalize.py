import re
from typing import Optional

_MARK_RX = re.compile(r"[™®©]", re.U)
_EDITION_RX = re.compile(
   r"\b(deluxe|definitive|gold|ultimate|goty|complete|remastered|hd|bundle|collection|director'?s cut|edition|standard|launch|classic)\b",
   re.I
)
_PLATFORM_NOISE_RX = re.compile(
   r"\b(ps\s*4|ps\s*5|playstation\s*4|playstation\s*5|xbox(\s+one|\s+series\s+x\|?s)?|series\s+x\|?s|nintendo\s+switch|switch)\b",
   re.I
)

_CURRENCY_SYMBOLS = {
   "USD": "$",
   "CAD": "$",
   "AUD": "$",
   "NZD": "$",
   "EUR": "€",
   "GBP": "£",
   "JPY": "¥",
   "CNY": "¥",
   "HKD": "$",
   "TWD": "$",
   "KRW": "₩",
}

_PLATFORM_MAP = {
   "ps4": "PS4",
   "ps5": "PS5",
   "playstation 4": "PS4",
   "playstation 5": "PS5",
   "ps4 & ps5": "PS4/PS5",
   "ps5|ps4": "PS4/PS5",
   "xbox one": "Xbox One",
   "xbox series x|s": "Xbox Series X|S",
   "xbox series x": "Xbox Series X|S",
   "xbox series s": "Xbox Series X|S",
   "xbox series": "Xbox Series X|S",
   "xbox": "Xbox",
   "windows": "Windows",
   "pc": "PC",
   "steam": "PC",
   "win32": "Windows",
   "switch": "Switch",
   "nintendo switch": "Switch",
   "xbox play anywhere": "Xbox Play Anywhere",
}

_RATING_MAP = {
   "everyone": "everyone",
   "everyone 10+": "everyone 10+",
   "e10+": "everyone 10+",
   "e 10+": "everyone 10+",
   "e 10 plus": "everyone 10+",
   "e for everyone": "everyone",
   "esrb everyone": "everyone",
   "esrb everyone 10+": "everyone 10+",
   "rating pending": "rating pending",
   "rp": "rating pending",
   "teen": "teen",
   "t": "teen",
   "esrb teen": "teen",
   "mature": "mature 17+",
   "mature 17+": "mature 17+",
   "m": "mature 17+",
   "esrb mature": "mature 17+",
   "pegi 3": "everyone",
   "pegi 7": "everyone 10+",
   "pegi 12": "teen",
   "pegi 16": "mature 17+",
   "pegi 18": "mature 17+",
   "cero a": "everyone",
   "cero b": "teen",
   "cero c": "mature 17+",
   "cero d": "mature 17+",
   "cero z": "mature 17+",
}

def clean_title(name: str) -> str:
   t = _MARK_RX.sub("", name or "").strip()
   t = re.sub(r"\s{2,}", " ", t)
   return t

def strip_edition_noise(name: str) -> str:
   t = clean_title(name)
   t = _PLATFORM_NOISE_RX.sub("", t)
   t = _EDITION_RX.sub("", t)
   t = re.sub(r"\s{2,}", " ", t).strip(" -–—")
   return t or clean_title(name)

def price_to_string(amount: Optional[float], currency: Optional[str], *, flags: Optional[str] = None) -> str:
   # Flags can be "Free", "Unavailable", "Announced", etc. If provided, prefer it.
   if flags:
      return flags
   if amount is None or currency is None:
      return "Unavailable"
   cur = (currency or "").upper()
   symbol = _CURRENCY_SYMBOLS.get(cur)
   if symbol in {"¥", "₩"}:
      return f"{symbol}{int(round(amount))}"
   if symbol:
      return f"{symbol}{amount:0.2f}"
   return f"{cur} {amount:0.2f}".strip()

def letter_bucket(name: str) -> str:
   ch = (name or "").strip()[:1].lower()
   if ch >= "a" and ch <= "z":
      return ch
   return "_"

def normalize_rating(value: Optional[str]) -> Optional[str]:
   if not value:
      return None
   v = re.sub(r"[^a-z0-9+ ]+", "", value.lower()).strip()
   return _RATING_MAP.get(v)

def normalize_platform(value: str) -> str:
   if not value:
      return ""
   key = value.strip().lower()
   return _PLATFORM_MAP.get(key, value.strip())

def normalize_platforms(values) -> list[str]:
   out = []
   seen = set()
   for v in values or []:
      norm = normalize_platform(str(v))
      if not norm:
         continue
      key = norm.lower()
      if key in seen:
         continue
      seen.add(key)
      out.append(norm)
   return out

def parse_price_string(value: str) -> Optional[float]:
   if not value or value.lower() in {"free", "unavailable"}:
      return 0.0 if value and value.lower() == "free" else None
   m = re.search(r"([0-9]+(?:[\.,][0-9]{2})?)", value)
   if not m:
      return None
   amt = m.group(1).replace(",", "")
   try:
      return float(amt)
   except ValueError:
      return None
