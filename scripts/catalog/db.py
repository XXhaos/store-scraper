# catalog/db.py
from __future__ import annotations

from datetime import datetime
import time
from typing import List, Sequence

from sqlalchemy import (
   JSON,
   Column,
   DateTime,
   Integer,
   String,
   UniqueConstraint,
   create_engine,
   delete,
   select,
)
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.exc import OperationalError

from catalog.models import GameRecord

Base = declarative_base()
_ENGINES: dict[str, Engine] = {}


class CachedGameRow(Base):
   __tablename__ = "cached_games"

   id = Column(Integer, primary_key=True)
   store = Column(String(64), nullable=False)
   cache_key = Column(String(256), nullable=False)
   payload = Column(JSON, nullable=False)
   updated_at = Column(DateTime(timezone=False), nullable=False, default=datetime.utcnow)

   __table_args__ = (UniqueConstraint("store", "cache_key", name="u_store_cache_key"),)


def _resolve_url(url: str) -> str:
   return url if "://" in url else f"sqlite:///{url}"


def _get_engine(url: str) -> Engine:
   eng = _ENGINES.get(url)
   if eng is None:
      engine_kwargs = {"future": True}
      parsed = make_url(url)
      if parsed.drivername.startswith("sqlite"):
         engine_kwargs["connect_args"] = {"timeout": 30, "check_same_thread": False}
      eng = create_engine(url, **engine_kwargs)
      Base.metadata.create_all(eng)
      _ENGINES[url] = eng
   return eng


def make_session(url: str = "sqlite:///catalog-cache.db") -> Session:
   """Create a synchronous SQLAlchemy session for the crawler cache."""

   url = _resolve_url(url)
   engine = _get_engine(url)
   return sessionmaker(bind=engine, expire_on_commit=False)()


def cache_key_for_record(record: GameRecord) -> str:
   return record.uuid or str(record.href)


class CatalogCache:
   """Simple helper for persisting adapter progress between runs."""

   def __init__(self, session: Session, *, commit_interval: int = 50):
      self._session = session
      self._commit_interval = max(1, commit_interval)
      self._pending_writes = 0

   def load(self, store: str) -> List[GameRecord]:
      """Load cached records for *store*."""

      rows = (
         self._session.execute(
            select(CachedGameRow).where(CachedGameRow.store == store)
         )
         .scalars()
         .all()
      )
      records: List[GameRecord] = []
      for row in rows:
         payload = row.payload or {}
         payload.setdefault("store", store)
         try:
            records.append(GameRecord.model_validate(payload))
         except Exception:
            # If a payload can no longer be validated, drop it so it doesn't
            # poison the cache forever. The adapter will refresh it shortly.
            self._session.delete(row)
      self._session.commit()
      return records

   def store_record(self, record: GameRecord) -> None:
      """Insert or update *record* inside the cache."""

      key = cache_key_for_record(record)
      payload = record.model_dump(mode="json")
      payload.setdefault("store", record.store)
      now = datetime.utcnow()

      with self._session.no_autoflush:
         existing = (
            self._session.execute(
               select(CachedGameRow)
               .where(CachedGameRow.store == record.store)
               .where(CachedGameRow.cache_key == key)
            )
            .scalars()
            .first()
         )

      if existing:
         existing.payload = payload
         existing.updated_at = now
      else:
         self._session.add(
            CachedGameRow(
               store=record.store,
               cache_key=key,
               payload=payload,
               updated_at=now,
            )
         )

      self._pending_writes += 1
      if self._pending_writes >= self._commit_interval:
         self.flush()

   def sync_keys(self, store: str, keys: Sequence[str]) -> None:
      """Remove cached rows for *store* that are no longer present."""

      key_list = list(keys)
      stmt = delete(CachedGameRow).where(CachedGameRow.store == store)
      if key_list:
         max_sqlite_variables = 900
         for start in range(0, len(key_list), max_sqlite_variables):
            chunk = key_list[start : start + max_sqlite_variables]
            stmt = stmt.where(~CachedGameRow.cache_key.in_(chunk))
      self._session.execute(stmt)
      self.flush()

   def flush(self) -> None:
      if self._pending_writes or self._session.new or self._session.dirty:
         retries = 5
         delay = 0.1
         for attempt in range(retries):
            try:
               self._session.commit()
               break
            except OperationalError as exc:
               message = str(exc).lower()
               if "database is locked" not in message:
                  raise
               self._session.rollback()
               if attempt == retries - 1:
                  raise
               time.sleep(delay)
               delay *= 2
      self._pending_writes = 0

   def close(self) -> None:
      try:
         self.flush()
      finally:
         self._session.close()
