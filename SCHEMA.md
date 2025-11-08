# This is the project's layout

game-store-catalog-py/
    ├─ catalog/
    │   ├─ __main__.py
    │   ├─ __init__.py
    │   ├─ models.py            # Pydantic models that mirror your JSON schema
    │   ├─ normalize.py         # title/price/date/platform normalization
    │   ├─ io_writer.py         # writes !.json and a..z/_.json in your exact format
    │   ├─ http.py              # resilient HTTP (rate limit, retries, backoff)
    │   ├─ dedupe.py            # optional cross-store clustering (title/year/publisher)
    │   ├─ ingest.py            # staging/merge helpers (SQLite/Postgres optional)
    │   ├─ adapters/
    │   │   ├─ __init__.py
    │   │   ├─ base.py          # Adapter interface
    │   │   ├─ steam.py         # Working example via public endpoints
    │   │   ├─ psn.py           # Skeleton (PlayStation)
    │   │   ├─ xbox.py          # Skeleton (Xbox)
    │   │   └─ nintendo.py      # Skeleton (Nintendo)
    │   └─ runner.py            # Orchestrates adapters, validation, writing
    ├─ scripts/
    │   └─ crawl.py             # CLI entrypoint
    ├─ tests/                   # room for pytest tests (fixtures later)
    ├─ pyproject.toml
    └─ README.md
