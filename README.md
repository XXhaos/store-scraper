# Overview

This project is used to update the databases used by [Game Store Catalog](https://github.com/Ephellon/game-store-catalog)—a cross-store indexing and data standardization project designed to maintain accurate, normalized information about games across multiple digital storefronts.

### Supported Stores

* Steam
* PlayStation Network (PSN)
* Xbox Store
* Nintendo eShop

### Purpose

* Aggregate and normalize game information from various platforms.
* Export consistent, language-agnostic JSON files for external applications, web frontends, or dashboards.
* Enable community-driven maintenance and mirroring of catalog data.

---

## JSON Schema

The project outputs **two JSON file types**. See the [online builder](https://minkcbos.retool.com/app/game-store-catalog) to help make URL schemas and see examples.

### 1. Bang File: `!.json`

A key/value map represented as a list of `[name, { ...props }]` pairs.
Each entry corresponds to a single game and includes metadata fields.

**Example:**

```json
[
  ["The Legend of Zelda: Tears of the Kingdom", {
    "type": "game",
    "price": "$69.99",
    "image": "https://assets.nintendo.com/zelda.jpg",
    "href": "https://store.nintendo.com/...",
    "uuid": "ZELDA-TOTK",
    "platforms": ["Switch"],
    "rating": "everyone 10+"
  }]
]
```

### 2. Per-letter Files: `_.json`, `a.json`, `b.json`, … `z.json`

Each file is an array of simple objects used for alphabetical browsing.

**Example:**

```json
[
  {
    "name": "A Plague Tale: Requiem",
    "type": "game",
    "price": "$49.99",
    "image": "https://cdn.store.steamstatic.com/...jpg",
    "href": "https://store.steampowered.com/app/1182900/",
    "uuid": "1182900",
    "platforms": ["Windows", "PS5", "Xbox Series X"],
    "rating": "mature 17+"
  }
]
```

---

## Architecture

### Adapter-based design

Each store uses a dedicated adapter implementing a common interface.
This makes the project modular and easy to extend.

```
/adapters
 ├─ steam.py       → Steam Store API
 ├─ psn.py         → PlayStation Store API
 ├─ xbox.py        → Microsoft Store API
 └─ nintendo.py    → Nintendo eShop
```

Adapters yield normalized `GameRecord` objects that match the schema above.

### Core Modules

* **models.py** — Defines the canonical data model.
* **normalize.py** — Cleans and formats raw fields.
* **http.py** — Handles throttled, retried HTTP requests.
* **io_writer.py** — Writes normalized JSON output (bang and per-letter files).
* **runner.py** — Orchestrates multiple adapters and output pipelines.

### Database (optional)

For persistent storage, you may stage game data into SQLite/PostgreSQL tables and merge changes via content hashes.

---

## Python Implementation

A new, resilient implementation written in **Python 3.11+** adds robust networking, validation, and normalization.
It exports data that exactly matches the JSON produced by the original project.

### Key Features

* Per-domain rate limiting, backoff, and retry logic.
* Unified price, title, and date normalization.
* Modular adapter system for multiple stores.
* Structured output matching `game-store-catalog`.
* Optional deduplication and data merge pipeline.
* Fully type-checked and tested with `pydantic`, `pytest`, and `ruff`.

---

## Installation & Setup

### Requirements

* Python 3.11+
* `httpx`, `pydantic`, `aiolimiter`, `tenacity`

### Quick Start

```bash
# Create a virtual environment
python -m venv .venv && source .venv/bin/activate  # (Windows: .venv\Scripts\activate)

# Install dependencies
pip install -e ".[dev]"

# Run a single store (Steam)
python scripts/crawl.py --stores steam --out ./out --country US --locale en-US

# Run multiple stores
python scripts/crawl.py --stores steam,psn,xbox,nintendo --out ./out
```

Outputs will appear as:

```
out/
 └─ steam/
    ├─ !.json
    ├─ _.json
    ├─ a.json
    ├─ b.json
    └─ ...
```

---

## Design Notes

* **Adapters:** One per store; each yields normalized records.
* **HTTP:** Centralized client with rate limiting and retries.
* **Normalization:** Cleans titles (™/®), standardizes pricing and release dates.
* **Validation:** Pydantic models enforce structure and type safety.
* **Writing:** Exports files in the same layout as the original catalog.
* **Optional Merge:** Stage → dedupe → merge with hash comparison.

### Reliability Enhancements

* Retries and exponential backoff for unstable endpoints.
* Avoids prototype modification (no patching built-ins).
* Respects store rate limits and TOS.
* Unified schema validation prevents malformed JSON.

---

## Example Project Layout

```
game-catalog-py/
 ├─ catalog/
 │   ├─ models.py
 │   ├─ normalize.py
 │   ├─ http.py
 │   ├─ io_writer.py
 │   ├─ adapters/
 │   │   ├─ steam.py
 │   │   ├─ psn.py
 │   │   ├─ xbox.py
 │   │   └─ nintendo.py
 │   └─ runner.py
 ├─ scripts/
 │   └─ crawl.py
 ├─ tests/
 └─ README.md
```

---

## Contributing

* Follow 3-space indentation for Python.
* Run `ruff` and `pytest` before submitting changes.
* Each adapter should return clean `GameRecord` objects.
* PRs adding new stores should include sample output and documentation.

---

## License

MIT — © Ephellon and contributors.
