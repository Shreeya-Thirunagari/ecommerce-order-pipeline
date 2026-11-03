# Yottaflex Local-First Order Data Pipeline

A complete, runnable local data pipeline that lands e-commerce order files into three zones (**raw → validated → curated**), handles late/out-of-order data, duplicates, bad records, partitioning, and safe replay/idempotency.

**Target audience**: 0–3 years experience data engineers.  
**Stack**: Python 3.10+, Pandas, Pydantic, PyArrow, Pytest. Pure local folders (no cloud required).

---

## Architecture / Flow

```
Incoming CSVs (data/sample or any folder)
        │
        ▼
┌───────────────────┐
│   INGESTION       │  Copy + enrich with source_file, ingestion_ts, run_id
│   (raw zone)      │  Content-hash recorded for idempotency
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│   VALIDATION      │  Pydantic schema + business rules
│                   │  Good rows → validated zone
│                   │  Bad rows  → quarantine (with rejection_reason)
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│   TRANSFORMATION    │  Deduplicate on (order_id, product_id)
│   (curated zone)  │  Partition by event_date (Hive-style)
│                   │  CSV output, append-safe re-runs
└───────────────────┘
```

Every run receives a unique `run_id`. Metrics (counts, status, rejection breakdown, duration) are written to `data/logs/run_<run_id>.json`.

### Zones

| Zone        | Purpose                                      | Format          |
|-------------|----------------------------------------------|-----------------|
| `raw/`      | Exact landing + metadata columns             | CSV             |
| `validated/`| Schema-passed records (audit trail)          | CSV             |
| `curated/`  | Clean, deduplicated, partitioned analytics data | CSV (Hive-style folders) |
| `quarantine/` | Rejected records + reason + source linkage | CSV             |
| `state/`    | Processed-file manifest (content hashes)     | JSON            |

---

## Domain & Data Dictionary

**Domain**: E-commerce order line items.  
**Grain**: One row per product within an order.  
**Business primary key**: `(order_id, product_id)`.

| Column       | Type          | Nullable | Description                          | Rules |
|--------------|---------------|----------|--------------------------------------|-------|
| order_id     | string        | No       | Order identifier                     | non-empty |
| product_id   | string        | No       | Product SKU                          | non-empty |
| customer_id  | string        | No       | Customer identifier                  | non-empty |
| order_ts     | datetime      | No       | Order placement time (UTC preferred) | not >1 day in future |
| event_date   | date          | No       | Derived from order_ts                | partition key |
| quantity     | integer       | No       | Units ordered                        | ≥ 1 |
| unit_price   | decimal(10,2) | No       | Price per unit                       | ≥ 0 |
| currency     | string        | No       | ISO code                             | USD, EUR, GBP, INR |
| status       | string        | No       | Order lifecycle state                | pending / confirmed / shipped / delivered / cancelled |
| source_file  | string        | No       | Original filename (added at ingest)  | metadata |
| ingestion_ts | datetime      | No       | When the file was landed             | metadata |
| run_id       | string        | No       | Pipeline run identifier              | metadata |

### Main Business Rules
1. All required columns must be present and non-null.
2. Quantity ≥ 1, unit_price ≥ 0.
3. Currency and status must belong to the controlled lists above.
4. `order_ts` more than `max_future_days` (default 1) in the future is rejected.
5. Records older than `late_data_days` (default 30) are **accepted** but flagged `is_late=True`.
6. Duplicate `(order_id, product_id)` → keep the row with the latest `order_ts` (then latest `ingestion_ts`).
7. Re-processing the exact same file content is a no-op for the curated zone (content-hash manifest).

---

## Assumptions
- Input files are CSV with a header row.
- Timestamps without timezone are treated as UTC.
- A single pipeline process runs at a time (no distributed locking).
- For this educational pipeline, curated partitions are overwritten/merged on each write; production would use more sophisticated compaction or a table format (Iceberg/Delta).

---

## Setup & Run Instructions (from a clean environment)

```bash
# 1. Clone / enter the project
cd yottaflex-data-pipeline

# 2. Create and activate a virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Generate the reproducible sample dataset
python scripts/generate_sample_data.py

# 5. (Optional) Generate a larger file (~120k rows)
python scripts/generate_large_data.py
# or: python scripts/generate_large_data.py --rows 250000

# 6. Run the pipeline against the sample data
python -m src.pipeline
# or point at any folder:
# python -m src.pipeline --incoming data/sample
# Force re-processing even if already seen:
# python -m src.pipeline --force

# 7. Run the automated tests
PYTHONPATH=. pytest tests/ -v
```

After a successful run you will find:
- Enriched files under `data/raw/`
- Clean validated CSVs under `data/validated/`
- Rejected rows under `data/quarantine/`
- Partitioned CSVs under `data/curated/event_date=YYYY-MM-DD/`
- Run metrics under `data/logs/run_*.json`
- Idempotency manifest under `data/state/processed_manifest.json`

---

## Observability

Every run logs and persists:
- `run_id`, start/end timestamps, duration
- input file count & record count
- accepted / rejected / late / duplicate counts
- rejection-reason histogram
- list of processed vs skipped files
- overall status (`success` | `partial` | `failed`) and optional failure reason

Example metric file snippet:
```json
{
  "run_id": "a1b2c3...",
  "status": "success",
  "input_records": 42,
  "accepted_records": 35,
  "rejected_records": 7,
  "rejection_reasons": {
    "quantity:Input should be greater than or equal to 1": 2
  },
  "duration_seconds": 0.85
}
```

---

## Idempotency, Retry & Backfill Behaviour

| Scenario                        | Behaviour |
|---------------------------------|-----------|
| Same file content re-submitted  | Skipped (hash already in manifest) |
| `--force` flag                  | Re-processes file; curated still deduplicates on PK |
| Crash after raw write, before curated | Re-run is safe; raw is re-created with new run_id, curated merges & dedups |
| Late-arriving data              | Accepted into the correct `event_date` partition |
| Partial file failure            | Other files continue; status becomes `partial` |

To clear the idempotency state (e.g. after intentional data fix):
```bash
rm data/state/processed_manifest.json
```

---

## Reconciliation / Validation Report

After a run you can quickly reconcile source → target:

```python
import pandas as pd
from pathlib import Path

# Source count
src = pd.read_csv("data/sample/orders_clean.csv")
print("Source rows:", len(src))

# Curated count
curated_rows = 0
for f in Path("data/curated").rglob("*.csv"):
    curated_rows += len(pd.read_csv(f))
print("Curated rows:", curated_rows)

# Quarantine
q = list(Path("data/quarantine").glob("*.csv"))
print("Quarantine files:", len(q))
```

Key aggregates (example):
```python
df = pd.concat([pd.read_csv(f) for f in Path("data/curated").rglob("*.csv")])
print(df.groupby("status")["quantity"].sum())
print(df.groupby("currency")["unit_price"].mean())
```

---

## Performance Notes

- Sample (≈40 rows) finishes in well under 1 second.
- 120 k rows (generated by `generate_large_data.py`) typically completes in a few seconds on a laptop (dominated by CSV parse + write).
- **One reasonable optimisation**: switch the large-generator and validation path to **Polars** (lazy scan + streaming sink) or read CSVs in chunks; this would reduce peak memory and improve throughput for multi-million-row files while keeping the same zone design.

---

## Known Limitations
- Single-process only; no concurrent writers to the same partition.
- CSV-only ingestion (easily extended to JSON/Parquet).
- No schema-evolution handling beyond “extra columns are ignored”.
- Quarantine keeps original values as strings; no automatic repair loop.
- Manifest is a simple JSON file (not atomic across machines).

---

## Design Decisions & Trade-offs

| Decision                    | Rationale                                      | Trade-off |
|-----------------------------|------------------------------------------------|-----------|
| Pydantic for validation     | Clear, typed, excellent error messages         | Slightly slower than pure vectorised checks |
| Content-hash idempotency    | Detects true re-delivery even if filename changes | Hashing large files has a cost |
| Hive-style CSV partitions     | Familiar, easy to inspect, zero extra deps       | Many small files if high cardinality; less efficient than Parquet |
| Keep_latest on PK           | Matches common “last write wins” analytics need | Loses history of intermediate states |
| Local folders only          | Zero external dependencies, fully reproducible | Not production-scale object storage |

---

## Possible Next Steps
- Add a simple Great Expectations or custom data-quality dashboard.
- Support JSON Lines and Parquet sources / Parquet curated output.
- Introduce a watermark table for incremental processing.
- Compaction job for small partition files.
- Docker-compose with a local MinIO to mimic real object storage.
- Airflow / Prefect / Dagster wrapper for scheduling.

---

## Project Layout

```
yottaflex-data-pipeline/
├── README.md
├── requirements.txt
├── config.yaml
├── data/
│   ├── raw/ validated/ curated/ quarantine/ sample/ state/ logs/
├── src/
│   ├── schema.py          # Pydantic models & controlled vocabularies
│   ├── ingestion.py       # Land files + metadata + hash
│   ├── validation.py      # Schema + business rules → accept/reject
│   ├── transformation.py  # Dedup + partitioned curated write
│   ├── pipeline.py        # Orchestration entry-point
│   ├── metrics.py         # Run observability
│   └── utils.py           # Config, logging, helpers
├── tests/
│   ├── conftest.py
│   ├── test_happy_path.py
│   └── test_edge_cases.py
├── scripts/
│   ├── generate_sample_data.py
│   └── generate_large_data.py
└── prompt_history/        # (reserved)
```

---

## Exact Commands Summary

```bash
# Setup
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Data
python scripts/generate_sample_data.py
python scripts/generate_large_data.py          # optional, ~120k rows

# Pipeline
python -m src.pipeline
python -m src.pipeline --force                 # ignore manifest

# Tests
PYTHONPATH=. pytest tests/ -v
```

Happy data engineering!
