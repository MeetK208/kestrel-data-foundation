# Kestrel Data Foundation

A small, honest analytical foundation for **Kestrel Provisions**, a fictional Indian food and grocery
distributor. It takes four messy source feeds, lands them faithfully, cleans them into one trustworthy
version of the truth, and leaves a clear path to business metrics on top.

The engine is [DuckDB](https://duckdb.org) (a single-file analytical database, like "SQLite for
analytics"). The pipeline is plain Python and SQL. The whole thing runs on a laptop; no cluster, no
cloud account, no services to start.

> New to the project? Read [`DECISIONS.md`](DECISIONS.md) first (one page: what is built, what is not,
> and why). The deep design rationale lives in
> [`DATA_MODEL_AND_IMPLEMENTATION_PLAN.md`](DATA_MODEL_AND_IMPLEMENTATION_PLAN.md).

---

## 1. The problem in one paragraph

The business runs four systems that never agree: point-of-sale transactions, refrigerated-truck
temperature telemetry, warehouse scan events, and ERP change-data-capture extracts. Four people quote
four different sales numbers in every meeting because each built their own extract with their own
undocumented assumptions. The CFO does not want another dashboard. He wants **one defined, documented,
queryable set of numbers that traces back to the row it came from.** This repository is the foundation
that makes that possible.

---

## 2. Architecture: the medallion pattern

Data moves through three layers of increasing trust. This is a common data-engineering pattern called
**medallion architecture**.

```
  SOURCE FILES                BRONZE                    SILVER                      GOLD
  (parquet + csv)       raw, faithful copy        cleaned & conformed        business metrics
                                                                             (designed, not built yet)

  pos_transactions  ─┐                          ┌ dedup, UTC→IST,          ┌ fact_sales
  reefer_telemetry  ─┤   load as-is, add        │ cases→eaches             │ fact_cold_chain
  wms_scan_events   ─┼─▶ lineage columns,  ──▶  ┤ °F→°C, clock fix,   ──▶  ┤ fact_order
  erp_cdc/*         ─┤   quarantine bad files   │ CDC→current + SCD2,      │ fact_wms_cycle
  reference/*.csv   ─┘   reconcile vs manifest  └ typed, deduped           └ conformed dims + KPI marts
```

| Layer | What it is | What lives here | Status |
|---|---|---|---|
| **Bronze** | An exact, replayable copy of the source. Nothing is cleaned or dropped. | Every source column as-is, plus lineage columns (which file, which run, when loaded). Corrupt files are quarantined, not fatal. | **Built** |
| **Silver** | One trustworthy, cleaned version of each entity. This is where the defects get fixed. | Deduplicated facts in consistent units and timezones, CDC rebuilt to current state, and point-in-time history (SCD2). | **Built (11 tables)** |
| **Gold** | Business-ready tables: the star schema and KPI marts the CFO's catalogue will document. | `fact_*` / `dim_*` tables and KPI marts (gross sales by channel, cold-chain excursion rate, etc.). | **Designed, not yet implemented** — see [`DECISIONS.md`](DECISIONS.md) |

**Why these three layers?** Bronze gives you traceability and lets you re-run without re-reading messy
sources. Silver is the single place defects are fixed, so every downstream number inherits the same
cleaning. Gold is fast and simple to query because the hard work already happened upstream.

---

## 3. Tech stack

| Thing | Choice | Why |
|---|---|---|
| Database / engine | DuckDB (single `.duckdb` file) | Runs in-process, reads Parquet directly, fast on a laptop, zero setup. |
| Language | Python 3.10+ (tested on 3.12) | Thin orchestration around SQL. |
| Transforms | SQL files (one per table) | The logic is readable and reviewable without running code. |
| Config | `config.yaml` | One schema contract per table, so the pipeline validates shape before it transforms. |

---

## 4. Repository layout

```
kestrel-data-foundation/
├── README.md                      ← you are here
├── DECISIONS.md                   ← start here: built / not built / assumptions
├── DATA_MODEL_AND_IMPLEMENTATION_PLAN.md   ← full design rationale, data model, 5 KPI definitions
├── config.yaml                    ← per-table schema contracts (bronze & silver column lists)
├── requirements.txt               ← Python dependencies
│
├── ingestion/                     ← the code that moves data
│   ├── generate_dataset.py        ← regenerates the synthetic source data
│   ├── bronze_ingest.py           ← SOURCE → BRONZE (run this first)
│   ├── silver_layer/              ← one small runner script per silver table
│   │   ├── pos_transactions.py
│   │   ├── reefer_telemetry.py
│   │   ├── wms_scan_events.py
│   │   ├── erp_sales_order_header.py
│   │   ├── erp_outlet_master.py            (current state)
│   │   ├── erp_outlet_master_history.py    (SCD2 history)
│   │   ├── erp_product_master.py
│   │   └── ref_*.py                        (reference tables)
│   └── utility/                   ← the reusable engine behind every silver script
│       ├── general_pipeline.py    ← validate → transform, returns run stats
│       ├── validate_tables.py     ← checks live table shape against config.yaml
│       └── run_sql.py             ← runs a .sql file statement by statement
│
├── sqls/                          ← all transformation logic, as SQL
│   ├── bronze_layer/              ← (empty: bronze is built in Python, not SQL)
│   ├── silver_layer/
│   │   ├── ddls/<table>.sql            ← CREATE TABLE for each silver table
│   │   └── transformations/<table>.sql ← the bronze→silver cleaning (incremental MERGE)
│   └── gold_layer/                ← (empty: gold is the next layer to build)
│
├── warehouse/
│   └── kestrel.duckdb             ← the database file (created on first run; git-ignored)
└── data/                          ← source + outputs (git-ignored; you supply or regenerate it)
    ├── raw/                       ← the six partitioned feeds (parquet)
    ├── reference/                 ← the five reference CSVs
    ├── _manifest/                 ← expected per-partition row counts (for reconciliation)
    ├── _quarantine/               ← corrupt files copied here by bronze (audit trail)
    └── _reports/                  ← per-run bronze log + JSON summary
```

---

## 5. Quickstart (cold start)

These steps take you from an empty checkout to a queryable database. Run everything from the
`kestrel-data-foundation/` folder.

### Step 0 — Get the data

The dataset is **not committed** (it is ~10 million rows). You have two options.

- **Regenerate it** from the shipped generator (reproducible, uses a fixed seed):
  ```bash
  python ingestion/generate_dataset.py --scale 1 --out data
  ```
- **Or** drop a supplied dataset into `data/` so you end up with `data/raw/`, `data/reference/`,
  and `data/_manifest/`.

### Step 1 — Set up Python

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Step 2 — Build the Bronze layer (source → raw tables)

```bash
python ingestion/bronze_ingest.py --data-dir data --db warehouse/kestrel.duckdb
```

This reads every feed file, loads it as-is into the `bronze_layer` schema, quarantines anything it
cannot read, and reconciles what it loaded against the manifest. It is **safe to re-run**: unchanged
files are skipped. When it finishes it prints a row count per table. Expect roughly:

```
pos_transactions        4,084,000
reefer_telemetry        3,713,926
wms_scan_events         1,496,000
erp_sales_order_header    963,307
...
quarantined files this run: 1      ← the one deliberately-corrupt telemetry file
```

### Step 3 — Build the Silver layer (clean the data)

Each silver table has its own small runner. They only read from Bronze, so the order does not matter.
Run them all with one loop:

```bash
for f in ingestion/silver_layer/*.py; do
  echo "== $f =="
  python "$f"
done
```

Or run a single table while you work on it:

```bash
python ingestion/silver_layer/pos_transactions.py
```

Each script validates the table shape against `config.yaml`, then applies its cleaning SQL and prints
stats (rows staged, rows merged, final row count).

### Step 4 — Look at the results

The quickest way, using the Python module you already installed:

```bash
python -c "import duckdb; con=duckdb.connect('warehouse/kestrel.duckdb', read_only=True); \
print(con.sql('SELECT table_schema, count(*) AS tables FROM information_schema.tables GROUP BY 1'))"
```

Or, if you have the separate DuckDB command-line shell installed (download it from
[duckdb.org/docs/installation](https://duckdb.org/docs/installation/)), open the file directly:

```bash
duckdb warehouse/kestrel.duckdb
```

Either way, these queries show the cleaning worked:

```sql
-- how many clean POS lines do we have, and how many were duplicates?
SELECT
  (SELECT count(*) FROM bronze_layer.pos_transactions) AS bronze_rows,
  (SELECT count(*) FROM silver_layer.pos_transactions) AS silver_rows;
-- 4,084,000 bronze → 4,000,000 silver (84,000 exact duplicates removed)

-- did the Fahrenheit→Celsius fix work? (COLDEYE readings should now look like fridge temps)
SELECT telemetry_vendor, round(avg(temp_value),1) AS avg_c
FROM silver_layer.reefer_telemetry
WHERE temp_value IS NOT NULL
GROUP BY 1;
```

---

## 6. What each layer does in detail

### Bronze — `ingestion/bronze_ingest.py`

Bronze is a faithful landing zone. Its rules:

- **As-is.** No dedup, no unit conversion, no timezone fixes. Those belong in Silver. Even both quantity
  columns (`qty` and `quantity_units`) and all CDC insert/update/delete events are kept.
- **Never crash.** Every file is isolated. A bad file is copied to `data/_quarantine/` and logged; the
  run carries on and still exits `0` (pass `--strict` to exit non-zero when anything is quarantined).
- **Idempotent.** Re-running skips files whose size and modified-time are unchanged. Changed files are
  reloaded cleanly inside a transaction, with a row-count check against the Parquet footer.
- **Auditable from SQL.** Every run writes to metadata tables you can query:

  | Table | What it tells you |
  |---|---|
  | `bronze_run_log` | one row per pipeline run, with status and a stats summary |
  | `bronze_file_log` | every file seen, its status (LOADED / QUARANTINED / SKIPPED / SOURCE_MISSING) and row count |
  | `bronze_quarantine_files` | the corrupt/unreadable files and why they failed |
  | `bronze_schema_drift_log` | columns that appeared, changed type, or went missing (catches the Q4-2025 POS schema change) |
  | `bronze_partition_reconciliation` | loaded rows vs the manifest's expected rows, per partition — this is how you find missing days |
  | `v_bronze_open_issues` | a view of the latest run's non-OK partitions |

Useful flags: `--full-refresh` (drop and reload everything), `--feeds pos_transactions reefer_telemetry`
(load a subset), `--strict`, `--threads`, `--memory-limit`.

### Silver — one SQL transform per table

Silver is where the mess gets fixed. Every table follows the same shape (a DDL file, a transformation
file, and a runner), and every transform is an **incremental `MERGE`**: it stages only recent rows,
deduplicates to one row per key, cleans them, and merges into the target. Re-running does not create
duplicates.

Here is what each feed's Silver step fixes, and why it matters:

| Table | Cleaning applied | Why |
|---|---|---|
| `pos_transactions` | Remove 84k exact duplicates on `(txn_id, txn_line_no)`; coalesce `qty`/`quantity_units` into one quantity; default missing `uom` to `EA`; convert the sale time from **UTC to IST** and keep both; convert case quantities to **eaches** using `ref_uom_conversion` (falling back to product-master `case_pack`); compute line gross and net money. | The partner feed delivers duplicates and changed schema in Q4 2025. Sales must be counted on the **India business day**, in a single unit (eaches), with no double counting. |
| `reefer_telemetry` | Subtract the **+7h clock offset** on firmware `2.1.4`; convert all temperatures to **°C** (vendor COLDEYE reports °F); infer the missing unit (~8% of rows) from the vendor; leave sensor-dropout nulls as null; deduplicate. | Two vendors use different conventions. Without the °F→°C fix, COLDEYE's ~4°C fridge readings look like breaches, which is the false "a third of trips breach" story. |
| `wms_scan_events` | Parse the **site-local** timestamp (no shift; this feed is already local); add a `stage_seq` so the six handling stages sort in order; deduplicate on `scan_id`. | Timestamps here follow a different convention from every other feed. Stage ordering is needed later for cycle time. |
| `erp_sales_order_header` | Rebuild **current state** per order from the CDC events, ordered by `(__op_ts, __seq)`; treat a delete as final; divide `PARTNER_API` order values by **1.085** to strip double-counted freight. | CDC is a stream of changes, not current state. One source system inflates order value; that is the unresolved Finance ticket. |
| `erp_outlet_master` / `erp_product_master` | Rebuild current state per key from CDC. | A clean, current dimension row per outlet / SKU. |
| `erp_outlet_master_history` | Build **SCD2 history**: one row per version with `valid_from` / `valid_to` / `is_current` / `is_deleted`. | Answers "what channel was this outlet on *back then*?" so historical reports do not use today's values. |
| `ref_*` (carrier, warehouse, fiscal calendar, legacy weekly, uom) | Type and pass through. | Clean reference/lookup tables. |

### Gold — not built yet

The Gold layer (the star schema and KPI marts) is designed but not implemented. The design, including
five fully specified KPIs, is in
[`DATA_MODEL_AND_IMPLEMENTATION_PLAN.md`](DATA_MODEL_AND_IMPLEMENTATION_PLAN.md). The honest reasoning
for stopping at Silver is in [`DECISIONS.md`](DECISIONS.md).

---

## 7. How the Silver engine works (for anyone adding a table)

You do not write new Python to add a Silver table. The flow is config-driven:

1. A runner in `ingestion/silver_layer/<table>.py` calls `run_silver_pipeline(...)` with the source and
   target table names.
2. `general_pipeline.py` loads the table's entry from `config.yaml`, then calls `validate_tables.py`,
   which checks that the live Bronze and Silver tables match the column lists in the config (creating
   the Silver table from its DDL file if it does not exist yet).
3. If validation passes, `run_sql.py` executes `sqls/silver_layer/transformations/<table>.sql`.

So to add a Silver table you: add its column contracts to `config.yaml`, write
`sqls/silver_layer/ddls/<table>.sql` and `sqls/silver_layer/transformations/<table>.sql`, and copy an
existing runner in `ingestion/silver_layer/`.

---

## 8. Data quality and lineage

- **Lineage columns** travel through every layer: `_bronze_file` (which source file), `_bronze_run_id`
  (which run), `_bronze_loaded_at` and `_silver_loaded_at` (when). Any silver row can be traced back to a
  source file.
- **Reconciliation** against `data/_manifest/expected_partitions.csv` tells you which days are missing or
  short in any feed, instead of missing data silently looking like a quiet day.
- **Quarantine** keeps a copy of every unreadable file with the reason, so a bad file is an incident you
  can see, not a crash.

---

## 9. Running at larger scale

The generator can produce 10× the data:

```bash
python ingestion/generate_dataset.py --scale 10 --out data_10x
python ingestion/bronze_ingest.py --data-dir data_10x --db warehouse/kestrel_10x.duckdb
```

Bronze loads file-by-file and scales gracefully. The Silver `MERGE` and SCD2 steps are the first to feel
memory pressure on a laptop; see [`DECISIONS.md`](DECISIONS.md) for what breaks first and at what volume.

---

## 10. Troubleshooting

| Symptom | Fix |
|---|---|
| `ModuleNotFoundError: yaml` | You skipped the install. Run `pip install -r requirements.txt` inside the venv. |
| Bronze prints `feed root folder missing` | `--data-dir` is wrong, or you have not generated/placed the data yet (Step 0). |
| A silver script errors in the `validate` step | The live table does not match `config.yaml`. The error lists each mismatched column; fix the DDL/config or re-run Bronze. |
| `MERGE` syntax error | Your DuckDB is too old. `MERGE INTO` needs DuckDB ≥ 1.4; `pip install -U duckdb`. |
| Want a clean rebuild | Delete `warehouse/kestrel.duckdb`, or run Bronze with `--full-refresh`, then re-run Silver. |

---

## 11. Documentation map

- [`DECISIONS.md`](DECISIONS.md) — the one-pager: what is built, what is not, assumptions, what is next,
  and what breaks first in production.
- [`DATA_MODEL_AND_IMPLEMENTATION_PLAN.md`](DATA_MODEL_AND_IMPLEMENTATION_PLAN.md) — the full data model,
  join map, the five KPI definitions, and the data-quality framework.
- `config.yaml` — the authoritative column contract for every table.
