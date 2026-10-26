# DECISIONS

One page. Read this before the code.

## What I built

A **Bronze** and **Silver** foundation on DuckDB that turns four disagreeing feeds into one clean,
traceable version of the truth.

- **Bronze** (`ingestion/bronze_ingest.py`): a faithful, replayable landing of all six Parquet feeds and
  five reference CSVs. It never drops or alters source data, quarantines the one corrupt file instead of
  crashing, is idempotent (re-running skips unchanged files), and reconciles what it loaded against the
  manifest so missing days are visible. Every run is auditable from SQL (run log, file log, quarantine,
  schema-drift, partition reconciliation).
- **Silver** (11 tables, SQL transforms): the defects are fixed here. POS deduplicated
  (4,084,000 → 4,000,000 rows) and converted to one quantity in **eaches** on the **IST business day**;
  telemetry normalised to **°C** with the **+7h firmware clock** corrected; ERP CDC rebuilt to current
  state (963,307 events → 320,000 orders) with deletes made final and `PARTNER_API` freight removed;
  outlets kept both current and as **SCD2 history** for point-in-time questions. Every row keeps lineage
  back to its source file.

The data model, join map, and five KPI definitions are written up in
`DATA_MODEL_AND_IMPLEMENTATION_PLAN.md`.

## What I deliberately did not build

- **The Gold layer and the KPI catalogue/SQL library.** This is the biggest cut and it is deliberate. I
  chose to get Bronze and Silver genuinely correct and traceable rather than ship a half-trusted Gold on
  top. Every KPI is only as good as the cleaning beneath it, and that is where the real traps were
  (°F/°C, UTC/IST, duplicates, CDC rebuild). The marts are designed and ready to build on Silver.
- **The "ask-anything" query tool.** Depends on Gold being in place first.
- **Carrier, trip, and delivery service-level metrics.** The feeds contain **no carrier key** and **no
  trip id**, and orders never reach a `DELIVERED` status. I did not invent these links; building that KPI
  honestly needs an assumption I would rather confirm with the owner.
- **An `--scale 10` run and a single-command orchestrator.** Documented, not executed.

## What I assumed (where the brief was unclear or the data contradicted the docs)

- **Business day = Asia/Kolkata (IST).** POS, telemetry, and ERP timestamps are UTC (`Z`); WMS is already
  site-local. All facts are dated on the IST event day, never the ingest day.
- **"Gross sales" = unit_price × quantity-in-eaches, before discount and tax.** This is a definition
  choice; it is stated so it can be argued with.
- **Current state from CDC = latest by `(__op_ts, __seq)`**, and a delete is final (a deleted order does
  not come back on a later-timestamped update).
- **`PARTNER_API` order values are inflated ×1.085** (reverse-engineered from the 4-decimal freight tell).
  Labelled an assumption; would confirm with Finance.
- **Excursion threshold = reading > 8 °C** after unit normalisation; chilled band 2–8 °C.
- **UOM:** missing `uom` defaults to `EA`; the 46 SKUs absent from `uom_conversion` fall back to
  product-master `case_pack` rather than being guessed or dropped.
- **The feed contract is wrong in at least one place, as warned.** It claims reefer `temp_unit` is
  "populated by both vendors" — in the data ~8% is null, so I infer it from the vendor. It also says the
  firmware clock issue was "believed fixed"; it is not (firmware `2.1.4` still runs +7h), so I correct it.

## What I would do next with two more weeks

1. Build the Gold star schema (`fact_sales`, `fact_order`, `fact_cold_chain`, `fact_wms_cycle` + conformed
   dims) and the five KPI marts.
2. Write the **KPI catalogue** (name, definition, grain, filters, source, owner, limitation) and a
   parameterised **SQL library** behind it.
3. Reconcile POS-derived weekly sales to the legacy Finance report and **explain** the variance (the
   report is synthetic and ends a week early, so the deliverable is the explained difference, not a match).
4. Add a data-quality results table that **gates** Gold on Silver checks, a one-command orchestrator, and
   a thin ask-anything tool that always prints the SQL it ran.

## What breaks first in production, and at what volume

- **The Silver `MERGE` and SCD2 steps break first.** They stage recent rows and run window functions over
  full key history inside a single DuckDB process under a 4 GB memory limit. Bronze scales gracefully
  because it loads file-by-file; Silver does not. On a laptop this is comfortable at **scale 1 (~10M
  rows)** and starts to spill to disk and slow sharply somewhere around **scale 5–10 (~50–100M rows)**.
  The fix is to make Silver partition-incremental (process by date) rather than whole-table, and to raise
  the memory limit / run off-laptop.
- **The incremental watermark assumes late data arrives within a few days.** POS uses a 4-day lookback; a
  file arriving later than that window is only picked up by a `--full-refresh`.
- **Single-file DuckDB is single-writer.** Fine for a batch run; it would not support concurrent writers
  without moving to a server-based warehouse.
- **No cross-table orchestration.** The 11 Silver scripts run independently, so a mid-way failure leaves a
  partial refresh (each table is internally transactional, but the set is not).
