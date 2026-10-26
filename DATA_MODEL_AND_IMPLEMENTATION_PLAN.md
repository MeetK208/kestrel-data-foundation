# Kestrel Provisions — Data Model, Architecture & Implementation Plan

A complete planning and reference document for the AISTRA take-home. It explains the data you
actually have (verified by reading the files and querying the Parquet), how the tables connect,
how to design Bronze/Silver/Gold layers, the five KPIs to build, a data-quality framework, the
business use cases, governance considerations, risks, and an ordered implementation roadmap.

> **No code or SQL here — on purpose.** This is for understanding, architecture, and decisions.
> Everything below is grounded in the real files in `data/` (schemas from `data/sql/*.sql`,
> samples from `data/sample_output/*.csv`, and live row counts from the Parquet in `data/raw/`).

**All figures below are measured at `--scale 1` (the shipped dataset), covering 2025-01-01 to
2026-06-30 (18 months).** The generator can produce larger volumes with `--scale`.

---

## Table of contents
1. [Understanding the available data](#1-understanding-the-available-data)
2. [The data model](#2-the-data-model)
3. [Bronze / Silver / Gold design](#3-bronze--silver--gold-design)
4. [Top 5 business KPIs](#4-top-5-business-kpis)
5. [Data quality framework](#5-data-quality-framework)
6. [Business use cases](#6-business-use-cases)
7. [Governance & modeling considerations](#7-governance--modeling-considerations)
8. [Risks, gaps & assumptions](#8-risks-gaps--assumptions)
9. [Implementation plan](#9-implementation-plan)
10. [Final assignment checklist](#10-final-assignment-checklist)

---

## 1. Understanding the available data

### 1.1 The business domain in one paragraph
Kestrel Provisions is a food & grocery **distributor** in India: ambient, chilled and frozen
goods move from **8 distribution centres** to ~2,500 retail outlets across **5 regions**, through
four sales **channels** — GT (general trade / kirana shops), MT (modern trade / supermarkets),
HORECA (hotels, restaurants, cafés) and ECOM (e-commerce dark stores). Four operational systems
each record part of the story and **none of them agree**. The data lets us answer two families of
questions: **financial** ("what did we actually sell, by channel, and does it match the report the
board sees?") and **operational** ("is the cold chain holding, how fast do warehouses turn orders
around, and where is data missing?").

### 1.2 What physically exists in `data/`
| Location | What it is | Notes |
|---|---|---|
| `data/raw/pos_transactions/` | POS sales lines (Parquet, partitioned by `ingest_date`) | **4,084,000 rows** |
| `data/raw/reefer_telemetry/` | Fridge-truck sensor readings (Parquet, partitioned by `dt`) | **3,713,926 readable rows** + 1 corrupt file |
| `data/raw/wms_scan_events/` | Warehouse barcode scans (Parquet, partitioned by `dt`) | **1,496,000 rows** |
| `data/raw/erp_cdc/sales_order_header/` | Order change-events (Parquet, partitioned by `extract_date`) | **963,307 rows → 320,000 orders** |
| `data/raw/erp_cdc/outlet_master/` | Outlet change-events | **8,774 rows → 2,400 outlets** |
| `data/raw/erp_cdc/product_master/` | Product change-events | **3,536 rows → 1,100 SKUs** |
| `data/reference/uom_conversion.csv` | SKU → eaches-per-case lookup | **1,054 SKUs (46 missing)** |
| `data/reference/warehouse_master.csv` | 8 warehouses: region, timezone, capacity | all `Asia/Kolkata` |
| `data/reference/carrier_master.csv` | 5 carriers: mode, SLA hours, rate | **no join key into the feeds** |
| `data/reference/fiscal_calendar.csv` | 546 days, fiscal year Apr–Mar | the official calendar |
| `data/reference/legacy_finance_weekly_report.csv` | Weekly board report | **78 weeks × 4 channels; values are not reproducible from POS** |
| `data/_manifest/expected_partitions.csv` | Row/byte counts the ingestion job claims it wrote | for reconciliation |
| `data/sql/*.sql` | Inferred DuckDB **DDL** (CREATE TABLE) per table | schema reference |
| `data/sample_output/*.csv` | First-1000-row samples per table (+ a post-drift POS sample) | quick preview |
| `data/Archive.zip` | A zip archive (contents not inspected in this analysis) | verify before relying on it |
| `ingestion/generate_dataset.py` | The dataset generator | defects marked `# DEFECT` |
| `ingestion/data_ddl_analysis.py` | Script that produced the DDL + samples | already skips the corrupt file |

> **Important status note.** `data/sql/` and `data/sample_output/` are **profiling outputs you
> generated**, not part of the official source. They are excellent as a data dictionary, but the
> source of truth is the Parquet in `data/raw/` plus `data/reference/`.

### 1.3 Table-by-table business meaning and key columns
Types below come from the inferred DDL (`data/sql/_all_tables.sql`). "PK/FK" are **logical** keys
(the data has no enforced constraints — this is a lake, not a relational database).

#### (A) `pos_transactions` — what was sold at the till  *(FACT, transaction-line grain)*
One row = **one line on one sales receipt** from MT and ECOM partners.
| Column | Type | Meaning / notes |
|---|---|---|
| `txn_id` | VARCHAR | Transaction id. **Logical key = (`txn_id`,`txn_line_no`)**. Exact-duplicate rows exist. |
| `txn_line_no` | SMALLINT | Line number within the transaction. |
| `basket_id` | VARCHAR | Groups lines into one shopper basket (use for basket counts). |
| `outlet_code` | VARCHAR | **FK → outlet_master.outlet_code**. |
| `channel` | VARCHAR | Channel as captured at the till (GT/MT/HORECA/ECOM). May differ from the outlet's master channel. |
| `sku_code` | VARCHAR | **FK → product_master.sku_code / uom_conversion.sku_code**. |
| `event_ts` | VARCHAR | Sale timestamp as text, **UTC with a `Z`** (e.g. `2025-01-01T05:02:02Z`). Business day is IST. |
| `qty` | INTEGER | Quantity **(pre-drift rows only; null afterwards)**. |
| `quantity_units` | INTEGER | Quantity **(post-drift rows only, from 2025-10-01)**. Same meaning as `qty`. |
| `uom` | VARCHAR | Unit of measure `EA`/`CS` **(exists only post-drift; null before)**. ~19% of post-drift lines are `CS`. |
| `unit_price` | DOUBLE | Price per unit, **ex-tax**. |
| `discount_amount`, `tax_amount` | DOUBLE | Line-level money. Tax ≈ 12% of line value in samples. |
| `payment_mode`, `till_id`, `cashier_id` | VARCHAR/SMALLINT | Operational attributes (cashier is person-level). |
| `promo_code`, `loyalty_id` | VARCHAR | Nullable; `loyalty_id` exists only post-drift. |
| `source_file` | VARCHAR | Originating file name (lineage). |
| `ingest_date` | DATE | **Partition** = the day the file landed, **not** the day of sale. |

Verified: 4,084,000 rows, 4,000,000 distinct `txn_id` → **84,000 duplicate rows (~2.1%)**; the
quantity splits almost evenly across `qty` (2,040,631 rows) and `quantity_units` (2,043,369 rows)
at the 2025-10-01 schema change.

#### (B) `reefer_telemetry` — is the cold chain holding?  *(FACT, sensor-reading grain)*
One row = **one temperature/GPS reading** from a refrigerated vehicle.
| Column | Type | Meaning / notes |
|---|---|---|
| `device_id`, `vehicle_registration` | VARCHAR | Sensor + vehicle (vehicle reg is PII-like). |
| `telemetry_vendor` | VARCHAR | `THERMLOG` or `COLDEYE` — **different unit conventions**. |
| `firmware_version` | VARCHAR | `2.1.4` fleet runs a **+7h clock offset** (9.1% of rows). |
| `route_code`, `warehouse_code` | VARCHAR | Assignment at reading time. **`warehouse_code` FK → warehouse_master**. |
| `gateway_id` | VARCHAR | Ingestion gateway. One gateway (`GW-017`) goes dark for two days. |
| `reading_ts` | VARCHAR | Device clock, **UTC `Z`** text. Skewed for firmware 2.1.4. |
| `temp_value` | DOUBLE | Temperature **in the vendor's own unit**. 22,212 nulls (sensor dropouts). |
| `temp_unit` | VARCHAR | `C`/`F`, **null on ~8%** → must be inferred from vendor. |
| `humidity_pct`, `door_open_flag`, `battery_pct` | DOUBLE/TINYINT | Sensor state. |
| `gps_lat`, `gps_lon` | DOUBLE | Position (PII-like / location data). |
| `dt` | DATE | **Partition** = event date. |

Verified: COLDEYE reads **Fahrenheit** (avg **39.6°F** ≈ 4.2°C); THERMLOG reads **Celsius** (avg
**4.2°C**). COLDEYE is ~**1.24M of 3.71M rows (~33.5%)** — see the KPI section for why that single
fact explains the "a third of trips breach" myth.

#### (C) `wms_scan_events` — how goods move through the warehouse  *(FACT, scan-event grain)*
One row = **one barcode scan**.
| Column | Type | Meaning / notes |
|---|---|---|
| `scan_id` | VARCHAR | **Unique key** (1,496,000 rows, all distinct — no duplicates). |
| `warehouse_code` | VARCHAR | **FK → warehouse_master**. |
| `event_type` | VARCHAR | Lifecycle stage: RECEIVE→PUTAWAY→PICK→PACK→STAGE→DISPATCH (roughly even split). |
| `order_number` | VARCHAR | **FK → sales_order_header.order_number** (nominal; see caveat below). |
| `sku_code`, `batch_id`, `qty_cases` | VARCHAR/SMALLINT | What was handled. |
| `pallet_id`, `dock_door`, `operator_id`, `handheld_device` | — | Location / who / device. |
| `event_ts` | VARCHAR | **Site-local** time `YYYY-MM-DD HH:MM:SS` — **no `Z`**, unlike every other feed. |
| `dt` | DATE | **Partition** = event date. |
| **Caveat** | | ~6.5% of scans are missing, and stages are distributed independently, so a given `order_number` is **not** a clean six-stage lifecycle. Cycle time is therefore **indicative**. |

#### (D) `erp_cdc/sales_order_header` — orders and their money  *(FACT after rebuild, order grain)*
Change-data-capture: Insert/Update/Delete events; current state must be **rebuilt**.
| Column | Type | Meaning / notes |
|---|---|---|
| `order_number` | VARCHAR | **Logical PK after rebuild**. 320,000 distinct orders from 963,307 events. |
| `__op` | VARCHAR | `I` insert (320,000) / `U` update (640,427) / `D` delete (2,880 ≈ 0.9%). |
| `__op_ts` | VARCHAR | Commit time (UTC `Z` text). **Order current state by (`__op_ts`,`__seq`)**. |
| `__seq` | BIGINT | Monotonic sequence; breaks ties when `__op_ts` is equal. |
| `outlet_code`, `warehouse_code`, `route_code` | VARCHAR | FKs to outlet / warehouse. |
| `order_date`, `requested_delivery_date` | VARCHAR | Dates as text. |
| `order_status` | VARCHAR | CREATED / CONFIRMED / PICKED / DISPATCHED. **DELIVERED never appears** — see risks. |
| `line_count` | SMALLINT | Number of lines on the order. |
| `order_value_gross`, `discount_amount`, `tax_amount` | DOUBLE | Money. |
| `source_system` | VARCHAR | `SFA_MOBILE` (577,759) / `ERP_WEB` (288,828) / `PARTNER_API` (96,720). |
| `extract_date` | DATE | **Partition** = the extract file's date (can be later than `__op_ts`). |
| **Tell** | | `PARTNER_API` values carry 4 decimals (e.g. `184640.1571`) vs clean 2-dp values — the signature of a **×1.085 freight double-count**. |

#### (E) `erp_cdc/outlet_master` — the shop dimension  *(DIMENSION, SCD2 candidate)*
Attributes change over time. Current/point-in-time state rebuilt from CDC per `outlet_code`.
| Column | Type | Meaning |
|---|---|---|
| `outlet_code` | VARCHAR | **PK** (2,400 outlets from 8,774 events). |
| `__op`,`__op_ts`,`__seq`,`extract_date` | — | CDC control columns. Seed inserts dated `2024-12-31` (day before data start). |
| `outlet_name`,`channel`,`outlet_format`,`city`,`route_code`,`warehouse_code` | VARCHAR | Attributes; `channel`/`route`/`warehouse` can change → reclassification history. |
| `credit_limit`,`credit_terms_days` | DOUBLE/BIGINT | Commercial terms. |
| `gst_number` | VARCHAR | Tax id (PII-like). **All start `27` regardless of city — implausible** (see risks). |
| `status` | VARCHAR | ACTIVE. |

#### (F) `erp_cdc/product_master` — the product dimension  *(DIMENSION, SCD2 candidate)*
| Column | Type | Meaning |
|---|---|---|
| `sku_code` | VARCHAR | **PK** (1,100 SKUs from 3,536 events). |
| `product_name`,`category`,`brand` | VARCHAR | Category ∈ {Bakery, Beverages, Dairy, Frozen, Ready to Eat, Sauces, Snacks, Staples}. |
| `case_pack` | BIGINT | Eaches per case per the ERP (compare with `uom_conversion`). |
| `mrp`,`list_price`,`gst_rate_pct` | DOUBLE | Pricing; `list_price` ≈ `mrp` × 0.76 in samples. |
| `shelf_life_days` | BIGINT | Shelf life. |
| `is_chilled` | BIGINT | 1 for Dairy & Frozen (~280 SKUs), else 0. Defines cold-chain scope. |
| `status` | VARCHAR | ACTIVE. |

#### (G) Reference / lookup tables
- **`ref_uom_conversion`** (LOOKUP/bridge): `sku_code`, `eaches_per_case`, `base_uom`, `case_uom`.
  **1,054 of 1,100 SKUs — 46 missing (4.2%)**. Needed to convert cases → eaches.
- **`ref_warehouse_master`** (DIMENSION): 8 sites; `region_name`, `timezone` (all `Asia/Kolkata`),
  `chilled_capacity_pallets`. The authority for **region** and **timezone**.
- **`ref_carrier_master`** (DIMENSION, currently orphan): 5 carriers (`CR-101`..`CR-105`), `mode`
  (SURFACE/REEFER/LCV), `sla_hours`, `rate_per_km`. **No `carrier_id` exists in any feed**, so this
  cannot be joined without an assumption (see risks).
- **`ref_fiscal_calendar`** (DIMENSION — date): 546 dates, `fiscal_year` (Apr–Mar), `fiscal_quarter`,
  `fiscal_month_no`, `iso_week`, `is_weekend`. **The only correct source of fiscal periods.**
- **`ref_legacy_finance_weekly`** (BENCHMARK, not a fact/dim): 78 weeks × 4 channels. The board's
  current report. **Its values are not reproducible from POS and it ends 2026-06-24** (data runs to
  06-30). Treat as a reconciliation target, not ground truth.
- **`manifest_expected_partitions`** (OPERATIONAL metadata): `feed`, `partition`, `file_count`,
  `row_count`, `bytes` — what the ingestion job believed it wrote. Used to detect missing/short data.

### 1.4 What's missing, ambiguous, or suspicious (first pass)
- **No carrier linkage** anywhere in the event feeds → "by carrier" questions need an assumption.
- **No trip identifier** in telemetry → "trips" must be approximated (e.g. device-day).
- **No `DELIVERED` status** in orders → true delivery service level can't be measured from status.
- **Legacy report is synthetic** (random) and ends a week early → exact reconciliation is impossible.
- **`gst_number` all prefixed `27`** (Maharashtra) even for Kochi/Chennai outlets → fabricated attribute.
- **Two quantity columns** (`qty`, `quantity_units`) + **two timestamp conventions** (UTC `Z` vs
  site-local) + **two temperature units** → every cross-feed metric needs normalisation first.
- **One corrupt Parquet file** (`reefer_telemetry/dt=2025-07-14/part-00000.parquet`) — already in
  `data/sql/_skipped_files.txt`.

---

## 2. The data model

### 2.1 Logical model (textual ER diagram)
```
                         ref_fiscal_calendar (DATE dim)
                                   │  (calendar_date = event business date)
                                   │
   ref_warehouse_master ──────────┐│┌───────────── ref_carrier_master
     (region, timezone)           │││               (mode, sla_hours)
            ▲ warehouse_code       │││                 ▲  (NO JOIN KEY — gap)
            │                      │││                 ┆
   ┌────────┴─────────┐   ┌────────┴┴┴────────┐   ┌────┴───────────────┐
   │  wms_scan_events  │   │  reefer_telemetry  │   │  (carrier unknown) │
   │  FACT (scan)      │   │  FACT (reading)    │   └────────────────────┘
   │  order_number ────┼──▶│                    │
   │  sku_code ──┐     │   │  warehouse_code ───┘
   └─────────────┼─────┘   └────────────────────┘
                 │
                 ▼
        erp_sales_order_header (FACT: order, after CDC rebuild)
                 │ outlet_code ▼              ▲ order_number
                 │                            │
   ┌─────────────┴────┐              ┌────────┴─────────┐
   │ erp_outlet_master│              │  pos_transactions │ FACT (sales line)
   │ DIM (SCD2)       │◀─outlet_code─┤  outlet_code      │
   │ channel, region  │              │  sku_code ───┐    │
   └──────────────────┘              │  channel     │    │
                                     └──────────────┼────┘
                                                    ▼
                          erp_product_master (DIM, SCD2) ──sku_code── ref_uom_conversion (LOOKUP)
                          category, brand, is_chilled                  eaches_per_case

   ref_legacy_finance_weekly  ── compare by (week_ending, channel) ─▶ POS-derived weekly sales
   manifest_expected_partitions ── compare by (feed, partition) ─────▶ actual partition row counts
```

### 2.2 Join keys, cardinality & confidence
| From | To | Key | Cardinality | Confidence |
|---|---|---|---|---|
| pos_transactions | erp_outlet_master | `outlet_code` | many lines → 1 outlet | **Confirmed** |
| pos_transactions | erp_product_master | `sku_code` | many lines → 1 product | **Confirmed** |
| pos_transactions | ref_uom_conversion | `sku_code` | many → 0/1 (46 SKUs unmatched) | **Confirmed (with gap)** |
| pos_transactions / all events | ref_fiscal_calendar | event business date → `calendar_date` | many → 1 | **Confirmed (after tz fix)** |
| wms_scan_events | erp_sales_order_header | `order_number` | many scans → 1 order | **Nominal** (synthetic, incomplete) |
| wms_scan_events / telemetry / orders / outlets | ref_warehouse_master | `warehouse_code` | many → 1 | **Confirmed** |
| erp_sales_order_header | erp_outlet_master | `outlet_code` | many orders → 1 outlet | **Confirmed** |
| reefer_telemetry | ref_carrier_master | — | unknown | **Cannot confirm (no key)** |
| reefer_telemetry | erp_sales_order_header | `route_code`? | unclear | **Unconfirmed** (both have route_code, no trip link) |
| ref_legacy_finance_weekly | POS weekly aggregate | `week_ending`+`channel` | 1 → 1 (benchmark) | **Confirmed as comparison only** |

### 2.3 Facts vs dimensions (and why)
- **Facts (measurable events):** `pos_transactions` (money/quantity), `reefer_telemetry` (temperature),
  `wms_scan_events` (timestamps for durations), `sales_order_header` (order value). These are high-volume,
  append-style, and sit at an event/line grain.
- **Dimensions (descriptive context):** `outlet_master`, `product_master` (both **SCD2** — attributes
  change over time), `warehouse_master`, `carrier_master`, and the **date** dimension (`fiscal_calendar`).
  `channel` is a **degenerate dimension** (an attribute carried on the fact).
- **Lookup/bridge:** `uom_conversion` (SKU→eaches). **Benchmark:** `legacy_finance_weekly`.
  **Operational metadata:** `manifest_expected_partitions`.

### 2.4 Recommended dimensional model (Gold)
A **star schema** is the right fit (simple, fast, easy to explain); snowflake only if you split
region out of warehouse, which isn't worth it at 8 sites.
- **Fact tables:** `fact_sales` (POS line grain), `fact_cold_chain_readings` (telemetry grain, or a
  daily/trip summary), `fact_wms_scans` (scan grain) plus a derived `fact_order` (order grain).
- **Conformed dimensions:** `dim_date` (from fiscal_calendar), `dim_outlet` (SCD2), `dim_product`
  (SCD2, carrying chilled flag and eaches-per-case), `dim_warehouse` (region, timezone), `dim_channel`.
- **Grain statements** (write these down — they anchor every metric):
  - `fact_sales` = one cleaned, de-duplicated POS line, dated on the **IST business day**, quantity in **eaches**.
  - `fact_cold_chain_readings` = one valid reading, temperature in **°C**, clock-corrected.
  - `fact_order` = one order in its **current** state after CDC rebuild (deletes removed).

### 2.5 Data flow (source → analytics)
```
Parquet/CSV sources ─▶ BRONZE (raw + ingest metadata, corrupt files quarantined)
                   ─▶ SILVER (dedup, tz→IST, °F→°C, qty→eaches, CDC→current/SCD2, typed, FK-validated)
                   ─▶ GOLD (star-schema facts + conformed dims + KPI marts)
                   ─▶ KPI catalogue + SQL library + ask-anything + reconciliation vs legacy/manifest
```

---

## 3. Bronze / Silver / Gold design

Medallion architecture = three stages of increasing trust. **Bronze** = faithful copy of source;
**Silver** = cleaned and conformed; **Gold** = business-ready analytics. Base every rule below on the
real feeds, not generic theory.

### 3.1 Bronze — raw, faithful, traceable
- **Purpose:** an exact, replayable landing copy so you can always trace a number to its origin and
  re-run without re-reading messy sources.
- **Inputs:** all six Parquet feeds + five reference CSVs + the manifest.
- **What to keep:** every source column **as-is** (including both `qty` and `quantity_units`, raw
  `temp_value`/`temp_unit`, all CDC `I/U/D` events — do **not** collapse them here).
- **Ingestion metadata to add:** source file path (POS already has `source_file`), the partition value
  (`ingest_date`/`dt`/`extract_date`), a load timestamp, and a batch/run id.
- **Duplicates & malformed records:** **do not remove** duplicates in Bronze — preserve them and
  de-duplicate in Silver, so the raw record is never lost. **Quarantine** unreadable files (the
  truncated `dt=2025-07-14` telemetry part) into an incident log rather than letting the load crash.
- **Incremental ingestion:** process by partition (one `ingest_date`/`dt`/`extract_date` at a time) so
  re-running a single day is cheap and idempotent.
- **Outputs:** Bronze tables mirroring each feed + an **ingestion/incident log** + observed per-partition
  row counts (to reconcile against the manifest).
- **Completion criteria:** every source partition is represented or logged as an incident; observed row
  counts are captured; no source information has been dropped or altered.

### 3.2 Silver — cleaned, standardized, business-ready
- **Purpose:** one trustworthy version of each entity, with consistent types, units, keys and timezones.
- **Per-feed responsibilities (all grounded in verified issues):**

| Feed | Silver transformations |
|---|---|
| POS | De-duplicate (84k exact dupes) on `(txn_id,txn_line_no)`; **coalesce `qty`/`quantity_units`** into one quantity; default missing `uom` to `EA`; convert **UTC→IST** and derive `event_date_ist`; **convert cases→eaches** via `uom_conversion` (flag the 46 unmatched SKUs instead of guessing); type the money columns. |
| Telemetry | De-duplicate (~3.2% exact dupes); **resolve unit** (`temp_unit`, else infer COLDEYE=F/THERMLOG=C); **convert all to °C**; **correct +7h** on firmware `2.1.4`; mark null `temp_value` as non-measurable (don't count as breach). |
| WMS | Parse **site-local** timestamp (no tz shift needed, all IST); keep stage events; accept that ~6.5% are missing (affects cycle time). |
| Orders | **Rebuild current state** per `order_number` ordered by `(__op_ts,__seq)`; **tombstone** any key with a `D` (remove ~2,880 deleted orders); keep `source_system` and flag `PARTNER_API` as freight-inflated. |
| Outlet / Product | Rebuild per key by `(__op_ts,__seq)`; build **SCD2 history** so you can ask "what was this outlet's channel back then" (needed for reclassification). |

- **Handling nulls / invalid / late / inconsistent:** missing `uom` → default EA + flag; null temperature
  → exclude from excursion math; late-arriving POS (event vs ingest date) → always date facts on the
  **event** day, never the ingest day; out-of-order CDC (`extract_date` after `__op_ts`) → rely on
  `(__op_ts,__seq)`, never file arrival.
- **Business keys / relationship validation:** confirm POS `outlet_code`/`sku_code` resolve to the
  rebuilt dimensions; record orphans rather than dropping them silently.
- **Outputs:** `silver_pos`, `silver_telemetry`, `silver_wms`, `silver_orders_current`,
  `dim_outlet` (SCD2), `dim_product` (SCD2), conformed `dim_warehouse`, `dim_date`.
- **Completion criteria:** one row per business key where expected; a single quantity column in eaches;
  all temperatures in °C; all event timestamps on the correct IST day; deletes removed; orphan and
  exclusion counts recorded.

### 3.3 Gold — analytics & business metrics
- **Purpose:** fast, clearly-defined tables for reporting and the KPI catalogue.
- **Recommended tables:**
  - `fact_sales` (POS line, eaches, IST date, keys to dims).
  - `fact_order` (order current state, with `source_system` and freight-adjusted value).
  - `fact_cold_chain` (reading-level, or a `trip/day × warehouse × vendor` summary with excursion flags).
  - `fact_wms_cycle` (per order: receive→dispatch durations where both exist).
  - Aggregates/marts: `mart_sales_by_channel_period`, `mart_excursion_rate`, `mart_cycle_time`,
    `mart_order_value_by_source`, `mart_data_completeness`.
  - Conformed dims shared across all facts: `dim_date`, `dim_outlet`, `dim_product`, `dim_warehouse`, `dim_channel`.
- **Modeling for reporting:** star joins (fact → dims) support slicing by time, channel, region,
  category, warehouse, vendor with minimal effort.
- **Supports the assignment's outcomes:** the Gold marts are exactly what the KPI catalogue documents and
  what the "ask-anything" tool queries; the reconciliation marts answer the CFO's "does it match the board
  report" and "which days are missing data."
- **Completion criteria:** every KPI in the catalogue maps to one Gold table/mart; each number is
  traceable back through Silver to a Bronze source row; aggregates reconcile to the Silver detail.

---

## 4. Top 5 business KPIs

All five are **confirmed implementable** with the data as-is. A sixth (service level) is listed as
**assumption-dependent**. Reporting dimensions assume the conformed dims above.

### KPI 1 — Gross Sales by Channel  *(Confirmed)*
- **Purpose / question:** "What did we sell, by channel, in a given fiscal period?" The CFO's "one set
  of numbers" starts here.
- **Logic (plain English):** from cleaned POS, value each line as unit price × quantity-in-eaches,
  before discount and tax; sum by channel and fiscal period, counting each sale on its **IST** day,
  after removing duplicates.
- **Sources:** `fact_sales` (from `pos_transactions`) + `dim_date` + `dim_channel`; `uom_conversion`
  feeds the eaches conversion in Silver.
- **Joins / filters / grain:** join POS → date on IST business day; grain = fiscal_year × fiscal_quarter
  (or month) × channel. Exclude duplicates and case-lines with no conversion (count them separately).
- **Interpretation:** the agreed revenue picture per channel; the base for mix analysis and the legacy
  reconciliation.
- **Dimensions:** time (fiscal), channel, region/warehouse, product category.
- **Edge cases / dependencies:** UTC→IST shift; `qty`/`quantity_units` coalesce; missing UOM (46 SKUs);
  duplicates. Getting any of these wrong moves the number.
- **Why valuable:** it is the headline artefact the CFO asked for and exercises the hardest POS issues.

### KPI 2 — Finance Reconciliation (POS vs Legacy Weekly Report)  *(Confirmed, with honest caveat)*
- **Purpose / question:** "Does our number match the board's weekly report, and if not, why?"
- **Logic:** compute weekly gross sales by channel two ways — (a) the **corrected** way (event/IST date,
  de-duplicated) and (b) the **legacy** way (grouped by ingest date, **not** de-duplicated) — and compare
  both to `legacy_finance_weekly`. Explain the variance; do not force an exact match.
- **Sources:** `fact_sales` + `ref_legacy_finance_weekly` + `dim_date`.
- **Joins / filters / grain:** align on `week_ending` × `channel`; weekly grain.
- **Interpretation:** quantifies the two known legacy biases (wrong day, double counting) and shows the
  corrected figure; demonstrates traceability.
- **Dimensions:** week, channel.
- **Edge cases / dependencies:** the legacy numbers are **synthetic/random** and end **2026-06-24**, so an
  exact tie is impossible — the deliverable is the **explained difference**, which is the point of the CFO's ask.
- **Why valuable:** directly answers the CFO; rewards honesty over a forced match.

### KPI 3 — Cold-Chain Excursion Rate  *(Confirmed)*
- **Purpose / question:** "What share of chilled readings/trips breached the 2–8°C band, by month and
  carrier/vendor?" Divya's priority.
- **Logic:** after converting all readings to °C and dropping nulls, flag readings above 8°C; excursion
  rate = breaching readings ÷ valid readings, grouped by month and vendor/warehouse.
- **Sources:** `fact_cold_chain` (from `reefer_telemetry`); optionally `product_master.is_chilled` for scope.
- **Joins / filters / grain:** month × vendor × warehouse; exclude null temperatures.
- **Interpretation:** the true cold-chain health signal.
- **Dimensions:** time (month), vendor, warehouse, region.
- **Edge cases / dependencies:** **the °F→°C conversion is decisive** — COLDEYE is ~33.5% of all readings
  and averages 39.6°F; comparing that raw to "8" makes a third of readings look like breaches, which is
  exactly the false "one-third of trips" figure Divya distrusts. Also: null units (8%), clock skew (9.1%),
  null temps, and the **lack of a true trip id / carrier key** (report by vendor, state the gap).
- **Why valuable:** turns a feared number into a defensible one and showcases multi-vendor normalisation.

### KPI 4 — Warehouse Cycle Time (Dock-to-Dispatch)  *(Confirmed, indicative)*
- **Purpose / question:** "How long do warehouses take from receive to dispatch?"
- **Logic:** per order at a warehouse, take the first RECEIVE and the last DISPATCH timestamp, measure the
  gap, and report the **median** per warehouse (median resists outliers from missing scans).
- **Sources:** `fact_wms_cycle` (from `wms_scan_events`) + `dim_warehouse`.
- **Joins / filters / grain:** per warehouse; only orders that have both a RECEIVE and a DISPATCH.
- **Interpretation:** relative throughput/health by site; a starting baseline, not a contractual SLA.
- **Dimensions:** warehouse, region, time (month).
- **Edge cases / dependencies:** ~6.5% of scans missing and stages are independent, so many orders lack a
  clean pair → **label the KPI "indicative"** and report coverage (how many orders were measurable).
- **Why valuable:** demonstrates event-stitching with honest handling of gaps.

### KPI 5 — Order Value by Source System & Comparability  *(Confirmed)*
- **Purpose / question:** "Order value by source system — are the three sources comparable?"
- **Logic:** from rebuilt current orders, sum `order_value_gross` by `source_system`; present both
  as-reported and a **freight-adjusted** figure for `PARTNER_API` (divide by 1.085); flag the source as
  not directly comparable.
- **Sources:** `fact_order` (from `sales_order_header`).
- **Joins / filters / grain:** per source_system; exclude tombstoned (deleted) orders.
- **Interpretation:** prevents apples-to-oranges totals; surfaces the unresolved 2025 Finance ticket.
- **Dimensions:** source_system, time, region/warehouse.
- **Edge cases / dependencies:** CDC rebuild correctness (order by `__op_ts,__seq`; remove ~2,880 deletes);
  the 1.085 factor is **reverse-engineered** from the 4-decimal tell (`184640.1571`) — label it an assumption.
- **Why valuable:** shows you catch a subtle, money-affecting data trap and handle comparability.

### Suggested KPI 6 — Delivery Service Level vs SLA  *(Assumption-dependent — flag clearly)*
- Would answer "% of orders delivered within carrier SLA hours." **Blocked by data:** there is **no
  `carrier_id`** in the feeds to reach `carrier_master.sla_hours`, and orders **never reach `DELIVERED`**.
  Only build if you either assume a carrier mapping (document it) or redefine the target (e.g.
  dispatch-vs-requested-delivery-date), and state the assumption loudly.

---

## 5. Data quality framework

For each check: **what**, **why**, **layer/table**, **what fails**, **handling**, and **action**
(block / quarantine / warn). A practical rule of thumb: **block** on things that corrupt keys or
make a load wrong; **quarantine** individual bad rows/files; **warn** on known, documented quirks.

### 5.1 Completeness
| Check | Why | Table/Layer | Failure | Handling | Action |
|---|---|---|---|---|---|
| Mandatory keys present (`txn_id`, `scan_id`, `order_number`, `outlet_code`, `sku_code`) | Keys drive every join | Bronze→Silver | Null/blank key | Route row to reject table | **Quarantine** |
| Partition coverage vs manifest | Detect missing days/files | Bronze | A manifest partition has no readable data | Log incident; investigate | **Warn/Block** per feed |
| Quantity present after coalesce | Sales need a quantity | Silver POS | Both `qty` and `quantity_units` null | Reject line | **Quarantine** |
| Temperature present for excursion | Can't judge a null | Silver telemetry | `temp_value` null | Exclude from rate (not a breach) | **Warn** |

### 5.2 Uniqueness
| Check | Why | Table/Layer | Failure | Handling | Action |
|---|---|---|---|---|---|
| POS line uniqueness `(txn_id,txn_line_no)` | 84k exact dupes inflate sales | Silver POS | Duplicate rows | De-duplicate, count removed | **Warn** (expected) |
| Telemetry exact-duplicate readings | ~3.2% dupes inflate rates | Silver telemetry | Duplicate rows | De-duplicate | **Warn** |
| One current row per dimension key | SCD integrity | Silver dims | >1 "current" per key | Keep latest by `(__op_ts,__seq)` | **Block** if unresolved |
| `scan_id` unique | Event integrity | Bronze WMS | Duplicate scan_id | Investigate | **Warn** (none observed) |

### 5.3 Validity
| Check | Why | Table/Layer | Failure | Handling | Action |
|---|---|---|---|---|---|
| `channel` ∈ {GT,MT,HORECA,ECOM} | Guard against new codes | Silver | Unknown value | Map or reject | **Quarantine** |
| `temp_unit` ∈ {C,F,null} and resolvable | Unit drives conversion | Silver telemetry | Unresolvable unit | Infer from vendor; else exclude | **Warn** |
| Temperature in plausible range after °C convert (e.g. −30..30) | Catch bad conversions | Silver | Out of range | Flag/exclude | **Warn** |
| `unit_price`, quantities ≥ 0 | Money sanity | Silver POS | Negative/zero price | Flag for review | **Warn** |
| `event_ts`/`reading_ts` parse as valid UTC; `wms.event_ts` as local | Timezone correctness | Silver | Parse failure | Reject row | **Quarantine** |

### 5.4 Referential integrity
| Check | Why | Table/Layer | Failure | Handling | Action |
|---|---|---|---|---|---|
| POS `outlet_code` → dim_outlet | Valid shop | Silver | Orphan | Keep + flag `unknown_outlet` | **Warn** |
| POS/WMS `sku_code` → dim_product | Valid product | Silver | Orphan | Keep + flag | **Warn** |
| `sku_code` → uom_conversion | Needed for eaches | Silver | 46 SKUs missing | Flag `missing_uom`; exclude from eaches totals | **Warn** |
| `warehouse_code` → dim_warehouse | Region/timezone | Silver | Orphan | Investigate | **Block** (only 8 known) |
| WMS `order_number` → fact_order | Link scans to orders | Gold | Orphan/no match | Measure coverage; don't fabricate | **Warn** |

### 5.5 Consistency
| Check | Why | Table/Layer | Failure | Handling | Action |
|---|---|---|---|---|---|
| POS till `channel` vs outlet master channel | Reclassification / mislabel | Gold | Mismatch | Report as reclassification signal | **Warn** |
| `case_pack` (product) vs `eaches_per_case` (uom) | Two sources of the same fact | Silver | Disagree | Prefer one, document choice | **Warn** |
| Region via warehouse consistent across feeds | Stable geography | Gold | Conflicting region | Use warehouse_master as authority | **Warn** |

### 5.6 Accuracy & reasonableness
| Check | Why | Table/Layer | Failure | Handling | Action |
|---|---|---|---|---|---|
| Excursion rate not absurd (e.g. not ~33% from unit error) | Catch the °F/°C trap | Gold | Rate spikes with raw °F | Verify conversion applied | **Block** metric if unconverted |
| `PARTNER_API` value inflation | Freight double count | Silver orders | 4-dp inflated values | Apply/flag 1.085 adjustment | **Warn** |
| Daily sales within expected band | Spot anomalies | Gold | Large unexplained swing | Investigate (promo? data gap?) | **Warn** |

### 5.7 Timeliness
| Check | Why | Table/Layer | Failure | Handling | Action |
|---|---|---|---|---|---|
| Event vs ingest lag (POS) | Late-arriving sales | Silver | Large lag | Date on event day; monitor backfill | **Warn** |
| Gateway continuity (telemetry) | `GW-017` 2-day outage looks like low volume | Gold | Gateway silent vs its own history | Flag outage window explicitly | **Warn** |
| Reporting period fully present | Trust period totals | Gold | Partial last period | Mark period incomplete | **Warn** |

### 5.8 Reconciliation
| Check | Why | Layer | Failure | Handling | Action |
|---|---|---|---|---|---|
| Source→Bronze row counts vs manifest | Detect truncation/missing | Bronze | Count mismatch (e.g. `dt=2025-07-14`) | Log incident; quarantine file | **Warn/Block** |
| Bronze→Silver counts | Dedup/rejects accounted for | Silver | Unexplained drop | Reconcile removed = dupes+rejects | **Block** if unexplained |
| Silver→Gold metric totals | Aggregates tie to detail | Gold | Sum(detail) ≠ aggregate | Fix aggregation | **Block** |

### 5.9 Transformation validation
- Verify °F→°C on a known COLDEYE row (≈39.6°F → ≈4.2°C); verify eaches conversion on a CS line;
  verify CDC rebuild removes the ~2,880 deleted orders; verify IST shift moves late-night UTC sales to
  the correct day. **Fail = metric is wrong even if the pipeline "ran."**

### 5.10 Historical handling
- Late-arriving POS (event vs ingest date), SCD2 attribute changes on outlets/products, and out-of-order
  CDC extracts: validate that historical reports use the **point-in-time** attribute, not today's value.

### 5.11 Tracking results & gatekeeping
- Keep a **data-quality results table** (check name, layer, table, rows checked, rows failed, timestamp,
  run id) and an **incident/quarantine log** (e.g. the corrupt file, orphan rows).
- Maintain **audit columns** (load time, source file, batch id) through all layers for lineage.
- **Gate Gold on Silver quality:** block Gold refresh if any **blocking** check fails; allow it with a
  visible warning if only **warn-level** checks trip. This stops unreliable data reaching dashboards.

---

## 6. Business use cases

Prioritised by value × feasibility. "Feasible now" = buildable with current data.

| # | Use case | Business problem | Data needed | Analysis/transformation | Outcome | KPI/metric | Feasible now? |
|---|---|---|---|---|---|---|---|
| 1 | **One agreed sales number** | Four teams quote four figures | POS, calendar, UOM, dims | Clean POS, eaches, IST date, aggregate by channel/period | Single trusted revenue view | KPI 1 | **Yes** |
| 2 | **Trust the board report** | Legacy report disputed | POS + legacy weekly | Reproduce legacy method + corrected; compare | Explained variance, corrected figure | KPI 2 | **Yes (explain, not match)** |
| 3 | **Cold-chain truth** | "A third of trips breach" distrusted | Telemetry (+ product chilled) | Normalise units/clock, excursion rate | Real breach rate by month/vendor | KPI 3 | **Yes** |
| 4 | **Warehouse throughput** | No defined cycle time | WMS scans, warehouse dim | Stitch receive→dispatch, median | Baseline cycle time by site | KPI 4 | **Yes (indicative)** |
| 5 | **Comparable order value** | Sources disagree with invoices | Orders (CDC) | Rebuild current, adjust PARTNER_API | Comparable value by source | KPI 5 | **Yes** |
| 6 | **Data observability** | "Missing data looks like low volume" | Manifest + feeds | Reconcile counts, outage/lag detection | Missing-day & outage report | Completeness metric | **Yes** |
| 7 | **Outlet reclassification** | Channel changes untracked | outlet_master CDC (SCD2) | Build history, detect channel changes | Who changed channel, when | Change report | **Yes (needs SCD2)** |
| 8 | **Delivery service level** | On-time performance unknown | Orders + carrier SLA | Join to SLA, compare to delivery | On-time % | KPI 6 | **No (no carrier key / no DELIVERED)** |

---

## 7. Governance & modeling considerations

| Topic | Recommendation | Necessity |
|---|---|---|
| **Naming** | One convention across layers (e.g. `bronze_*`, `silver_*`, `fact_*`, `dim_*`, `mart_*`); snake_case columns. | **Necessary** |
| **Keys** | Business keys = `outlet_code`, `sku_code`, `order_number`, `warehouse_code`. Add **surrogate keys** on SCD2 dims so facts can point at the correct historical version. | **Necessary** (surrogates: recommended) |
| **Slowly changing dimensions** | SCD2 for `dim_outlet` and `dim_product` (track channel/category/price over time) — required for point-in-time reporting and reclassification. | **Necessary for KPI 7 / historical accuracy** |
| **Historical tracking / audit** | Keep `effective_from`/`effective_to` on SCD2 dims; carry `load_ts`, `source_file`, `batch_id`, `__op_ts`, `__seq` through layers. | **Necessary** |
| **PII / sensitive data** | `gst_number`, `cashier_id`, `loyalty_id`, `vehicle_registration`, `gps_lat/lon` are personal/location data. Restrict, avoid in public outputs, consider masking. Note the GST values look fabricated (all `27…`). | **Necessary to acknowledge; masking optional** |
| **Lineage / source-to-target** | Maintain a mapping from each Gold column back to Silver/Bronze/source. The provided DDLs + sample outputs are a good starting data dictionary. | **Necessary (at least documented)** |
| **Incremental & idempotent** | Process per partition; re-running a day yields identical output (the generator's fixed seed helps testing). | **Necessary** |
| **Error handling / rejects / recovery** | Reject tables + incident log + quarantine for corrupt files; ability to replay a partition. | **Necessary** |
| **Ownership of definitions** | KPI catalogue names an **owner** per metric (Finance MIS vs Supply Chain Ops) and the known limitation. | **Necessary (the CFO asked for it)** |

---

## 8. Risks, gaps & assumptions

**Confirmed gaps (facts):**
- **No carrier key** in any event feed → `carrier_master` is effectively orphan; "by carrier" needs an
  assumed mapping. *Validate: ask the assignment owner for a vehicle/route→carrier mapping.*
- **No trip identifier** in telemetry → "trips" must be approximated (device-day). *Validate: confirm the
  intended trip definition.*
- **No `DELIVERED` status** in orders (statuses stop at DISPATCHED) → delivery service level not measurable
  from status. *Validate: confirm whether dispatch is an acceptable proxy.*
- **Legacy report is synthetic/random** and ends **2026-06-24** → exact reconciliation impossible by design.
- **46 SKUs missing from `uom_conversion`** → eaches totals incomplete for those SKUs.
- **One corrupt telemetry file** (`dt=2025-07-14/part-00000`) → excluded; counts won't match manifest there.
- **`GW-017` outage** on 2026-02-11/12 → looks like low volume, not an error.
- **`gst_number` all `27…`** regardless of city → fabricated attribute; don't derive geography from it.

**Assumptions you will make (label each in DECISIONS.md and validate):**
- Business day = **Asia/Kolkata**; POS/telemetry/ERP timestamps are UTC, WMS is already local.
- "Gross sales" = unit_price × quantity-in-eaches, **pre-discount, pre-tax** (state it; it's a choice).
- `unit_price` is **per each** (MRP-based), so case lines are valued after eaches conversion.
- Current state from CDC = latest by **(`__op_ts`,`__seq`)**; any `D` tombstones the key.
- **PARTNER_API** inflation factor = **1.085** (reverse-engineered from 4-dp values; confirm with owner).
- Excursion threshold = **>8°C** after unit normalisation; chilled band 2–8°C.

**Risks to KPI accuracy:** forgetting °F→°C (KPI 3), grouping POS on ingest instead of event date
(KPI 1/2), not removing duplicates (KPI 1/3), not rebuilding CDC correctly (KPI 5), treating missing WMS
scans as real zeros (KPI 4).

> **Do not invent** carrier links, trip definitions, or the legacy report's logic. Where you must proceed,
> proceed on a **clearly labelled assumption** and say how it would be validated.

---

## 9. Implementation plan

Each phase: **what**, **why**, **output/deliverable**, **dependencies**, **done when**. Tasks marked
*(optional)* are enhancements — do them only if core work is solid.

### Phase 1 — Initial assessment
- **What:** confirm files, row counts, schemas (use the provided DDLs + samples), and list the known issues.
- **Why:** you can't model what you don't understand; this prevents wrong assumptions later.
- **Output:** a short data-inventory note (feeds, grains, counts, issues) — much of it is already in §1.
- **Dependencies:** access to `data/` (present).
- **Done when:** you can state each table's grain, keys, and top issues from memory.

### Phase 2 — Data modeling
- **What:** finalise facts/dims, join keys, cardinality, grain statements, and the star schema (§2).
- **Why:** the model is the contract every later layer follows.
- **Output:** a logical model + grain statements + source-to-target sketch.
- **Dependencies:** Phase 1.
- **Done when:** every KPI maps to a fact + dims with a written grain.

### Phase 3 — Bronze implementation
- **What:** land each feed faithfully with ingest metadata; quarantine the corrupt file; capture observed
  per-partition counts.
- **Why:** traceability and replayability; never lose source truth.
- **Output:** Bronze tables + ingestion/incident log + observed counts.
- **Dependencies:** Phase 2.
- **Done when:** all partitions represented or logged; nothing dropped silently.

### Phase 4 — Silver implementation
- **What:** apply the per-feed cleaning rules (§3.2): dedup, tz→IST, °F→°C + clock fix, qty→eaches,
  CDC→current, SCD2 dims, typing, FK validation.
- **Why:** this is where the data becomes trustworthy and comparable.
- **Output:** Silver facts + conformed dims + reject tables + exclusion counts.
- **Dependencies:** Phase 3.
- **Done when:** one row per business key; single eaches quantity; all °C; correct IST dates; deletes gone.

### Phase 5 — Gold implementation
- **What:** build star-schema facts + conformed dims + KPI marts + reconciliation marts (§3.3).
- **Why:** fast, clear tables for reporting and the ask-anything tool.
- **Output:** Gold facts/dims/marts.
- **Dependencies:** Phase 4.
- **Done when:** each KPI has a backing Gold table and aggregates tie to Silver detail.

### Phase 6 — KPI implementation
- **What:** finalise the five KPI definitions (name, grain, filters, source, owner, limitation) and compute them.
- **Why:** the KPI catalogue is the CFO's core artefact.
- **Output:** KPI catalogue entries + the queries behind them (your SQL library).
- **Dependencies:** Phase 5.
- **Done when:** a stranger could reproduce each number from the catalogue alone.

### Phase 7 — Data quality implementation
- **What:** implement and prioritise the checks in §5 across layers; wire the DQ results + incident logs;
  gate Gold on blocking checks.
- **Why:** prevents unreliable data reaching reports; evidence you handle mess deliberately.
- **Output:** DQ results table + incident log + gating rule.
- **Dependencies:** Phases 3–5.
- **Done when:** every known issue has a check that warns/quarantines/blocks as designed.

### Phase 8 — Testing & reconciliation
- **What:** source→Bronze→Silver→Gold count reconciliation; transformation spot-checks (°C, eaches, CDC,
  IST); legacy-report comparison; manifest completeness; `GW-017` outage detection.
- **Why:** proves correctness, not just "it ran."
- **Output:** reconciliation results + a few documented test cases.
- **Dependencies:** Phases 5–7.
- **Done when:** counts reconcile (with dupes/rejects explained) and transformations verified on known rows.

### Phase 9 — Documentation
- **What:** data dictionary (from DDLs), model diagram, lineage/source-to-target, assumptions, DQ outcomes,
  the KPI catalogue, `README.md` (cold-start run) and one-page `DECISIONS.md`.
- **Why:** they read `DECISIONS.md` before the code; documentation is graded.
- **Output:** the docs above in the repo.
- **Dependencies:** Phases 1–8.
- **Done when:** someone can run and understand the project with no verbal help.

### Phase 10 — Final delivery
- **What:** confirm the single documented run command works cold; the ask-anything tool shows its SQL;
  dataset is not committed; commit history is incremental; push to one GitHub repo.
- **Why:** these are explicit submission rules.
- **Output:** the submitted repository + acceptance walk-through.
- **Dependencies:** all prior phases.
- **Done when:** the final checklist (§10) is fully ticked.

**Essential vs optional:** Essential = Phases 1–6, 8–10 for KPIs 1–5 plus the completeness check. Optional
= SCD2-driven reclassification (KPI 7), service level (KPI 6, assumption-heavy), `--scale 10` execution,
and any UI beyond a simple query tool.

---

## 10. Final assignment checklist

**Data inventory & business understanding**
- [ ] Every feed's grain, keys, row count, and known issues documented (§1).
- [ ] Business domain and the two question families (finance / operations) explained.

**Models**
- [ ] Logical model with join keys, cardinality, and confidence (§2.2).
- [ ] Dimensional (star) model: facts, conformed dims, grain statements (§2.4).

**Bronze / Silver / Gold**
- [ ] Bronze: faithful landing + ingest metadata + corrupt-file quarantine.
- [ ] Silver: dedup, tz→IST, °F→°C + clock fix, qty→eaches, CDC→current, SCD2 dims, FK validation.
- [ ] Gold: star-schema facts/dims + KPI & reconciliation marts.

**Lineage & mapping**
- [ ] Source-to-target mapping and audit columns (`load_ts`, `source_file`, `batch_id`, `__op_ts`, `__seq`).

**KPIs (5 prioritised, defined)**
- [ ] Gross sales by channel; Finance reconciliation; Cold-chain excursion rate; Warehouse cycle time; Order value by source.
- [ ] Each with name, definition, grain, filters, sources, owner, limitation.
- [ ] Service level (KPI 6) explicitly marked assumption-dependent (or omitted with reason).

**Data quality**
- [ ] Checks implemented across completeness, uniqueness, validity, referential integrity, consistency,
      reasonableness, timeliness, reconciliation, transformation, historical (§5).
- [ ] DQ results table + incident/quarantine log + Gold gating rule.

**Testing & reconciliation**
- [ ] Source→Bronze→Silver→Gold counts reconcile (dupes/rejects explained).
- [ ] Transformation spot-checks pass (°C, eaches, CDC delete removal, IST day).
- [ ] Legacy comparison + manifest completeness + GW-017 outage detected.

**Use cases & reporting**
- [ ] Prioritised use-case list with feasibility (§6) and suggested reporting dimensions.

**Assumptions, limitations, open questions**
- [ ] All assumptions labelled (tz, gross definition, 1.085 factor, excursion threshold, CDC ordering).
- [ ] Gaps noted (carrier key, trip id, DELIVERED status, synthetic legacy report, missing UOM SKUs).
- [ ] Questions for the assignment owner listed.

**Deliverables & acceptance**
- [ ] One command runs the pipeline cold (README).
- [ ] KPI catalogue + SQL library organised and runnable.
- [ ] Ask-anything capability shows the SQL it ran.
- [ ] `DECISIONS.md` ≤ 1 page (built / not built / assumptions / next / breaks-first-at-what-volume).
- [ ] Dataset **not** committed; generator referenced; incremental commit history; single GitHub repo.

---

### Appendix — verified quick facts (scale 1)
- POS: 4,084,000 rows; 4,000,000 txns; **84,000 duplicate rows**; split `qty`/`quantity_units` ≈ 2.04M/2.04M; `uom` null (pre-drift) 2.04M, EA 1.65M, CS 0.39M.
- Telemetry: **3,713,926 readable** rows (+1 corrupt file); COLDEYE=°F avg **39.6** (~1.24M rows ≈ 33.5%), THERMLOG=°C avg **4.2**; `temp_unit` null **8.0%**; firmware 2.1.4 **9.1%**; null temp 22,212.
- WMS: 1,496,000 rows, all unique `scan_id`; six stages ~even.
- Orders: 963,307 events → **320,000 orders**; I/U/D = 320,000 / 640,427 / **2,880**; sources SFA_MOBILE 577,759 / ERP_WEB 288,828 / PARTNER_API 96,720; statuses CREATED/CONFIRMED/PICKED/DISPATCHED (**no DELIVERED**).
- Outlets: 8,774 events → **2,400 outlets**. Products: 3,536 events → **1,100 SKUs** (Dairy+Frozen chilled).
- UOM lookup: **1,054 SKUs** (46 missing). Warehouses: 8 (all Asia/Kolkata). Carriers: 5 (no join key). Calendar: 546 days (FY Apr–Mar). Legacy report: 78 weeks × 4 channels, synthetic, ends 2026-06-24.

*Grounded in the actual files in `data/`. Where the data could not confirm a relationship (carrier, trip,
delivery), this document says so rather than inventing one.*
