#!/usr/bin/env python3
"""
Kestrel Provisions: BRONZE ingestion (raw parquet/csv -> DuckDB).

Principles
  * Bronze = as-is. No dedup, no unit conversion, no timezone fixes. Those belong in silver.
  * Never crash. Every file, feed and step is isolated; a bad file is quarantined and the
    run carries on. Exit code is 0 unless --strict is passed.
  * Idempotent. Re-running skips unchanged files; changed files are reloaded cleanly.
  * Everything is auditable from SQL: bronze_run_log, bronze_file_log, bronze_quarantine_files,
    bronze_schema_drift_log, bronze_partition_reconciliation, bronze_issue_log.

Usage
    python ingestion/bronze_ingest.py --data-dir data --db warehouse/kestrel.duckdb
    python ingestion/bronze_ingest.py --full-refresh          # drop + reload everything
    python ingestion/bronze_ingest.py --feeds pos_transactions reefer_telemetry
"""
import argparse, collections, datetime as dt, hashlib, json, logging, re, shutil, sys, traceback, uuid
from dataclasses import dataclass
from pathlib import Path

import duckdb

BRONZE_SCHEMA = "bronze_layer"   # change this one line to rename the schema

LINEAGE = [("_bronze_run_id", "VARCHAR"), ("_bronze_file", "VARCHAR"), ("_bronze_loaded_at", "TIMESTAMP")]
LINEAGE_NAMES = {n for n, _ in LINEAGE}


def parse_cols(spec):
    out = []
    for item in spec.split(","):
        name, typ = item.split()
        out.append((name.strip('"'), typ))
    return out


# --------------------------------------------------------------------------- bronze schemas (your DDL)
TABLES = {
    "pos_transactions": parse_cols("""txn_id VARCHAR, txn_line_no SMALLINT, basket_id VARCHAR, outlet_code VARCHAR,
        channel VARCHAR, sku_code VARCHAR, event_ts VARCHAR, qty INTEGER, unit_price DOUBLE, discount_amount DOUBLE,
        tax_amount DOUBLE, payment_mode VARCHAR, till_id SMALLINT, cashier_id VARCHAR, promo_code VARCHAR,
        source_file VARCHAR, quantity_units INTEGER, uom VARCHAR, loyalty_id VARCHAR, ingest_date DATE"""),
    "reefer_telemetry": parse_cols("""device_id VARCHAR, telemetry_vendor VARCHAR, firmware_version VARCHAR,
        vehicle_registration VARCHAR, route_code VARCHAR, warehouse_code VARCHAR, gateway_id VARCHAR,
        reading_ts VARCHAR, temp_value DOUBLE, temp_unit VARCHAR, humidity_pct DOUBLE, door_open_flag TINYINT,
        gps_lat DOUBLE, gps_lon DOUBLE, battery_pct DOUBLE, dt DATE"""),
    "wms_scan_events": parse_cols("""scan_id VARCHAR, warehouse_code VARCHAR, event_type VARCHAR, order_number VARCHAR,
        sku_code VARCHAR, batch_id VARCHAR, qty_cases SMALLINT, pallet_id VARCHAR, dock_door TINYINT,
        operator_id VARCHAR, handheld_device VARCHAR, event_ts VARCHAR, dt DATE"""),
    "erp_outlet_master": parse_cols("""outlet_code VARCHAR, __op VARCHAR, __op_ts VARCHAR, __seq BIGINT,
        outlet_name VARCHAR, channel VARCHAR, outlet_format VARCHAR, city VARCHAR, route_code VARCHAR,
        warehouse_code VARCHAR, credit_limit DOUBLE, credit_terms_days BIGINT, gst_number VARCHAR,
        status VARCHAR, extract_date DATE"""),
    "erp_product_master": parse_cols("""sku_code VARCHAR, __op VARCHAR, __op_ts VARCHAR, __seq BIGINT,
        product_name VARCHAR, category VARCHAR, brand VARCHAR, case_pack BIGINT, mrp DOUBLE, list_price DOUBLE,
        gst_rate_pct DOUBLE, shelf_life_days BIGINT, is_chilled BIGINT, status VARCHAR, extract_date DATE"""),
    "erp_sales_order_header": parse_cols("""order_number VARCHAR, __op VARCHAR, __op_ts VARCHAR, __seq BIGINT,
        outlet_code VARCHAR, warehouse_code VARCHAR, route_code VARCHAR, order_date VARCHAR,
        requested_delivery_date VARCHAR, order_status VARCHAR, line_count SMALLINT, order_value_gross DOUBLE,
        discount_amount DOUBLE, tax_amount DOUBLE, source_system VARCHAR, extract_date DATE"""),
    "ref_uom_conversion": parse_cols("sku_code VARCHAR, eaches_per_case BIGINT, base_uom VARCHAR, case_uom VARCHAR"),
    "ref_warehouse_master": parse_cols("""warehouse_code VARCHAR, warehouse_name VARCHAR, city VARCHAR,
        region_name VARCHAR, timezone VARCHAR, chilled_capacity_pallets BIGINT"""),
    "ref_carrier_master": parse_cols("carrier_id VARCHAR, carrier_name VARCHAR, mode VARCHAR, sla_hours BIGINT, rate_per_km DOUBLE"),
    "ref_fiscal_calendar": parse_cols("""calendar_date DATE, fiscal_year VARCHAR, fiscal_quarter VARCHAR,
        fiscal_month_no BIGINT, iso_week BIGINT, day_of_week VARCHAR, is_weekend BIGINT"""),
    "ref_legacy_finance_weekly": parse_cols("""week_ending DATE, channel VARCHAR, gross_sales_inr DOUBLE,
        units_sold BIGINT, basket_count BIGINT, prepared_by VARCHAR, note VARCHAR"""),
    "manifest_expected_partitions": parse_cols("feed VARCHAR, partition VARCHAR, file_count BIGINT, row_count BIGINT, bytes BIGINT"),
}


@dataclass
class Feed:
    name: str        # logical feed name (matches manifest.feed for the 3 manifest feeds)
    rel_root: str    # folder under data-dir
    table: str
    part_col: str


FEEDS = [
    Feed("pos_transactions", "raw/pos_transactions", "pos_transactions", "ingest_date"),
    Feed("reefer_telemetry", "raw/reefer_telemetry", "reefer_telemetry", "dt"),
    Feed("wms_scan_events", "raw/wms_scan_events", "wms_scan_events", "dt"),
    Feed("erp_outlet_master", "raw/erp_cdc/outlet_master", "erp_outlet_master", "extract_date"),
    Feed("erp_product_master", "raw/erp_cdc/product_master", "erp_product_master", "extract_date"),
    Feed("erp_sales_order_header", "raw/erp_cdc/sales_order_header", "erp_sales_order_header", "extract_date"),
]

# (relative path, table) : full-refresh CSVs, transactional (a bad file keeps the previous load)
CSV_SOURCES = [
    ("_manifest/expected_partitions.csv", "manifest_expected_partitions"),
    ("reference/uom_conversion.csv", "ref_uom_conversion"),
    ("reference/warehouse_master.csv", "ref_warehouse_master"),
    ("reference/carrier_master.csv", "ref_carrier_master"),
    ("reference/fiscal_calendar.csv", "ref_fiscal_calendar"),
    ("reference/legacy_finance_weekly_report.csv", "ref_legacy_finance_weekly"),
]

META_DDL = [
    """CREATE TABLE IF NOT EXISTS bronze_run_log (run_id VARCHAR, started_at TIMESTAMP, finished_at TIMESTAMP,
        status VARCHAR, summary VARCHAR)""",
    """CREATE TABLE IF NOT EXISTS bronze_file_log (file_path VARCHAR, feed VARCHAR, partition_key VARCHAR,
        file_size BIGINT, file_mtime DOUBLE, status VARCHAR, expected_rows BIGINT, loaded_rows BIGINT,
        schema_signature VARCHAR, error VARCHAR, run_id VARCHAR, processed_at TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS bronze_quarantine_files (run_id VARCHAR, feed VARCHAR, partition_key VARCHAR,
        file_path VARCHAR, quarantine_path VARCHAR, reason_code VARCHAR, error_message VARCHAR,
        file_size BIGINT, quarantined_at TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS bronze_schema_drift_log (run_id VARCHAR, feed VARCHAR, example_file VARCHAR,
        schema_signature VARCHAR, change_type VARCHAR, column_name VARCHAR, expected_type VARCHAR,
        observed_type VARCHAR, detected_at TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS bronze_partition_reconciliation (run_id VARCHAR, feed VARCHAR,
        partition_key VARCHAR, status VARCHAR, detail VARCHAR, expected_files BIGINT, disk_files BIGINT,
        loaded_files BIGINT, quarantined_files BIGINT, expected_rows BIGINT, loaded_rows BIGINT,
        expected_bytes BIGINT, disk_bytes BIGINT, checked_at TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS bronze_issue_log (run_id VARCHAR, severity VARCHAR, component VARCHAR,
        message VARCHAR, detail VARCHAR, logged_at TIMESTAMP)""",
]


def qs(s):   # SQL string literal
    return "'" + str(s).replace("'", "''") + "'"


def qi(s):   # SQL identifier
    return '"' + str(s).replace('"', '""') + '"'


class Ctx:
    def __init__(self, con, run_id, data_dir, qdir, log):
        self.con, self.run_id, self.data_dir, self.qdir, self.log = con, run_id, data_dir, qdir, log
        self.stats = collections.Counter()
        self.seen_sigs = set()
        self.tcols = {}          # table -> {col: type}  (data columns only, no lineage)
        self.disk = {}           # feed -> {partition_dir: [n_parquet_files, bytes]}
        self.issue_count = 0

    def count(self, feed, outcome):
        self.stats[(feed, outcome)] += 1

    def exec(self, sql, params=None):
        """Metadata write that must never raise."""
        try:
            self.con.execute(sql, params or [])
        except Exception as e:  # noqa
            self.log.error("metadata write failed (%s): %s", e, sql[:90])

    def issue(self, severity, component, message, detail=""):
        self.issue_count += 1 if severity in ("ERROR", "CRITICAL", "WARNING") else 0
        getattr(self.log, "error" if severity in ("ERROR", "CRITICAL") else "warning")("[%s] %s: %s", component, message, detail[:300])
        self.exec("INSERT INTO bronze_issue_log VALUES (?,?,?,?,?,?)",
                  [self.run_id, severity, component, message, detail[:2000], dt.datetime.now()])


# --------------------------------------------------------------------------- setup
def setup_schema(ctx, full_refresh):
    con = ctx.con
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {qi(BRONZE_SCHEMA)}")
    con.execute(f"USE {qi(BRONZE_SCHEMA)}")   # every unqualified table below now lives in bronze_layer
    for ddl in META_DDL:
        con.execute(ddl)
    for table, cols in TABLES.items():
        if full_refresh:
            con.execute(f"DROP TABLE IF EXISTS {qi(table)}")
        all_cols = cols + LINEAGE
        con.execute(f"CREATE TABLE IF NOT EXISTS {qi(table)} (" + ", ".join(f"{qi(n)} {t}" for n, t in all_cols) + ")")
        # tables created earlier from the plain DDL (no lineage cols) or missing columns: heal them
        for n, t in all_cols:
            con.execute(f"ALTER TABLE {qi(table)} ADD COLUMN IF NOT EXISTS {qi(n)} {t}")
        ctx.tcols[table] = {n: t for n, t in con.execute(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema=? AND table_name=?",
            [BRONZE_SCHEMA, table]).fetchall()
            if n not in LINEAGE_NAMES}
    if full_refresh:
        con.execute("DELETE FROM bronze_file_log")
    con.execute("""CREATE OR REPLACE VIEW v_bronze_open_issues AS
        SELECT * FROM bronze_partition_reconciliation
        WHERE run_id = (SELECT max(run_id) FROM bronze_partition_reconciliation) AND status <> 'OK'""")


# --------------------------------------------------------------------------- file log / quarantine
def log_file(ctx, path, feed, pkey, size, mtime, status, exp_rows=None, loaded=None, sig=None, err=None):
    ctx.exec("DELETE FROM bronze_file_log WHERE file_path=?", [str(path)])
    ctx.exec("INSERT INTO bronze_file_log VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
             [str(path), feed, pkey, size, mtime, status, exp_rows, loaded, sig,
              (err or "")[:2000] or None, ctx.run_id, dt.datetime.now()])


def quarantine(ctx, feed, pname, f, reason, err):
    f = Path(f)
    try:
        st = f.stat(); size, mtime = st.st_size, st.st_mtime
    except OSError:
        size, mtime = None, None
    qpath = None
    try:
        dest = ctx.qdir / feed / pname
        dest.mkdir(parents=True, exist_ok=True)
        shutil.copy2(f, dest / f.name)       # copy, never move: bronze source stays untouched
        qpath = str(dest / f.name)
    except Exception as e:  # noqa
        qpath = f"COPY_FAILED: {e}"
    ctx.exec("INSERT INTO bronze_quarantine_files VALUES (?,?,?,?,?,?,?,?,?)",
             [ctx.run_id, feed, pname, str(f), qpath, reason, str(err)[:2000], size, dt.datetime.now()])
    log_file(ctx, f, feed, pname, size, mtime, "QUARANTINED", err=f"{reason}: {err}")
    ctx.count(feed, "QUARANTINED")
    ctx.log.warning("QUARANTINED %s [%s] %s", f, reason, str(err)[:200])


# --------------------------------------------------------------------------- parquet ingestion
def parse_partition(pname, col):
    m = re.fullmatch(rf"{re.escape(col)}=(\d{{4}}-\d{{2}}-\d{{2}})", pname)
    if not m:
        raise ValueError(f"partition dir '{pname}' does not match '{col}=YYYY-MM-DD'")
    return dt.date.fromisoformat(m.group(1)).isoformat()   # also rejects 2025-02-30


def log_drift(ctx, feed, path, sig, fcols):
    tcols = ctx.tcols[feed.table]
    now = dt.datetime.now()
    rows = []
    for n, t in fcols.items():
        if n == feed.part_col:
            continue
        if n not in tcols:
            rows.append((feed.name, path, sig, "NEW_COLUMN", n, None, t))
        elif t.upper() != tcols[n].upper():
            rows.append((feed.name, path, sig, "TYPE_CHANGE", n, tcols[n], t))
    for n, t in tcols.items():
        if n != feed.part_col and n not in fcols:
            rows.append((feed.name, path, sig, "MISSING_COLUMN", n, t, None))
    for r in rows:
        ctx.exec("INSERT INTO bronze_schema_drift_log VALUES (?,?,?,?,?,?,?,?,?)", [ctx.run_id, *r, now])
    if rows:
        ctx.log.info("schema drift in %s (sig %s): %s", feed.name, sig, ", ".join(f"{r[3]}:{r[4]}" for r in rows))


def ingest_parquet_file(ctx, feed, pname, f):
    con, path = ctx.con, str(f)
    try:
        st = f.stat()
    except OSError as e:
        quarantine(ctx, feed.name, pname, f, "FILE_UNREADABLE", e); return

    prev = con.execute("SELECT file_size, file_mtime, status FROM bronze_file_log WHERE file_path=?", [path]).fetchone()
    unchanged = prev and prev[0] == st.st_size and prev[1] == st.st_mtime
    if unchanged and prev[2] == "LOADED":
        ctx.count(feed.name, "SKIPPED_UNCHANGED"); return
    if unchanged and prev[2] == "QUARANTINED":
        ctx.count(feed.name, "SKIPPED_ALREADY_QUARANTINED"); return
    reload_ = bool(prev and prev[2] == "LOADED")

    try:
        pval = parse_partition(pname, feed.part_col)
    except ValueError as e:
        quarantine(ctx, feed.name, pname, f, "BAD_PARTITION_NAME", e); return

    # 1. can the file be opened at all? (truncated file => footer missing => fails here)
    try:
        desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet({qs(path)})").fetchall()
        exp_rows = con.execute(f"SELECT sum(num_rows) FROM parquet_file_metadata({qs(path)})").fetchone()[0] or 0
    except Exception as e:  # noqa
        quarantine(ctx, feed.name, pname, f, "UNREADABLE_PARQUET", e); return

    fcols = {r[0]: r[1] for r in desc}
    sig = hashlib.md5(json.dumps(sorted(fcols.items())).encode()).hexdigest()[:10]

    # 2. schema drift: log once per signature, evolve the bronze table for new columns
    try:
        if (feed.name, sig) not in ctx.seen_sigs:
            ctx.seen_sigs.add((feed.name, sig))
            log_drift(ctx, feed, path, sig, fcols)
        for n, t in fcols.items():
            if n != feed.part_col and n not in ctx.tcols[feed.table]:
                con.execute(f"ALTER TABLE {qi(feed.table)} ADD COLUMN IF NOT EXISTS {qi(n)} {t}")
                ctx.tcols[feed.table][n] = t
    except Exception as e:  # noqa
        quarantine(ctx, feed.name, pname, f, "SCHEMA_EVOLUTION_FAILED", e); return

    # 3. load inside a transaction, verify row count, commit or roll back
    tcols = ctx.tcols[feed.table]
    sel = [f"CAST({qi(n)} AS {tcols[n]}) AS {qi(n)}" for n in fcols if n != feed.part_col and n in tcols]
    sel.append(f"CAST({qs(pval)} AS DATE) AS {qi(feed.part_col)}")   # partition value comes from the path
    sel += ["? AS _bronze_run_id", "? AS _bronze_file", "CAST(? AS TIMESTAMP) AS _bronze_loaded_at"]
    sql = f"INSERT INTO {qi(feed.table)} BY NAME SELECT {', '.join(sel)} FROM read_parquet({qs(path)})"
    try:
        con.execute("BEGIN")
        if reload_:
            con.execute(f"DELETE FROM {qi(feed.table)} WHERE _bronze_file = ?", [path])
        loaded = con.execute(sql, [ctx.run_id, path, dt.datetime.now()]).fetchone()[0]
        if loaded != exp_rows:
            raise RuntimeError(f"row count mismatch: parquet metadata={exp_rows} inserted={loaded}")
        con.execute("COMMIT")
    except Exception as e:  # noqa
        try:
            con.execute("ROLLBACK")
        except Exception:  # noqa
            pass
        quarantine(ctx, feed.name, pname, f, "LOAD_FAILED", e); return

    log_file(ctx, f, feed.name, pname, st.st_size, st.st_mtime, "LOADED", exp_rows, loaded, sig)
    ctx.count(feed.name, "LOADED")
    ctx.stats[(feed.name, "ROWS")] += loaded


def ingest_feed(ctx, feed):
    root = ctx.data_dir / feed.rel_root
    ctx.disk[feed.name] = {}
    if not root.is_dir():
        ctx.issue("ERROR", feed.name, "feed root folder missing", str(root)); return
    n = 0
    on_disk = set()
    for pdir in sorted(p for p in root.iterdir() if p.is_dir()):
        files = sorted(x for x in pdir.iterdir() if x.is_file())
        pq_files = [x for x in files if x.suffix == ".parquet"]
        ctx.disk[feed.name][pdir.name] = [len(pq_files), sum(x.stat().st_size for x in pq_files)]
        if not pq_files:
            ctx.issue("WARNING", feed.name, "partition has no parquet files", pdir.name)
        for f in files:
            if f.suffix != ".parquet":
                ctx.count(feed.name, "IGNORED_NON_PARQUET"); continue
            on_disk.add(str(f))
            try:
                ingest_parquet_file(ctx, feed, pdir.name, f)
            except Exception as e:  # last-resort guard: nothing escapes
                ctx.issue("ERROR", feed.name, "unexpected failure on file", f"{f}: {e}\n{traceback.format_exc()}")
                try:
                    ctx.con.execute("ROLLBACK")
                except Exception:  # noqa
                    pass
            n += 1
            if n % 250 == 0:
                ctx.log.info("%s: %d files processed", feed.name, n)
    # files we loaded before that are gone now (deleted partition / part file)
    for (p,) in ctx.con.execute("SELECT file_path FROM bronze_file_log WHERE feed=? AND status IN ('LOADED','QUARANTINED')",
                                [feed.name]).fetchall():
        if p not in on_disk:
            ctx.exec("UPDATE bronze_file_log SET status='SOURCE_MISSING', run_id=? WHERE file_path=?", [ctx.run_id, p])
            ctx.issue("WARNING", feed.name, "previously ingested file no longer on disk (bronze rows kept)", p)


# --------------------------------------------------------------------------- csv (reference + manifest)
def ingest_csv(ctx, rel, table):
    con, path = ctx.con, ctx.data_dir / rel
    feed = f"csv:{table}"
    if not path.is_file():
        ctx.issue("ERROR", feed, "source file missing, previous bronze load (if any) retained", str(path))
        log_file(ctx, path, feed, None, None, None, "MISSING", err="file not found")
        ctx.count(feed, "MISSING"); return
    st = path.stat()
    try:
        hdr = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_csv({qs(path)}, header=true, all_varchar=true)").fetchall()]
        want = ctx.tcols[table]
        missing = [c for c in want if c not in hdr]
        if missing:
            raise ValueError(f"header missing columns {missing}; found {hdr}")
        extra = [c for c in hdr if c not in want]
        for c in extra:
            ctx.exec("INSERT INTO bronze_schema_drift_log VALUES (?,?,?,?,?,?,?,?,?)",
                     [ctx.run_id, feed, str(path), None, "NEW_COLUMN_IGNORED", c, None, "VARCHAR", dt.datetime.now()])
        exp = con.execute(f"SELECT count(*) FROM read_csv({qs(path)}, header=true, all_varchar=true)").fetchone()[0]
        sel = ", ".join(f"CAST({qi(c)} AS {t}) AS {qi(c)}" for c, t in want.items())
        con.execute("BEGIN")
        con.execute(f"DELETE FROM {qi(table)}")
        loaded = con.execute(
            f"INSERT INTO {qi(table)} BY NAME SELECT {sel}, ? AS _bronze_run_id, ? AS _bronze_file, "
            f"CAST(? AS TIMESTAMP) AS _bronze_loaded_at FROM read_csv({qs(path)}, header=true, all_varchar=true)",
            [ctx.run_id, str(path), dt.datetime.now()]).fetchone()[0]
        if loaded != exp:
            raise RuntimeError(f"row count mismatch csv={exp} inserted={loaded}")
        con.execute("COMMIT")
        log_file(ctx, path, feed, None, st.st_size, st.st_mtime, "LOADED", exp, loaded)
        ctx.count(feed, "LOADED"); ctx.stats[(feed, "ROWS")] += loaded
    except Exception as e:  # noqa
        try:
            con.execute("ROLLBACK")
        except Exception:  # noqa
            pass
        quarantine(ctx, feed, "csv", path, "CSV_LOAD_FAILED", e)
        ctx.issue("ERROR", feed, "csv load failed, previous bronze load retained", str(e))


# --------------------------------------------------------------------------- reconciliation vs manifest
def reconcile(ctx):
    con = ctx.con
    man = con.execute('SELECT feed, "partition", file_count, row_count, bytes FROM manifest_expected_partitions').fetchall()
    if not man:
        ctx.issue("ERROR", "reconcile", "manifest table is empty/not loaded: reconciliation skipped"); return
    expected = collections.defaultdict(dict)
    for f, p, fc, rc, b in man:
        expected[f][p] = (fc, rc, b)
    now = dt.datetime.now()
    summary = collections.Counter()
    for feed in FEEDS:
        if feed.name not in expected:
            continue
        exp, disk = expected[feed.name], ctx.disk.get(feed.name, {})
        loaded_rows = dict(con.execute(
            f"SELECT {qs(feed.part_col + '=')} || CAST({qi(feed.part_col)} AS VARCHAR), count(*) FROM {qi(feed.table)} GROUP BY 1").fetchall())
        fstat = collections.defaultdict(dict)
        for p, s, c in con.execute("SELECT partition_key, status, count(*) FROM bronze_file_log WHERE feed=? GROUP BY 1,2",
                                   [feed.name]).fetchall():
            fstat[p][s] = c
        out = []
        for p in sorted(set(exp) | set(disk)):
            efc, erc, eb = exp.get(p, (None, None, None))
            dfc, db = disk.get(p, (0, 0))
            lf, qf = fstat[p].get("LOADED", 0), fstat[p].get("QUARANTINED", 0)
            lr = loaded_rows.get(p, 0)
            flags = []
            if p not in exp:
                status = "UNEXPECTED_PARTITION"
            else:
                if p not in disk:
                    flags.append("MISSING_PARTITION")
                if qf:
                    flags.append("QUARANTINED_FILES")
                if p in disk and efc != dfc:
                    flags.append(f"FILE_COUNT(exp={efc},disk={dfc})")
                if p in disk and eb != db:
                    flags.append(f"BYTES(exp={eb},disk={db})")
                if erc != lr:
                    flags.append(f"ROW_COUNT(exp={erc},loaded={lr})")
                status = flags[0].split("(")[0] if flags else "OK"
            summary[status] += 1
            out.append([ctx.run_id, feed.name, p, status, "; ".join(flags), efc, dfc, lf, qf, erc, lr, eb, db, now])
        try:
            con.executemany("INSERT INTO bronze_partition_reconciliation VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", out)
        except Exception as e:  # noqa
            ctx.issue("ERROR", "reconcile", f"could not write reconciliation for {feed.name}", str(e))
        for r in out:
            if r[3] != "OK":
                ctx.log.warning("RECON %-18s %-24s %-22s %s", r[1], r[2], r[3], r[4])
    ctx.stats[("reconcile", "summary")] = dict(summary)
    ctx.log.info("reconciliation summary: %s", dict(summary))


# --------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--db", default="warehouse/kestrel.duckdb")
    ap.add_argument("--quarantine-dir", default=None, help="default: <data-dir>/_quarantine")
    ap.add_argument("--report-dir", default=None, help="default: <data-dir>/_reports")
    ap.add_argument("--feeds", nargs="*", default=None, help="subset of feed names")
    ap.add_argument("--full-refresh", action="store_true")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--memory-limit", default="4GB")
    ap.add_argument("--strict", action="store_true", help="exit 1 if any issue/quarantine (default: always exit 0)")
    a = ap.parse_args()

    data_dir = Path(a.data_dir)
    qdir = Path(a.quarantine_dir) if a.quarantine_dir else data_dir / "_quarantine"
    rdir = Path(a.report_dir) if a.report_dir else data_dir / "_reports"
    rdir.mkdir(parents=True, exist_ok=True)
    run_id = dt.datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(rdir / f"bronze_{run_id}.log")])
    log = logging.getLogger("bronze")
    started = dt.datetime.now()
    log.info("run %s | data=%s db=%s full_refresh=%s", run_id, data_dir, a.db, a.full_refresh)

    Path(a.db).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(a.db)
    con.execute(f"PRAGMA threads={int(a.threads)}")
    con.execute(f"PRAGMA memory_limit={qs(a.memory_limit)}")
    ctx = Ctx(con, run_id, data_dir, qdir, log)
    status = "SUCCESS"

    def step(name, fn, *args):
        try:
            fn(*args)
        except Exception as e:  # a failed step must not stop later steps
            ctx.issue("CRITICAL", name, f"step crashed: {e}", traceback.format_exc())

    try:
        step("setup_schema", setup_schema, ctx, a.full_refresh)
        step("csv:manifest", ingest_csv, ctx, *CSV_SOURCES[0])        # manifest first
        for feed in FEEDS:
            if a.feeds and feed.name not in a.feeds:
                continue
            step(f"feed:{feed.name}", ingest_feed, ctx, feed)
        for rel, table in CSV_SOURCES[1:]:
            step(f"csv:{table}", ingest_csv, ctx, rel, table)
        if not a.feeds:
            step("reconcile", reconcile, ctx)
    finally:
        quarantined = sum(v for (f, o), v in ctx.stats.items() if o == "QUARANTINED")
        if ctx.issue_count or quarantined:
            status = "COMPLETED_WITH_ISSUES"
        summary = {f"{f}|{o}": v for (f, o), v in ctx.stats.items()}
        ctx.exec("INSERT INTO bronze_run_log VALUES (?,?,?,?,?)",
                 [run_id, started, dt.datetime.now(), status, json.dumps(summary, default=str)])
        try:
            (rdir / f"bronze_{run_id}.json").write_text(json.dumps(
                {"run_id": run_id, "status": status, "quarantined_files": quarantined, "stats": summary}, indent=2, default=str))
        except Exception:  # noqa
            pass
        try:
            con.execute("CHECKPOINT")
            print(f"\n=== bronze row counts (run {run_id}: {status}) ===")
            for t in TABLES:
                print(f"  {t:<30} {con.execute(f'SELECT count(*) FROM {qi(t)}').fetchone()[0]:>12,}")
            print(f"  quarantined files this run: {quarantined}")
            con.close()
        except Exception:  # noqa
            pass
    return 1 if (a.strict and status != "SUCCESS") else 0


if __name__ == "__main__":
    sys.exit(main())
