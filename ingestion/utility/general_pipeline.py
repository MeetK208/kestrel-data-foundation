#!/usr/bin/env python3
"""
Generic bronze -> silver pipeline step. One function, works for every table.

    from silver_pipeline import run_silver_pipeline, PipelineError

    stats = run_silver_pipeline(
        source_schema="bronze_layer", source_table="pos_transactions",
        target_schema="silver_layer", target_table="pos_transactions",
        config_path="kestrel-data-foundation/ingestion/silver_layer/config.yaml",
        db_path="warehouse/kestrel.duckdb")                    # db_path optional

What it does
  1. Reads the config entry for the table (root key = target_table, falls back to source_table).
  2. validate_tables : bronze must exist, silver is created from its DDL if missing,
                       both live tables must match the config columns.
  3. run_sql_file    : runs <sql_transformation folder of target_schema>/<target_table>.sql
  4. Returns the stats. Any failure raises PipelineError (.step says where: config / validate / transform),
     and nothing after the failed step is run.

Needs validate_tables.py and run_sql.py in the same folder.  pip install duckdb pyyaml
"""
import logging
import sys
import time
from pathlib import Path

import duckdb
import yaml

_UTILITY_DIR = Path(__file__).resolve().parent                    # .../kestrel-data-foundation/ingestion/utility
REPO_ROOT = _UTILITY_DIR.parents[1]                                # .../kestrel-data-foundation
if str(_UTILITY_DIR) not in sys.path:
    sys.path.insert(0, str(_UTILITY_DIR))
from validate_tables import ValidationError, validate_tables      # noqa: E402
from run_sql import run_sql_file                                  # noqa: E402

log = logging.getLogger("silver_pipeline")


class PipelineError(Exception):
    """A pipeline step failed. `.step` is 'config', 'validate' or 'transform'; the original error is __cause__."""

    def __init__(self, message, step):
        super().__init__(message)
        self.step = step


def _abs(rel):
    """Config paths look like 'kestrel-data-foundation/sqls/...'; find them whatever the working dir is."""
    for base in (REPO_ROOT.parent, REPO_ROOT, Path.cwd()):
        if (base / rel).exists():
            return str(base / rel)
    return rel


def _load_config(config_path, table_names):
    path = Path(config_path)
    if not path.is_file():
        raise FileNotFoundError(f"config file not found: {path}")
    cfg = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    key = next((n for n in table_names if n in cfg), None)
    if key is None:
        raise ValueError(f"no entry for {table_names} in {path} (root keys: {list(cfg)})")
    for layer in (cfg[key].get("schemas") or {}).values():
        for k in ("ddl_path", "sql_transformation"):
            if (layer or {}).get(k):
                layer[k] = _abs(layer[k])
    return cfg, cfg[key]


def _sql_file(entry, target_schema, target_table):
    layer = (entry.get("schemas") or {}).get(target_schema)
    if layer is None:
        raise ValueError(f"schema '{target_schema}' not found in config 'schemas' (found: {list(entry.get('schemas') or {})})")
    folder = layer.get("sql_transformation")
    if not folder:
        raise ValueError(f"config schema '{target_schema}' has no 'sql_transformation' folder")
    path = Path(folder) / f"{target_table}.sql"
    if not path.is_file():
        raise FileNotFoundError(f"transformation SQL not found: {path}")
    return path


def run_silver_pipeline(source_schema, source_table, target_schema, target_table, config_path, db_path=None):
    db_path = db_path or REPO_ROOT / "warehouse" / "kestrel.duckdb"
    label = f"{source_schema}.{source_table} -> {target_schema}.{target_table}"
    started = time.perf_counter()
    step, con = "config", None
    try:
        cfg, entry = _load_config(config_path, [target_table, source_table])
        sql_file = _sql_file(entry, target_schema, target_table)
        con = duckdb.connect(str(db_path))

        step = "validate"
        log.info("step 1/2: validate %s", label)
        validate_tables(con, source_schema, source_table, target_schema, target_table, cfg)   # raises on any problem

        step = "transform"
        log.info("step 2/2: run %s", sql_file.name)
        results = run_sql_file(con, sql_file)
        counts = []
        for r in results:
            count = r["rows"][0][0] if r["columns"] == ["Count"] and r["rows"] else None
            counts.append(count)
            log.info("  statement %d: %.3fs%s", r["statement"], r["seconds"], "" if count is None else f", rows={count:,}")
        target_rows = con.execute(f'SELECT count(*) FROM "{target_schema}"."{target_table}"').fetchone()[0]
    except (ValidationError, RuntimeError, FileNotFoundError, ValueError, duckdb.Error) as e:
        raise PipelineError(f"{step} failed for {label}: {type(e).__name__}: {e}", step) from e
    finally:
        if con is not None:
            con.close()

    done = [c for c in counts if c is not None]
    stats = {"source": f"{source_schema}.{source_table}", "target": f"{target_schema}.{target_table}",
             "seconds": round(time.perf_counter() - started, 3),
             "rows_staged": done[0] if done else None,                    # first Count = stage statement
             "rows_merged": done[-1] if len(done) > 1 else None,          # last Count  = merge statement
             "target_rows": target_rows, "statements": results}
    log.info("done in %.1fs | staged=%s merged=%s target_rows=%s",
             stats["seconds"], stats["rows_staged"], stats["rows_merged"], f"{target_rows:,}")
    return stats
