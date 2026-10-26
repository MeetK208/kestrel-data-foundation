#!/usr/bin/env python3
"""
Table validator (DuckDB): run before a bronze -> silver transformation, for any table.

    from validate_tables import validate_tables
    validate_tables(con, "bronze_layer", "pos_transactions",
                    "silver_layer", "pos_transactions", "tables_config.yaml")

Steps
  1. Load the config entry (root key = silver table name, fallback bronze table name).
     The passed schema names must exist under that entry's `schemas` block.
  2. Bronze schema + table must exist, otherwise BronzeTableMissingError.
  3. Silver table missing -> create the schema, then the table from
     <ddl_path>/<silver_table>.sql  (falls back to the YAML silver_columns if that file is missing).
  4. Compare the LIVE bronze table with `bronze_columns` and the LIVE silver table with
     `silver_columns` (column name + data type). ignore_columns are skipped; extra live columns
     are tolerated when allow_extra_columns is true.
  5. All good -> return True. Any difference -> SchemaMismatchError listing every issue.

Needs: pip install pyyaml   (the connection you pass in is a DuckDB connection)
"""
from __future__ import annotations

import logging
from pathlib import Path

import yaml

log = logging.getLogger("table_validator")

_TYPE_ALIASES = {
    "INT": "INTEGER", "INT4": "INTEGER", "SIGNED": "INTEGER",
    "INT8": "BIGINT", "LONG": "BIGINT",
    "INT2": "SMALLINT", "SHORT": "SMALLINT", "INT1": "TINYINT",
    "STRING": "VARCHAR", "TEXT": "VARCHAR", "CHAR": "VARCHAR", "BPCHAR": "VARCHAR",
    "FLOAT8": "DOUBLE", "FLOAT4": "FLOAT", "REAL": "FLOAT",
    "BOOL": "BOOLEAN", "DATETIME": "TIMESTAMP",
}


# --------------------------------------------------------------------------- errors
class ValidationError(Exception):
    """Base class: catch this to handle every validation failure."""


class ConfigError(ValidationError):
    """Config entry, a file it points to, or the DDL is missing or invalid."""


class BronzeTableMissingError(ValidationError):
    """Bronze schema or table does not exist."""


class SchemaMismatchError(ValidationError):
    """A live table does not match the config. Details in .mismatches."""

    def __init__(self, message, mismatches):
        super().__init__(message)
        self.mismatches = mismatches


# --------------------------------------------------------------------------- db helpers
def _qi(name):
    return '"' + str(name).replace('"', '""') + '"'


def _norm_type(t):
    t = str(t).strip().upper()
    return _TYPE_ALIASES.get(t, t)


def _schema_exists(con, schema):
    return con.execute("SELECT count(*) FROM information_schema.schemata WHERE schema_name = ?",
                       [schema]).fetchone()[0] > 0


def _table_exists(con, schema, table):
    return con.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                       [schema, table]).fetchone()[0] > 0


def _live_columns(con, schema, table):
    rows = con.execute("SELECT column_name, data_type FROM information_schema.columns "
                       "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
                       [schema, table]).fetchall()
    return {n.lower(): _norm_type(t) for n, t in rows}


# --------------------------------------------------------------------------- config helpers
def _load_config(config, table_names):
    """Return (entry, config_dir). `config` is a YAML path or an already-loaded dict."""
    cfg_dir = Path.cwd()
    if isinstance(config, (str, Path)):
        path = Path(config)
        if not path.is_file():
            raise ConfigError(f"config file not found: {path}")
        cfg_dir = path.resolve().parent
        with open(path, encoding="utf-8") as fh:
            config = yaml.safe_load(fh) or {}
    if not isinstance(config, dict):
        raise ConfigError("config must be a YAML file path or a dict")
    for name in table_names:
        if name in config:
            return config[name], cfg_dir
    raise ConfigError(f"no entry for {table_names} in config (root keys: {list(config)})")


def _layer(entry, schema, cols_key):
    schemas = (entry or {}).get("schemas") or {}
    if schema not in schemas:
        raise ConfigError(f"schema '{schema}' not found in config 'schemas' (found: {list(schemas)})")
    layer = schemas[schema] or {}
    cols = layer.get(cols_key)
    if not isinstance(cols, dict) or not cols:
        raise ConfigError(f"config schema '{schema}' needs a non-empty '{cols_key}' mapping of column: TYPE")
    return layer


def _locate(rel_folder, filename, bases):
    """Find <rel_folder>/<filename>; relative paths are tried from the working dir, then the config dir."""
    folder = Path(rel_folder)
    candidates = [folder] if folder.is_absolute() else [b / folder for b in bases]
    for c in candidates:
        if (c / filename).is_file():
            return c / filename
    return None


# --------------------------------------------------------------------------- comparison
def _compare(label, schema, table, expected, live, ignore, allow_extra):
    """Pure function: returns a list of issue strings (empty list = match)."""
    exp = {k.lower(): _norm_type(v) for k, v in expected.items() if k.lower() not in ignore}
    act = {k: v for k, v in live.items() if k not in ignore}
    where = f"{schema}.{table}"
    issues = []
    for c, t in exp.items():
        if c not in act:
            issues.append(f"[{label}] MISSING_COLUMN '{c}' ({t}) is in the config but not in {where}")
        elif act[c] != t:
            issues.append(f"[{label}] TYPE_MISMATCH '{c}': config={t} {where}={act[c]}")
    if not allow_extra:
        for c, t in act.items():
            if c not in exp:
                issues.append(f"[{label}] EXTRA_COLUMN '{c}' ({t}) is in {where} but not in the config")
    return issues


# --------------------------------------------------------------------------- silver creation
def _create_silver(con, schema, table, layer, bases):
    ddl_file = _locate(layer.get("ddl_path", ""), f"{table}.sql", bases) if layer.get("ddl_path") else None
    if ddl_file:
        ddl = ddl_file.read_text(encoding="utf-8").replace("{silver_schema}", schema).replace("{silver_table}", table)
        source = str(ddl_file)
    else:
        cols = ", ".join(f"{_qi(c)} {t}" for c, t in layer["silver_columns"].items())
        ddl = f"CREATE TABLE IF NOT EXISTS {_qi(schema)}.{_qi(table)} ({cols})"
        source = "config silver_columns (no DDL file found)"
        log.warning("DDL file %s/%s.sql not found, creating silver from config columns",
                    layer.get("ddl_path"), table)
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {_qi(schema)}")
        con.execute(ddl)
    except Exception as e:
        raise ConfigError(f"could not create {schema}.{table} from {source}: {e}") from e
    if not _table_exists(con, schema, table):
        raise ConfigError(f"{source} ran but did not create {schema}.{table}; check the names inside the DDL")
    log.info("created silver table %s.%s from %s", schema, table, source)


# --------------------------------------------------------------------------- main function
def validate_tables(con, bronze_schema, bronze_table, silver_schema, silver_table, config):
    """
    Returns True if bronze exists and both live tables match the config.
    Raises ConfigError / BronzeTableMissingError / SchemaMismatchError otherwise.
    """
    entry, cfg_dir = _load_config(config, [silver_table, bronze_table])
    bases = [Path.cwd(), cfg_dir]
    b_layer = _layer(entry, bronze_schema, "bronze_columns")
    s_layer = _layer(entry, silver_schema, "silver_columns")

    # 1. bronze must exist
    if not _schema_exists(con, bronze_schema):
        raise BronzeTableMissingError(f"bronze schema '{bronze_schema}' does not exist")
    if not _table_exists(con, bronze_schema, bronze_table):
        raise BronzeTableMissingError(f"bronze table {bronze_schema}.{bronze_table} does not exist")
    log.info("bronze table %s.%s found", bronze_schema, bronze_table)

    # 2. silver: create schema + table if missing
    if _table_exists(con, silver_schema, silver_table):
        log.info("silver table %s.%s found", silver_schema, silver_table)
    else:
        log.warning("silver table %s.%s missing", silver_schema, silver_table)
        _create_silver(con, silver_schema, silver_table, s_layer, bases)

    # 3. transformation file is informational only
    if s_layer.get("sql_transformation") and not _locate(s_layer["sql_transformation"], f"{silver_table}.sql", bases):
        log.warning("no transformation file %s/%s.sql found (not checked further)",
                    s_layer["sql_transformation"], silver_table)

    # 4. compare live schemas with the config
    issues = []
    for label, schema, table, layer, key in (
            ("bronze", bronze_schema, bronze_table, b_layer, "bronze_columns"),
            ("silver", silver_schema, silver_table, s_layer, "silver_columns")):
        ignore = {c.lower() for c in (layer.get("ignore_columns") or [])}
        issues += _compare(label, schema, table, layer[key], _live_columns(con, schema, table),
                           ignore, bool(layer.get("allow_extra_columns", False)))

    if issues:
        for i in issues:
            log.error(i)
        raise SchemaMismatchError(
            f"{len(issues)} schema issue(s) for {bronze_table}/{silver_table}:\n  " + "\n  ".join(issues),
            issues)

    log.info("OK: %s.%s and %s.%s match the config", bronze_schema, bronze_table, silver_schema, silver_table)
    return True


# --------------------------------------------------------------------------- CLI
if __name__ == "__main__":
    import argparse
    import sys

    import duckdb

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="warehouse/kestrel.duckdb")
    ap.add_argument("--config", default="tables_config.yaml")
    ap.add_argument("--bronze-schema", required=True)
    ap.add_argument("--bronze-table", required=True)
    ap.add_argument("--silver-schema", required=True)
    ap.add_argument("--silver-table", required=True)
    a = ap.parse_args()

    connection = duckdb.connect(a.db)
    try:
        print("OK" if validate_tables(connection, a.bronze_schema, a.bronze_table,
                                      a.silver_schema, a.silver_table, a.config) else "FAILED")
    except ValidationError as err:
        print(f"{type(err).__name__}: {err}")
        sys.exit(1)
    finally:
        connection.close()