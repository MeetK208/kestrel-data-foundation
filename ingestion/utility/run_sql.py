"""Run a .sql file on DuckDB and return what DuckDB returned for each statement.

    results = run_sql_file(con, "my_query.sql")
    # [{"statement": 1, "seconds": 0.01, "columns": ["Count"], "rows": [(50,)]}, ...]
"""
import time
from pathlib import Path

import duckdb


def run_sql_file(con, sql_file):
    sql = Path(sql_file).read_text(encoding="utf-8")
    try:
        statements = [s.query for s in duckdb.extract_statements(sql)]
    except AttributeError:          # very old DuckDB: run the whole file, only the last result comes back
        statements = [sql]

    results = []
    for n, stmt in enumerate(statements, start=1):
        t = time.perf_counter()
        try:
            cur = con.execute(stmt)
            desc = cur.description
            rows = cur.fetchall() if desc else []
        except Exception as e:
            raise RuntimeError(f"statement {n} failed: {e}") from e
        results.append({"statement": n,
                        "seconds": round(time.perf_counter() - t, 4),
                        "columns": [d[0] for d in desc] if desc else [],
                        "rows": rows})
    return results