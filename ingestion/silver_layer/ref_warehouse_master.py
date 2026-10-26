import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ingestion.utility.general_pipeline import run_silver_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    force=True,          # makes sure it takes effect even if something configured logging earlier
)

stats = run_silver_pipeline(
    source_schema="bronze_layer",
    source_table="ref_warehouse_master",
    target_schema="silver_layer",
    target_table="ref_warehouse_master",
    config_path=str(REPO_ROOT / "config.yaml"),
    db_path=str(REPO_ROOT / "warehouse" / "kestrel.duckdb"),
)
print(stats)