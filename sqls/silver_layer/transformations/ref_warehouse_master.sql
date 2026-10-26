-- ---------------------------------------------------------------------
-- ref_warehouse_master: bronze CSV -> silver, full reload (all or nothing)
-- No defects. Copied as is, the lineage columns are slimmed down
-- ---------------------------------------------------------------------

BEGIN;

DELETE FROM silver_layer.ref_warehouse_master;

INSERT INTO silver_layer.ref_warehouse_master (
  warehouse_code,
  warehouse_name,
  city,
  region_name,
  timezone,
  chilled_capacity_pallets,
  _bronze_loaded_at,
  _silver_loaded_at
)
SELECT
  b.warehouse_code,
  b.warehouse_name,
  b.city,
  b.region_name,
  b.timezone,
  b.chilled_capacity_pallets,
  b._bronze_loaded_at,
  CAST(current_timestamp AS TIMESTAMP) AS _silver_loaded_at
FROM bronze_layer.ref_warehouse_master b;

COMMIT;