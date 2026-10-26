-- ---------------------------------------------------------------------
-- ref_carrier_master: bronze CSV -> silver, full reload (all or nothing)
-- No defects. Copied as is, the lineage columns are slimmed down
-- ---------------------------------------------------------------------

BEGIN;

DELETE FROM silver_layer.ref_carrier_master;

INSERT INTO silver_layer.ref_carrier_master (
  carrier_id,
  carrier_name,
  mode,
  sla_hours,
  rate_per_km,
  _bronze_loaded_at,
  _silver_loaded_at
)
SELECT
  b.carrier_id,
  b.carrier_name,
  b.mode,
  b.sla_hours,
  b.rate_per_km,
  b._bronze_loaded_at,
  CAST(current_timestamp AS TIMESTAMP) AS _silver_loaded_at
FROM bronze_layer.ref_carrier_master b;

COMMIT;