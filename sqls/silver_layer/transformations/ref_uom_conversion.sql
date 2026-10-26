-- ---------------------------------------------------------------------
-- ref_uom_conversion: bronze CSV -> silver, full reload (all or nothing)
-- Fixes     : L16 the CSV is missing about 4.2% of SKUs. Those SKUs take eaches_per_case from the
--             product master case_pack, and conversion_source says which source each row came from
-- Run order : silver erp_product_master must be loaded first
-- ---------------------------------------------------------------------

BEGIN;

DELETE FROM silver_layer.ref_uom_conversion;

INSERT INTO silver_layer.ref_uom_conversion (
  sku_code,
  eaches_per_case,
  base_uom,
  case_uom,
  conversion_source,
  _bronze_loaded_at,
  _silver_loaded_at
)
SELECT
  c.sku_code,
  c.eaches_per_case,
  c.base_uom,
  c.case_uom,
  'UOM_REF' AS conversion_source,
  c._bronze_loaded_at,
  CAST(current_timestamp AS TIMESTAMP) AS _silver_loaded_at
FROM bronze_layer.ref_uom_conversion c
UNION ALL
SELECT
  p.sku_code,
  p.case_pack,
  'EA',  -- the CSV uses EA / CS for every SKU
  'CS',
  'PRODUCT_MASTER',
  p._bronze_loaded_at,
  CAST(current_timestamp AS TIMESTAMP)
FROM silver_layer.erp_product_master p
WHERE NOT EXISTS (
  SELECT 1
  FROM bronze_layer.ref_uom_conversion c
  WHERE c.sku_code = p.sku_code
);

COMMIT;