-- ---------------------------------------------------------------------
-- erp_product_master: bronze CDC -> silver current state (incremental, MERGE)
-- Key       : sku_code, one row per SKU
-- Increment : SKUs with a bronze record loaded within 1 day of the newest _bronze_loaded_at in silver
--             Their state is rebuilt from ALL their bronze records, so a record that arrives
--             late (L12) can never overwrite a newer one
-- Order     : (__op_ts, __seq). __seq breaks ties on the same __op_ts (L13)
-- Deleted   : kept in silver with is_deleted = true when the newest record is a delete (D)
--             The attributes of a deleted SKU come from its last non-delete record
--             To make a delete final instead, use  BOOL_OR(__op = 'D') OVER (PARTITION BY sku_code)
-- ---------------------------------------------------------------------

-- 1. STAGE: rebuild the current state of every key touched by new bronze records
CREATE OR REPLACE TEMP TABLE product_stage
AS
WITH
  watermark AS (
    SELECT
      COALESCE(
        MAX(_bronze_loaded_at) - INTERVAL '1 day',
        TIMESTAMP '1900-01-01') AS loaded_after
    FROM silver_layer.erp_product_master
  ),
  touched AS (
    SELECT DISTINCT b.sku_code
    FROM bronze_layer.erp_product_master b, watermark w
    WHERE b._bronze_loaded_at > w.loaded_after
  ),
  history AS (  -- every change record of the touched keys, not only the new ones
    SELECT
      b.*,
      try_strptime(b.__op_ts, '%Y-%m-%dT%H:%M:%SZ') AS op_ts,
      MAX(b._bronze_loaded_at) OVER (PARTITION BY b.sku_code) AS max_loaded_at
    FROM bronze_layer.erp_product_master b
    WHERE b.sku_code IN (SELECT sku_code FROM touched)
  ),
  ranked AS (  -- order by (__op_ts, __seq), never by file arrival
    SELECT
      h.*,
      ROW_NUMBER() OVER (PARTITION BY sku_code ORDER BY op_ts DESC, __seq DESC) AS rn_any,
      ROW_NUMBER() OVER (PARTITION BY sku_code ORDER BY (__op = 'D'), op_ts DESC, __seq DESC) AS rn_live
    FROM history h
  )
SELECT
  l.sku_code,
  l.product_name,
  l.category,
  l.brand,
  l.case_pack,
  l.mrp,
  l.list_price,
  l.gst_rate_pct,
  l.shelf_life_days,
  CAST(l.is_chilled AS BOOLEAN) AS is_chilled,
  l.status,
  a.__op = 'D' AS is_deleted,
  a.op_ts AS last_op_ts,
  a.__seq AS last_seq,
  a.max_loaded_at AS _bronze_loaded_at,
  CAST(current_timestamp AS TIMESTAMP) AS _silver_loaded_at
FROM ranked a
JOIN ranked l
  ON l.sku_code = a.sku_code
  AND a.rn_any = 1  -- newest record of any kind
  AND l.rn_live = 1;  -- newest record that is not a delete: the attributes come from here

-- 2. MERGE: explicit column lists. The stage is rebuilt from full history, so it is always authoritative
--    New silver column? add it to the stage SELECT above AND to both lists below
MERGE INTO silver_layer.erp_product_master AS t
USING product_stage AS s
ON  t.sku_code = s.sku_code
WHEN MATCHED THEN
  UPDATE SET
    product_name = s.product_name,
    category = s.category,
    brand = s.brand,
    case_pack = s.case_pack,
    mrp = s.mrp,
    list_price = s.list_price,
    gst_rate_pct = s.gst_rate_pct,
    shelf_life_days = s.shelf_life_days,
    is_chilled = s.is_chilled,
    status = s.status,
    is_deleted = s.is_deleted,
    last_op_ts = s.last_op_ts,
    last_seq = s.last_seq,
    _bronze_loaded_at = s._bronze_loaded_at,
    _silver_loaded_at = s._silver_loaded_at
WHEN NOT MATCHED THEN
  INSERT (
    sku_code,
    product_name,
    category,
    brand,
    case_pack,
    mrp,
    list_price,
    gst_rate_pct,
    shelf_life_days,
    is_chilled,
    status,
    is_deleted,
    last_op_ts,
    last_seq,
    _bronze_loaded_at,
    _silver_loaded_at
  )
  VALUES (
    s.sku_code,
    s.product_name,
    s.category,
    s.brand,
    s.case_pack,
    s.mrp,
    s.list_price,
    s.gst_rate_pct,
    s.shelf_life_days,
    s.is_chilled,
    s.status,
    s.is_deleted,
    s.last_op_ts,
    s.last_seq,
    s._bronze_loaded_at,
    s._silver_loaded_at
  );