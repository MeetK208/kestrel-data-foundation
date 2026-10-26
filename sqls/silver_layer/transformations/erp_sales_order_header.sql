-- ---------------------------------------------------------------------
-- erp_sales_order_header: bronze CDC -> silver current state (incremental, MERGE)
-- Key       : order_number, one row per order
-- Increment : orders with a bronze record loaded within 1 day of the newest _bronze_loaded_at in silver
--             Their state is rebuilt from ALL their bronze records
-- Order     : (__op_ts, __seq) for the attributes
-- Fixes     : L14 PARTNER_API freight removed from order_value_gross, L15 deleted orders flagged is_deleted
-- Deleted   : a delete (D) is final. The D record copies the insert's __op_ts, so by time every update
--             looks newer than the delete and a plain latest-wins would bring deleted orders back
-- ---------------------------------------------------------------------

-- 1. STAGE: rebuild the current state of every key touched by new bronze records
CREATE OR REPLACE TEMP TABLE order_stage
AS
WITH
  watermark AS (
    SELECT
      COALESCE(
        MAX(_bronze_loaded_at) - INTERVAL '1 day',
        TIMESTAMP '1900-01-01') AS loaded_after
    FROM silver_layer.erp_sales_order_header
  ),
  touched AS (
    SELECT DISTINCT b.order_number
    FROM bronze_layer.erp_sales_order_header b, watermark w
    WHERE b._bronze_loaded_at > w.loaded_after
  ),
  history AS (  -- every change record of the touched keys, not only the new ones
    SELECT
      b.*,
      try_strptime(b.__op_ts, '%Y-%m-%dT%H:%M:%SZ') AS op_ts,
      MAX(b._bronze_loaded_at) OVER (PARTITION BY b.order_number) AS max_loaded_at,
      BOOL_OR(b.__op = 'D') OVER (PARTITION BY b.order_number) AS any_delete
    FROM bronze_layer.erp_sales_order_header b
    WHERE b.order_number IN (SELECT order_number FROM touched)
  ),
  ranked AS (  -- order by (__op_ts, __seq), never by file arrival
    SELECT
      h.*,
      ROW_NUMBER() OVER (PARTITION BY order_number ORDER BY op_ts DESC, __seq DESC) AS rn_any,
      ROW_NUMBER() OVER (PARTITION BY order_number ORDER BY (__op = 'D'), op_ts DESC, __seq DESC) AS rn_live
    FROM history h
  )
SELECT
  l.order_number,
  l.outlet_code,
  l.warehouse_code,
  l.route_code,
  TRY_CAST(l.order_date AS DATE) AS order_date,
  TRY_CAST(l.requested_delivery_date AS DATE) AS requested_delivery_date,
  l.order_status,
  l.line_count,
  -- L14: PARTNER_API added freight (x1.085) into the gross value, take it back out
  ROUND(
    CASE
      WHEN l.source_system IS NOT DISTINCT FROM 'PARTNER_API' THEN l.order_value_gross / 1.085
      ELSE l.order_value_gross
      END,
    2) AS order_value_gross,
  l.source_system IS NOT DISTINCT FROM 'PARTNER_API' AS freight_corrected,
  l.discount_amount,
  l.tax_amount,
  l.source_system,
  -- L15: a delete is final, the delete record keeps the insert's __op_ts so it never sorts last
  a.any_delete AS is_deleted,
  a.op_ts AS last_op_ts,
  a.__seq AS last_seq,
  a.max_loaded_at AS _bronze_loaded_at,
  CAST(current_timestamp AS TIMESTAMP) AS _silver_loaded_at
FROM ranked a
JOIN ranked l
  ON l.order_number = a.order_number
  AND a.rn_any = 1  -- newest record of any kind
  AND l.rn_live = 1;  -- newest record that is not a delete: the attributes come from here

-- 2. MERGE: explicit column lists. The stage is rebuilt from full history, so it is always authoritative
--    New silver column? add it to the stage SELECT above AND to both lists below
MERGE INTO silver_layer.erp_sales_order_header AS t
USING order_stage AS s
ON  t.order_number = s.order_number
WHEN MATCHED THEN
  UPDATE SET
    outlet_code = s.outlet_code,
    warehouse_code = s.warehouse_code,
    route_code = s.route_code,
    order_date = s.order_date,
    requested_delivery_date = s.requested_delivery_date,
    order_status = s.order_status,
    line_count = s.line_count,
    order_value_gross = s.order_value_gross,
    freight_corrected = s.freight_corrected,
    discount_amount = s.discount_amount,
    tax_amount = s.tax_amount,
    source_system = s.source_system,
    is_deleted = s.is_deleted,
    last_op_ts = s.last_op_ts,
    last_seq = s.last_seq,
    _bronze_loaded_at = s._bronze_loaded_at,
    _silver_loaded_at = s._silver_loaded_at
WHEN NOT MATCHED THEN
  INSERT (
    order_number,
    outlet_code,
    warehouse_code,
    route_code,
    order_date,
    requested_delivery_date,
    order_status,
    line_count,
    order_value_gross,
    freight_corrected,
    discount_amount,
    tax_amount,
    source_system,
    is_deleted,
    last_op_ts,
    last_seq,
    _bronze_loaded_at,
    _silver_loaded_at
  )
  VALUES (
    s.order_number,
    s.outlet_code,
    s.warehouse_code,
    s.route_code,
    s.order_date,
    s.requested_delivery_date,
    s.order_status,
    s.line_count,
    s.order_value_gross,
    s.freight_corrected,
    s.discount_amount,
    s.tax_amount,
    s.source_system,
    s.is_deleted,
    s.last_op_ts,
    s.last_seq,
    s._bronze_loaded_at,
    s._silver_loaded_at
  );