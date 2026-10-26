-- ---------------------------------------------------------------------
-- erp_outlet_master: bronze CDC -> silver current state (incremental, MERGE)
-- Key       : outlet_code, one row per outlet
-- Increment : outlets with a bronze record loaded within 1 day of the newest _bronze_loaded_at in silver
--             Their state is rebuilt from ALL their bronze records, so a record that arrives
--             late (L12) can never overwrite a newer one
-- Order     : (__op_ts, __seq). __seq breaks ties on the same __op_ts (L13)
-- Deleted   : kept in silver with is_deleted = true when the newest record is a delete (D)
--             The attributes of a deleted outlet come from its last non-delete record
--             To make a delete final instead, use  BOOL_OR(__op = 'D') OVER (PARTITION BY outlet_code)
-- ---------------------------------------------------------------------

-- 1. STAGE: rebuild the current state of every key touched by new bronze records
CREATE OR REPLACE TEMP TABLE outlet_stage
AS
WITH
  watermark AS (
    SELECT
      COALESCE(
        MAX(_bronze_loaded_at) - INTERVAL '1 day',
        TIMESTAMP '1900-01-01') AS loaded_after
    FROM silver_layer.erp_outlet_master
  ),
  touched AS (
    SELECT DISTINCT b.outlet_code
    FROM bronze_layer.erp_outlet_master b, watermark w
    WHERE b._bronze_loaded_at > w.loaded_after
  ),
  history AS (  -- every change record of the touched keys, not only the new ones
    SELECT
      b.*,
      try_strptime(b.__op_ts, '%Y-%m-%dT%H:%M:%SZ') AS op_ts,
      MAX(b._bronze_loaded_at) OVER (PARTITION BY b.outlet_code) AS max_loaded_at
    FROM bronze_layer.erp_outlet_master b
    WHERE b.outlet_code IN (SELECT outlet_code FROM touched)
  ),
  ranked AS (  -- order by (__op_ts, __seq), never by file arrival
    SELECT
      h.*,
      ROW_NUMBER() OVER (PARTITION BY outlet_code ORDER BY op_ts DESC, __seq DESC) AS rn_any,
      ROW_NUMBER() OVER (PARTITION BY outlet_code ORDER BY (__op = 'D'), op_ts DESC, __seq DESC) AS rn_live
    FROM history h
  )
SELECT
  l.outlet_code,
  l.outlet_name,
  l.channel,
  l.outlet_format,
  l.city,
  l.route_code,
  l.warehouse_code,
  l.credit_limit,
  l.credit_terms_days,
  l.gst_number,
  l.status,
  a.__op = 'D' AS is_deleted,
  a.op_ts AS last_op_ts,
  a.__seq AS last_seq,
  a.max_loaded_at AS _bronze_loaded_at,
  CAST(current_timestamp AS TIMESTAMP) AS _silver_loaded_at
FROM ranked a
JOIN ranked l
  ON l.outlet_code = a.outlet_code
  AND a.rn_any = 1  -- newest record of any kind
  AND l.rn_live = 1;  -- newest record that is not a delete: the attributes come from here

-- 2. MERGE: explicit column lists. The stage is rebuilt from full history, so it is always authoritative
--    New silver column? add it to the stage SELECT above AND to both lists below
MERGE INTO silver_layer.erp_outlet_master AS t
USING outlet_stage AS s
ON  t.outlet_code = s.outlet_code
WHEN MATCHED THEN
  UPDATE SET
    outlet_name = s.outlet_name,
    channel = s.channel,
    outlet_format = s.outlet_format,
    city = s.city,
    route_code = s.route_code,
    warehouse_code = s.warehouse_code,
    credit_limit = s.credit_limit,
    credit_terms_days = s.credit_terms_days,
    gst_number = s.gst_number,
    status = s.status,
    is_deleted = s.is_deleted,
    last_op_ts = s.last_op_ts,
    last_seq = s.last_seq,
    _bronze_loaded_at = s._bronze_loaded_at,
    _silver_loaded_at = s._silver_loaded_at
WHEN NOT MATCHED THEN
  INSERT (
    outlet_code,
    outlet_name,
    channel,
    outlet_format,
    city,
    route_code,
    warehouse_code,
    credit_limit,
    credit_terms_days,
    gst_number,
    status,
    is_deleted,
    last_op_ts,
    last_seq,
    _bronze_loaded_at,
    _silver_loaded_at
  )
  VALUES (
    s.outlet_code,
    s.outlet_name,
    s.channel,
    s.outlet_format,
    s.city,
    s.route_code,
    s.warehouse_code,
    s.credit_limit,
    s.credit_terms_days,
    s.gst_number,
    s.status,
    s.is_deleted,
    s.last_op_ts,
    s.last_seq,
    s._bronze_loaded_at,
    s._silver_loaded_at
  );