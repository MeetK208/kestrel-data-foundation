-- ---------------------------------------------------------------------
-- erp_outlet_master_history: bronze CDC -> silver, one row per version of an outlet
-- Answers : "what was this outlet like on that date?" (KP-3155)
-- Version : valid_from = when the change happened, valid_to = when the next change happened
--           The newest version has valid_to empty (is_current = true)
-- Order   : (__op_ts, __seq), never file arrival, so late records land in the right place (L12)
--           Two records on the same __op_ts: the higher __seq wins, the other one never applied and is dropped (L13)
-- Deletes : a delete starts a version with is_deleted = true, keeping the last real attributes
-- Load    : outlets with a bronze record loaded within 1 day of the newest _bronze_loaded_at in silver
--           All their versions are rebuilt from full history, then replaced (delete + insert in one transaction)
-- Times   : valid_from / valid_to are __op_ts as delivered (UTC). Compare them with event_ts_utc, not IST dates
-- ---------------------------------------------------------------------

-- 1. STAGE: rebuild every version of the touched outlets
CREATE OR REPLACE TEMP TABLE outlet_history_stage
AS
WITH
  watermark AS (
    SELECT
      COALESCE(
        MAX(_bronze_loaded_at) - INTERVAL '1 day',
        TIMESTAMP '1900-01-01') AS loaded_after
    FROM silver_layer.erp_outlet_master_history
  ),
  touched AS (
    SELECT DISTINCT b.outlet_code
    FROM bronze_layer.erp_outlet_master b, watermark w
    WHERE b._bronze_loaded_at > w.loaded_after
  ),
  history AS (  -- every change record of the touched outlets, not only the new ones
    SELECT
      b.*,
      try_strptime(b.__op_ts, '%Y-%m-%dT%H:%M:%SZ') AS op_ts,
      MAX(b._bronze_loaded_at) OVER (PARTITION BY b.outlet_code) AS max_loaded_at
    FROM bronze_layer.erp_outlet_master b
    WHERE b.outlet_code IN (SELECT outlet_code FROM touched)
  ),
  ordered AS (  -- number the records in real-time order. A record with an unreadable time cannot be placed, so it is skipped
    SELECT
      h.*,
      ROW_NUMBER() OVER (PARTITION BY outlet_code ORDER BY op_ts, __seq) AS rn
    FROM history h
    WHERE op_ts IS NOT NULL
  ),
  governed AS (
    SELECT
      o.*,
      -- number of the newest non-delete record up to this one: where the attributes come from
      MAX(CASE WHEN __op <> 'D' THEN rn END) OVER (PARTITION BY outlet_code ORDER BY rn) AS live_rn,
      -- the next record's time ends this version
      LEAD(op_ts) OVER (PARTITION BY outlet_code ORDER BY rn) AS next_op_ts
    FROM ordered o
  ),
  versions AS (
    SELECT
      g.outlet_code,
      g.op_ts AS valid_from,
      g.next_op_ts AS valid_to,
      g.__op = 'D' AS is_deleted,
      g.__seq AS source_seq,
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
      g.max_loaded_at
    FROM governed g
    LEFT JOIN ordered l
      ON l.outlet_code = g.outlet_code
      AND l.rn = g.live_rn
    WHERE g.next_op_ts IS NULL OR g.next_op_ts > g.op_ts  -- drop versions replaced in the same instant (ties)
  )
SELECT
  v.outlet_code,
  ROW_NUMBER() OVER (PARTITION BY v.outlet_code ORDER BY v.valid_from, v.source_seq) AS version_no,
  v.valid_from,
  v.valid_to,
  v.valid_to IS NULL AS is_current,
  v.is_deleted,
  v.outlet_name,
  v.channel,
  v.outlet_format,
  v.city,
  v.route_code,
  v.warehouse_code,
  v.credit_limit,
  v.credit_terms_days,
  v.gst_number,
  v.status,
  v.source_seq,
  v.max_loaded_at AS _bronze_loaded_at,
  CAST(current_timestamp AS TIMESTAMP) AS _silver_loaded_at
FROM versions v;

-- 2. REPLACE: swap in the rebuilt versions, all or nothing
--    New silver column? add it to the stage SELECT above AND to the INSERT list below
BEGIN;

DELETE FROM silver_layer.erp_outlet_master_history
WHERE outlet_code IN (SELECT outlet_code FROM outlet_history_stage);

INSERT INTO silver_layer.erp_outlet_master_history (
  outlet_code,
  version_no,
  valid_from,
  valid_to,
  is_current,
  is_deleted,
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
  source_seq,
  _bronze_loaded_at,
  _silver_loaded_at
)
SELECT
  outlet_code,
  version_no,
  valid_from,
  valid_to,
  is_current,
  is_deleted,
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
  source_seq,
  _bronze_loaded_at,
  _silver_loaded_at
FROM outlet_history_stage;

COMMIT;