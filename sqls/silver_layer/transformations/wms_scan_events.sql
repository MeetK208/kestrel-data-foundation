-- ---------------------------------------------------------------------
-- wms_scan_events: bronze -> silver (incremental, MERGE)
-- Key       : scan_id (a real unique id, so no key collisions to worry about)
-- Increment : bronze rows loaded within 1 day of the newest _bronze_loaded_at already in silver
--             (the overlap is harmless: MERGE skips rows it already has)
-- Fixes     : event_ts text -> TIMESTAMP (already local IST, no conversion), stage_seq for ordering stages
-- Not fixable here: L11 (about 6.5% of scan events were never emitted, those rows do not exist)
-- ---------------------------------------------------------------------

-- 1. STAGE: new bronze rows, cleaned, one row per key
CREATE OR REPLACE TEMP TABLE wms_stage
AS
WITH
  watermark AS (
    SELECT
      COALESCE(
        MAX(_bronze_loaded_at) - INTERVAL '1 day',
        TIMESTAMP '1900-01-01') AS loaded_after
    FROM silver_layer.wms_scan_events
  ),
  new_rows AS (
    SELECT
      b.scan_id,
      b.warehouse_code,
      b.event_type,
      -- position of the scan in the warehouse flow, so cycle times can be stitched in order
      CAST(
        CASE b.event_type
          WHEN 'RECEIVE' THEN 1
          WHEN 'PUTAWAY' THEN 2
          WHEN 'PICK' THEN 3
          WHEN 'PACK' THEN 4
          WHEN 'STAGE' THEN 5
          WHEN 'DISPATCH' THEN 6
          END AS SMALLINT) AS stage_seq,
      b.order_number,
      b.sku_code,
      b.batch_id,
      b.qty_cases,
      b.pallet_id,
      b.dock_door,
      b.operator_id,
      b.handheld_device,
      try_strptime(b.event_ts, '%Y-%m-%d %H:%M:%S') AS event_ts,
      b.dt,
      b._bronze_loaded_at
    FROM bronze_layer.wms_scan_events b, watermark w
    WHERE b._bronze_loaded_at > w.loaded_after
  ),
  dedup AS (  -- MERGE needs exactly one source row per key
    SELECT *
    FROM new_rows
    QUALIFY
      ROW_NUMBER()
        OVER (
          PARTITION BY scan_id
          ORDER BY _bronze_loaded_at DESC
        )
      = 1
  )
SELECT
  d.scan_id,
  d.warehouse_code,
  d.event_type,
  d.stage_seq,
  d.order_number,
  d.sku_code,
  d.batch_id,
  d.qty_cases,
  d.pallet_id,
  d.dock_door,
  d.operator_id,
  d.handheld_device,
  d.event_ts,
  CAST(d.event_ts AS DATE) AS business_date,
  d.dt,
  d._bronze_loaded_at,
  CAST(current_timestamp AS TIMESTAMP) AS _silver_loaded_at
FROM dedup d;

-- 2. MERGE: explicit column lists. Key column is not updated
--    New silver column? add it to the stage SELECT above AND to both lists below
MERGE INTO silver_layer.wms_scan_events AS t
USING wms_stage AS s
ON  t.scan_id = s.scan_id
WHEN MATCHED AND s._bronze_loaded_at > t._bronze_loaded_at THEN  -- bronze reloaded a corrected file
  UPDATE SET
    warehouse_code = s.warehouse_code,
    event_type = s.event_type,
    stage_seq = s.stage_seq,
    order_number = s.order_number,
    sku_code = s.sku_code,
    batch_id = s.batch_id,
    qty_cases = s.qty_cases,
    pallet_id = s.pallet_id,
    dock_door = s.dock_door,
    operator_id = s.operator_id,
    handheld_device = s.handheld_device,
    event_ts = s.event_ts,
    business_date = s.business_date,
    dt = s.dt,
    _bronze_loaded_at = s._bronze_loaded_at,
    _silver_loaded_at = s._silver_loaded_at
WHEN NOT MATCHED THEN  -- new scan events only
  INSERT (
    scan_id,
    warehouse_code,
    event_type,
    stage_seq,
    order_number,
    sku_code,
    batch_id,
    qty_cases,
    pallet_id,
    dock_door,
    operator_id,
    handheld_device,
    event_ts,
    business_date,
    dt,
    _bronze_loaded_at,
    _silver_loaded_at
  )
  VALUES (
    s.scan_id,
    s.warehouse_code,
    s.event_type,
    s.stage_seq,
    s.order_number,
    s.sku_code,
    s.batch_id,
    s.qty_cases,
    s.pallet_id,
    s.dock_door,
    s.operator_id,
    s.handheld_device,
    s.event_ts,
    s.business_date,
    s.dt,
    s._bronze_loaded_at,
    s._silver_loaded_at
  );