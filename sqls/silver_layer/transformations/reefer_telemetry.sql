-- ---------------------------------------------------------------------
-- reefer_telemetry: bronze -> silver (incremental, MERGE)
-- Key       : device_id, route_code, warehouse_code, gateway_id + reading_ts 
-- Increment : bronze rows loaded within 1 day of the newest _bronze_loaded_at already in silver
--             (the overlap is harmless: MERGE skips rows it already has)
-- Fixes     : L5 device clock +7h, L6 Fahrenheit, L7 missing unit, L8 dropouts stay NULL, L9 duplicates
-- Not fixable here: L10 (gateway outage = rows that never existed), L18 (quarantined file in bronze)
-- ---------------------------------------------------------------------

-- 1. STAGE: new bronze rows, cleaned, one row per key
CREATE OR REPLACE TEMP TABLE telemetry_stage
AS
WITH
  watermark AS (
    SELECT
      COALESCE(
        MAX(_bronze_loaded_at) - INTERVAL '1 day',
        TIMESTAMP '1900-01-01') AS loaded_after
    FROM silver_layer.reefer_telemetry
  ),
  new_rows AS (
    SELECT
      b.device_id,
      b.telemetry_vendor,
      b.firmware_version,
      b.vehicle_registration,
      b.route_code,
      b.warehouse_code,
      b.gateway_id,
      -- L5: firmware 2.1.4 devices run 7 hours ahead, subtract 7h to get the real time
      try_strptime(b.reading_ts, '%Y-%m-%dT%H:%M:%SZ')
        - CASE
          WHEN b.firmware_version IS NOT DISTINCT FROM '2.1.4'
            THEN INTERVAL 7 HOUR
          ELSE INTERVAL 0 HOUR
          END AS reading_ts,
      b.firmware_version IS NOT DISTINCT FROM '2.1.4' AS clock_corrected,
      b.dt,
      b.temp_value,
      -- L7: unit missing on ~8% of rows, infer from vendor (COLDEYE reports Fahrenheit)
      COALESCE(
        b.temp_unit,
        CASE WHEN b.telemetry_vendor = 'COLDEYE' THEN 'F' ELSE 'C' END)
        AS temp_unit,
      b.temp_unit IS NULL AS temp_unit_inferred,
      b.humidity_pct,
      b.door_open_flag,
      b.gps_lat,
      b.gps_lon,
      b.battery_pct,
      b._bronze_loaded_at
    FROM bronze_layer.reefer_telemetry b, watermark w
    WHERE b._bronze_loaded_at > w.loaded_after
  ),
  dedup AS (  -- L9: exact duplicates collapse here. MERGE needs exactly one source row per key
    SELECT *
    FROM new_rows
    WHERE reading_ts IS NOT NULL
    QUALIFY
      ROW_NUMBER()
        OVER (
          PARTITION BY device_id, route_code, warehouse_code, gateway_id, reading_ts
          ORDER BY _bronze_loaded_at DESC
        )
      = 1
  )
SELECT
  d.device_id,
  d.telemetry_vendor,
  d.firmware_version,
  d.vehicle_registration,
  d.route_code,
  d.warehouse_code,
  d.gateway_id,
  d.reading_ts,
  CAST(d.reading_ts AS DATE) AS business_date,
  d.dt,
  d.clock_corrected,
  -- L6 + L8: always Celsius. A sensor dropout stays NULL, it is never filled in
  ROUND(
    CASE
      WHEN isnan(d.temp_value) THEN NULL
      WHEN d.temp_unit = 'F' THEN (d.temp_value - 32) * 5.0 / 9.0
      ELSE d.temp_value
      END,
    2) AS temp_value,
  d.temp_unit,
  d.temp_unit_inferred,
  d.humidity_pct,
  d.door_open_flag,
  d.gps_lat,
  d.gps_lon,
  d.battery_pct,
  d._bronze_loaded_at,
  CAST(current_timestamp AS TIMESTAMP) AS _silver_loaded_at
FROM dedup d;

-- 2. MERGE: explicit column lists. Key columns are not updated
--    New silver column? add it to the stage SELECT above AND to both lists below
MERGE INTO silver_layer.reefer_telemetry AS t
USING telemetry_stage AS s
ON  t.device_id  = s.device_id
AND t.route_code = s.route_code
AND t.warehouse_code = s.warehouse_code
AND t.gateway_id = s.gateway_id
AND t.reading_ts = s.reading_ts
WHEN MATCHED AND s._bronze_loaded_at > t._bronze_loaded_at THEN  -- bronze reloaded a corrected file
  UPDATE SET
    telemetry_vendor = s.telemetry_vendor,
    firmware_version = s.firmware_version,
    vehicle_registration = s.vehicle_registration,
    route_code = s.route_code,
    warehouse_code = s.warehouse_code,
    gateway_id = s.gateway_id,
    business_date = s.business_date,
    dt = s.dt,
    clock_corrected = s.clock_corrected,
    temp_value = s.temp_value,
    temp_unit = s.temp_unit,
    temp_unit_inferred = s.temp_unit_inferred,
    humidity_pct = s.humidity_pct,
    door_open_flag = s.door_open_flag,
    gps_lat = s.gps_lat,
    gps_lon = s.gps_lon,
    battery_pct = s.battery_pct,
    _bronze_loaded_at = s._bronze_loaded_at,
    _silver_loaded_at = s._silver_loaded_at
WHEN NOT MATCHED THEN  -- new readings only
  INSERT (
    device_id,
    telemetry_vendor,
    firmware_version,
    vehicle_registration,
    route_code,
    warehouse_code,
    gateway_id,
    reading_ts,
    business_date,
    dt,
    clock_corrected,
    temp_value,
    temp_unit,
    temp_unit_inferred,
    humidity_pct,
    door_open_flag,
    gps_lat,
    gps_lon,
    battery_pct,
    _bronze_loaded_at,
    _silver_loaded_at
  )
  VALUES (
    s.device_id,
    s.telemetry_vendor,
    s.firmware_version,
    s.vehicle_registration,
    s.route_code,
    s.warehouse_code,
    s.gateway_id,
    s.reading_ts,
    s.business_date,
    s.dt,
    s.clock_corrected,
    s.temp_value,
    s.temp_unit,
    s.temp_unit_inferred,
    s.humidity_pct,
    s.door_open_flag,
    s.gps_lat,
    s.gps_lon,
    s.battery_pct,
    s._bronze_loaded_at,
    s._silver_loaded_at
  );