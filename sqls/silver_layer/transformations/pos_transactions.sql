-- ---------------------------------------------------------------------
-- 1. STAGE: last 4 days from bronze, cleaned, one row per key
-- ---------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE pos_stage
AS
WITH
  watermark AS (
    SELECT
      COALESCE(max(event_ts_ist), TIMESTAMP '1900-01-01')
      - INTERVAL 330 MINUTE
      - INTERVAL 4 DAY AS from_utc
    FROM silver_layer.pos_transactions
  ),
  new_rows AS (
    SELECT
      b.* EXCLUDE(qty, quantity_units, uom, event_ts),
      TRY_CAST(b.event_ts AS TIMESTAMP) AS event_ts_utc,  -- 'Z' string -> UTC
      TRY_CAST(b.event_ts AS TIMESTAMP) + INTERVAL 330 MINUTE
        AS event_ts_ist,  -- IST has no DST
      COALESCE(b.quantity_units, b.qty) AS qty,  -- drift: v2 or v1 column
      COALESCE(b.uom, 'EA') AS uom,  -- v1 files had no uom
      b.uom IS NULL AS uom_inferred
    FROM bronze_layer.pos_transactions b, watermark w
    WHERE TRY_CAST(b.event_ts AS TIMESTAMP) > w.from_utc
  ),
  dedup AS (  -- MERGE needs exactly one source row per key
    SELECT *
    FROM new_rows
    QUALIFY
      ROW_NUMBER()
        OVER (
          PARTITION BY txn_id, txn_line_no
          ORDER BY event_ts_utc DESC
        )
      = 1
  ),
  uom_lookup AS (
    SELECT sku_code, eaches_per_case, 'UOM_REF' AS conversion_source
    FROM bronze_layer.ref_uom_conversion
    UNION ALL
    SELECT p.sku_code, p.case_pack, 'PRODUCT_MASTER'
    FROM
      (
        SELECT sku_code, case_pack
        FROM bronze_layer.erp_product_master
        QUALIFY
          ROW_NUMBER()
            OVER (PARTITION BY sku_code ORDER BY __op_ts DESC, __seq DESC)
          = 1
      ) p
    WHERE
      p.sku_code NOT IN (SELECT sku_code FROM bronze_layer.ref_uom_conversion)
  )
SELECT
  d.txn_id,
  d.txn_line_no,
  d.basket_id,
  d.outlet_code,
  d.channel,
  d.sku_code,
  d.event_ts_utc,
  d.event_ts_ist,
  d.qty,
  d.uom,
  d.uom_inferred,
  u.eaches_per_case,
  u.conversion_source,
  CASE
    WHEN d.uom = 'CS' THEN d.qty * u.eaches_per_case
    ELSE d.qty
    END AS qty_eaches,
  d.unit_price,
  d.discount_amount,
  d.tax_amount,
  ROUND(d.qty * d.unit_price, 2) AS line_gross_amount,
  ROUND(d.qty * d.unit_price - d.discount_amount + d.tax_amount, 2)
    AS line_net_amount,
  d.payment_mode,
  d.till_id,
  d.cashier_id,
  d.promo_code,
  d.loyalty_id,
  d.source_file,
  d.ingest_date,
  date_diff('day', CAST(d.event_ts_ist AS DATE), d.ingest_date)
    AS ingest_lag_days,
  d._bronze_loaded_at,
  CAST(current_timestamp AS TIMESTAMP) AS _silver_loaded_at
FROM dedup d
LEFT JOIN uom_lookup u
  ON u.sku_code = d.sku_code;

-- ---------------------------------------------------------------------
-- 2. MERGE: explicit column lists, so column order / extra columns never matter.
--    Key = txn_id + txn_line_no + event_ts_utc (not updated).
--    New silver column? add it to the stage SELECT above AND to both lists below.
-- ---------------------------------------------------------------------
MERGE INTO silver_layer.pos_transactions AS t
USING pos_stage AS s
ON  t.txn_id       = s.txn_id
AND t.txn_line_no  = s.txn_line_no
AND t.event_ts_utc = s.event_ts_utc
WHEN MATCHED AND s._bronze_loaded_at > t._bronze_loaded_at THEN  -- bronze reloaded a corrected file
  UPDATE SET
    basket_id = s.basket_id,
    outlet_code = s.outlet_code,
    channel = s.channel,
    sku_code = s.sku_code,
    event_ts_ist = s.event_ts_ist,
    qty = s.qty,
    uom = s.uom,
    uom_inferred = s.uom_inferred,
    eaches_per_case = s.eaches_per_case,
    conversion_source = s.conversion_source,
    qty_eaches = s.qty_eaches,
    unit_price = s.unit_price,
    discount_amount = s.discount_amount,
    tax_amount = s.tax_amount,
    line_gross_amount = s.line_gross_amount,
    line_net_amount = s.line_net_amount,
    payment_mode = s.payment_mode,
    till_id = s.till_id,
    cashier_id = s.cashier_id,
    promo_code = s.promo_code,
    loyalty_id = s.loyalty_id,
    source_file = s.source_file,
    ingest_date = s.ingest_date,
    ingest_lag_days = s.ingest_lag_days,
    _bronze_loaded_at = s._bronze_loaded_at,
    _silver_loaded_at = s._silver_loaded_at
WHEN NOT MATCHED THEN  -- late / new rows only
  INSERT (
    txn_id,
    txn_line_no,
    basket_id,
    outlet_code,
    channel,
    sku_code,
    event_ts_utc,
    event_ts_ist,
    qty,
    uom,
    uom_inferred,
    eaches_per_case,
    conversion_source,
    qty_eaches,
    unit_price,
    discount_amount,
    tax_amount,
    line_gross_amount,
    line_net_amount,
    payment_mode,
    till_id,
    cashier_id,
    promo_code,
    loyalty_id,
    source_file,
    ingest_date,
    ingest_lag_days,
    _bronze_loaded_at,
    _silver_loaded_at
  )
  VALUES (
    s.txn_id,
    s.txn_line_no,
    s.basket_id,
    s.outlet_code,
    s.channel,
    s.sku_code,
    s.event_ts_utc,
    s.event_ts_ist,
    s.qty,
    s.uom,
    s.uom_inferred,
    s.eaches_per_case,
    s.conversion_source,
    s.qty_eaches,
    s.unit_price,
    s.discount_amount,
    s.tax_amount,
    s.line_gross_amount,
    s.line_net_amount,
    s.payment_mode,
    s.till_id,
    s.cashier_id,
    s.promo_code,
    s.loyalty_id,
    s.source_file,
    s.ingest_date,
    s.ingest_lag_days,
    s._bronze_loaded_at,
    s._silver_loaded_at
  );