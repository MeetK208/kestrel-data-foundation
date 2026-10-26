-- ---------------------------------------------------------------------
-- ref_legacy_finance_weekly: bronze CSV -> silver, full reload (all or nothing)
-- L17: this report groups by ingest date and does not remove duplicates, so it is NOT the truth
--      It is copied exactly as published and only used to explain differences to silver numbers
-- ---------------------------------------------------------------------

BEGIN;

DELETE FROM silver_layer.ref_legacy_finance_weekly;

INSERT INTO silver_layer.ref_legacy_finance_weekly (
  week_ending,
  channel,
  gross_sales_inr,
  units_sold,
  basket_count,
  prepared_by,
  note,
  _bronze_loaded_at,
  _silver_loaded_at
)
SELECT
  b.week_ending,
  b.channel,
  b.gross_sales_inr,
  b.units_sold,
  b.basket_count,
  b.prepared_by,
  b.note,
  b._bronze_loaded_at,
  CAST(current_timestamp AS TIMESTAMP) AS _silver_loaded_at
FROM bronze_layer.ref_legacy_finance_weekly b;

COMMIT;