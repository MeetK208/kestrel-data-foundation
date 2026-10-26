-- ---------------------------------------------------------------------
-- ref_fiscal_calendar: bronze CSV -> silver, full reload (all or nothing)
-- No defects. is_weekend becomes a boolean, everything else is copied as is
-- ---------------------------------------------------------------------

BEGIN;

DELETE FROM silver_layer.ref_fiscal_calendar;

INSERT INTO silver_layer.ref_fiscal_calendar (
  calendar_date,
  fiscal_year,
  fiscal_quarter,
  fiscal_month_no,
  iso_week,
  day_of_week,
  is_weekend,
  _bronze_loaded_at,
  _silver_loaded_at
)
SELECT
  b.calendar_date,
  b.fiscal_year,
  b.fiscal_quarter,
  b.fiscal_month_no,
  b.iso_week,
  b.day_of_week,
  CAST(b.is_weekend AS BOOLEAN) AS is_weekend,
  b._bronze_loaded_at,
  CAST(current_timestamp AS TIMESTAMP) AS _silver_loaded_at
FROM bronze_layer.ref_fiscal_calendar b;

COMMIT;