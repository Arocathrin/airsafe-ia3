-- Tables/views used by the automated pipeline (run ONCE before deploying it)

CREATE TABLE IF NOT EXISTS airsafe.forecast_log (
  run_ts TIMESTAMP,
  data_asof TIMESTAMP,
  forecast_for TIMESTAMP,
  pm25_pred FLOAT64,
  pm25_lo FLOAT64,
  pm25_hi FLOAT64,
  category STRING
);

CREATE TABLE IF NOT EXISTS airsafe.pipeline_runs (
  run_ts TIMESTAMP,
  api_rows INT64,
  hourly_rows INT64,
  last_obs TIMESTAMP,
  retrained BOOL,
  status STRING
);

-- Deployment prerequisite: airsafe.pm25_estimates stores numerical PM2.5 estimates
-- from Open-Meteo Air Quality API (distinct from measured ground station data).
-- Run this script once before deploying the automated pipeline.
-- In addition, pipeline/main.py defensively issues CREATE TABLE IF NOT EXISTS
-- before running MERGE_ESTIMATES_SQL to prevent missing table errors.
CREATE TABLE IF NOT EXISTS airsafe.pm25_estimates (
  ts TIMESTAMP,
  pm25_estimate FLOAT64,
  fetched_at TIMESTAMP,
  source STRING,
  data_kind STRING,
  latitude FLOAT64,
  longitude FLOAT64
);

-- Seed the log with the forecast we already have (only if the log is empty)
INSERT INTO airsafe.forecast_log
SELECT CURRENT_TIMESTAMP(), (SELECT MAX(ts) FROM airsafe.pm25_clean),
       ts, pm25_pred, pm25_lo, pm25_hi, category
FROM airsafe.forecast_latest
WHERE NOT EXISTS (SELECT 1 FROM airsafe.forecast_log);

-- Forecast accuracy tracker: past forecasts vs what actually happened
CREATE OR REPLACE VIEW airsafe.v_accuracy AS
SELECT
  l.data_asof,
  l.forecast_for,
  TIMESTAMP_DIFF(l.forecast_for, l.data_asof, HOUR) AS lead_h,
  l.pm25_pred,
  c.pm25 AS actual,
  ABS(l.pm25_pred - c.pm25) AS abs_error,
  c.pm25 BETWEEN l.pm25_lo AND l.pm25_hi AS inside_interval
FROM airsafe.forecast_log l
JOIN airsafe.pm25_clean c ON c.ts = l.forecast_for;

CREATE OR REPLACE VIEW airsafe.v_accuracy_summary AS
SELECT
  IF(lead_h <= 24, '1-24h ahead', '25-48h ahead') AS horizon,
  COUNT(*) AS n,
  ROUND(AVG(abs_error), 2) AS mae,
  ROUND(100 * AVG(CAST(inside_interval AS INT64)), 1) AS interval_coverage_pct
FROM airsafe.v_accuracy
GROUP BY horizon;