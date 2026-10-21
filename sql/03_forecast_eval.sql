-- 7-day forecasts (168 hours) from the end of training, with 90% intervals.

CREATE OR REPLACE TABLE airsafe.fc_arima AS
SELECT
  forecast_timestamp AS ts,
  forecast_value AS pred,
  prediction_interval_lower_bound AS lo,
  prediction_interval_upper_bound AS hi
FROM ML.FORECAST(MODEL airsafe.arima_plus, STRUCT(168 AS horizon, 0.9 AS confidence_level));

-- XREG needs the weather for each future hour. Here we use observed weather for the
-- test window (in production the live pipeline uses Open-Meteo's forecast instead).
CREATE OR REPLACE TABLE airsafe.fc_xreg AS
SELECT
  forecast_timestamp AS ts,
  forecast_value AS pred,
  prediction_interval_lower_bound AS lo,
  prediction_interval_upper_bound AS hi
FROM ML.FORECAST(
  MODEL airsafe.arima_plus_xreg,
  STRUCT(168 AS horizon, 0.9 AS confidence_level),
  (
    SELECT ts_ist AS ts, temp_c, rh_pct, wind_kmh, pressure_hpa, precip_mm
    FROM airsafe.history_raw_12235796
    WHERE ts_ist > (SELECT MAX(ts) FROM airsafe.pm25_train)
    ORDER BY ts_ist
    LIMIT 168
  )
);

-- Combine actuals, two baselines, and both models.
CREATE OR REPLACE TABLE airsafe.forecast_eval AS
WITH last_train AS (
  SELECT MAX(ts) AS t FROM airsafe.pm25_train
),
persist AS (
  -- Baseline 1: repeat the last observed value
  SELECT pm25 AS v FROM airsafe.pm25_train WHERE ts = (SELECT t FROM last_train)
),
profile AS (
  -- Baseline 2: average PM2.5 for each hour of day over the last 7 training days
  SELECT EXTRACT(HOUR FROM ts) AS h, AVG(pm25) AS v
  FROM airsafe.pm25_train
  WHERE ts > TIMESTAMP_SUB((SELECT t FROM last_train), INTERVAL 7 DAY)
  GROUP BY h
)
SELECT
  a.ts,
  TIMESTAMP_DIFF(a.ts, (SELECT t FROM last_train), HOUR) AS lead_h,
  a.pm25 AS actual,
  (SELECT v FROM persist) AS pred_persistence,
  p.v AS pred_profile,
  f1.pred AS pred_arima,
  f2.pred AS pred_xreg,
  f1.lo AS arima_lo, f1.hi AS arima_hi,
  f2.lo AS xreg_lo, f2.hi AS xreg_hi
FROM airsafe.pm25_test a
JOIN profile p ON EXTRACT(HOUR FROM a.ts) = p.h
JOIN airsafe.fc_arima f1 ON f1.ts = a.ts
JOIN airsafe.fc_xreg f2 ON f2.ts = a.ts;