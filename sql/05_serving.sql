-- Serving layer: final model, risk categories, cleanest windows, spike log.

-- 1) Final model trained on ALL clean data (ARIMA_PLUS was best at 24h and 48h)
CREATE OR REPLACE MODEL airsafe.arima_final
OPTIONS (
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'ts',
  time_series_data_col = 'pm25',
  data_frequency = 'HOURLY',
  auto_arima = TRUE
) AS
SELECT ts, pm25 FROM airsafe.pm25_clean;

-- 2) Guideline table: this is the ONLY source Gemini may use for advice (grounding).
-- Category bands follow the CPCB National AQI PM2.5 breakpoints (ug/m3).
-- Verify wording against the CPCB AQI document and cite it in your report.
CREATE OR REPLACE TABLE airsafe.advisory_guidelines AS
SELECT * FROM UNNEST([
  STRUCT(1 AS level, 'Good' AS category, 0.0 AS min_pm25, 30.0 AS max_pm25,
    'Minimal health impact.' AS health_impact,
    'Normal outdoor activity is fine.' AS general_advice,
    'No special precautions needed.' AS sensitive_advice),
  STRUCT(2, 'Satisfactory', 30.0, 60.0,
    'Minor breathing discomfort for sensitive people.',
    'Outdoor activity is fine for most people.',
    'People with asthma or heart disease should watch for symptoms and keep medication handy.'),
  STRUCT(3, 'Moderate', 60.0, 90.0,
    'Breathing discomfort for people with lung or heart disease, children and older adults.',
    'Reduce long or intense outdoor exercise.',
    'Children, older adults and people with asthma or heart disease should limit time outdoors and carry medication.'),
  STRUCT(4, 'Poor', 90.0, 120.0,
    'Breathing discomfort for most people on prolonged exposure.',
    'Avoid prolonged outdoor exertion; prefer cleaner hours of the day.',
    'Sensitive groups should stay indoors as much as possible and use a well-fitted N95-type mask outside.'),
  STRUCT(5, 'Very Poor', 120.0, 250.0,
    'Respiratory illness likely on prolonged exposure.',
    'Avoid outdoor exercise; keep windows closed during peak hours.',
    'Sensitive groups should stay indoors and follow their medical action plan.'),
  STRUCT(6, 'Severe', 250.0, 100000.0,
    'Affects healthy people and seriously affects those with existing disease.',
    'Stay indoors; avoid all outdoor exertion.',
    'Sensitive groups should stay indoors and seek medical advice if symptoms appear.')
]);

-- 3) 48-hour forecast with risk category (negative forecasts clipped to 0)
CREATE OR REPLACE TABLE airsafe.forecast_latest AS
SELECT
  f.forecast_timestamp AS ts,
  GREATEST(f.forecast_value, 0) AS pm25_pred,
  GREATEST(f.prediction_interval_lower_bound, 0) AS pm25_lo,
  f.prediction_interval_upper_bound AS pm25_hi,
  g.level,
  g.category,
  g.general_advice,
  g.sensitive_advice
FROM ML.FORECAST(MODEL airsafe.arima_final, STRUCT(48 AS horizon, 0.9 AS confidence_level)) AS f
JOIN airsafe.advisory_guidelines g
  ON GREATEST(f.forecast_value, 0) >= g.min_pm25
 AND GREATEST(f.forecast_value, 0) <  g.max_pm25;

-- 4) Cleanest 3-hour windows in the next 24h (daytime starts 05:00-19:00 only)
CREATE OR REPLACE VIEW airsafe.best_windows AS
WITH w AS (
  SELECT
    ts AS window_start,
    TIMESTAMP_ADD(ts, INTERVAL 3 HOUR) AS window_end,
    AVG(pm25_pred) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS avg_pm25,
    COUNT(*) OVER (ORDER BY ts ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS n
  FROM airsafe.forecast_latest
  WHERE ts < TIMESTAMP_ADD((SELECT MIN(ts) FROM airsafe.forecast_latest), INTERVAL 27 HOUR)
)
SELECT window_start, window_end, ROUND(avg_pm25, 1) AS avg_pm25,
       RANK() OVER (ORDER BY avg_pm25) AS clean_rank
FROM w
WHERE n = 3
  AND EXTRACT(HOUR FROM window_start) BETWEEN 5 AND 19
  AND window_start < TIMESTAMP_ADD((SELECT MIN(ts) FROM airsafe.forecast_latest), INTERVAL 24 HOUR);

-- 5) Spike log: historical hours the model flags as abnormal
CREATE OR REPLACE TABLE airsafe.anomaly_history AS
SELECT ts, pm25, lower_bound, upper_bound, anomaly_probability
FROM ML.DETECT_ANOMALIES(MODEL airsafe.arima_final, STRUCT(0.95 AS anomaly_prob_threshold))
WHERE is_anomaly;

-- Quick check
SELECT ts, ROUND(pm25_pred, 1) AS pm25_pred, category FROM airsafe.forecast_latest ORDER BY ts LIMIT 12;