-- Dashboard tables/views for Looker Studio

-- 1) Metrics in long format (one row per model x horizon)
CREATE OR REPLACE TABLE airsafe.model_metrics AS
WITH long AS (
  SELECT e.lead_h, e.actual, m.model, m.pred
  FROM airsafe.forecast_eval e,
  UNNEST([
    STRUCT('Persistence' AS model, e.pred_persistence AS pred),
    STRUCT('Daily profile', e.pred_profile),
    STRUCT('ARIMA_PLUS', e.pred_arima),
    STRUCT('ARIMA_PLUS_XREG', e.pred_xreg)
  ]) AS m
)
SELECT
  h AS horizon_hours,
  model,
  COUNT(*) AS n_hours,
  ROUND(AVG(ABS(actual - pred)), 2) AS mae,
  ROUND(SQRT(AVG(POW(actual - pred, 2))), 2) AS rmse
FROM long, UNNEST([24, 48, 168]) AS h
WHERE lead_h <= h
GROUP BY horizon_hours, model;

-- 2) History with risk category for each hour
CREATE OR REPLACE VIEW airsafe.v_history AS
SELECT c.ts, c.pm25, c.temp_c, c.rh_pct, c.wind_kmh, g.category, g.level
FROM airsafe.pm25_clean c
JOIN airsafe.advisory_guidelines g
  ON c.pm25 >= g.min_pm25 AND c.pm25 < g.max_pm25;

-- 3) Data freshness and quality (single row, drives the pipeline health panel)
CREATE OR REPLACE VIEW airsafe.v_pipeline_health AS
SELECT
  (SELECT MAX(ts) FROM airsafe.pm25_clean) AS last_observation,
  (SELECT COUNT(*) FROM airsafe.history_raw_12235796) AS hours_in_window,
  (SELECT COUNT(*) FROM airsafe.pm25_clean) AS usable_hours,
  ROUND(100 * (1 - (SELECT COUNT(*) FROM airsafe.pm25_clean) /
        (SELECT COUNT(*) FROM airsafe.history_raw_12235796)), 1) AS removed_pct,
  (SELECT COUNTIF(pm25_flag = 'stuck') FROM airsafe.history_raw_12235796) AS stuck_hours,
  (SELECT COUNTIF(pm25_flag = 'outlier') FROM airsafe.history_raw_12235796) AS outlier_hours;

-- 4) Daily average PM2.5 for trend charts
CREATE OR REPLACE VIEW airsafe.v_daily AS
SELECT DATE(ts) AS day, ROUND(AVG(pm25), 1) AS avg_pm25, ROUND(MAX(pm25), 1) AS max_pm25
FROM airsafe.pm25_clean
GROUP BY day;

SELECT * FROM airsafe.model_metrics ORDER BY horizon_hours, mae;