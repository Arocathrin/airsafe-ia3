-- MAE / RMSE for each model at 24h, 48h and 168h horizons (chronological holdout)
SELECT
  h AS horizon_hours,
  COUNT(*) AS n_hours,
  ROUND(AVG(ABS(actual - pred_persistence)), 2) AS mae_persistence,
  ROUND(AVG(ABS(actual - pred_profile)), 2)     AS mae_daily_profile,
  ROUND(AVG(ABS(actual - pred_arima)), 2)       AS mae_arima_plus,
  ROUND(AVG(ABS(actual - pred_xreg)), 2)        AS mae_arima_xreg,
  ROUND(SQRT(AVG(POW(actual - pred_persistence, 2))), 2) AS rmse_persistence,
  ROUND(SQRT(AVG(POW(actual - pred_profile, 2))), 2)     AS rmse_daily_profile,
  ROUND(SQRT(AVG(POW(actual - pred_arima, 2))), 2)       AS rmse_arima_plus,
  ROUND(SQRT(AVG(POW(actual - pred_xreg, 2))), 2)        AS rmse_arima_xreg
FROM airsafe.forecast_eval, UNNEST([24, 48, 168]) AS h
WHERE lead_h <= h
GROUP BY h
ORDER BY h;