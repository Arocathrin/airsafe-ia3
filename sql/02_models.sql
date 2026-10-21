-- BigQuery ML models (trained only on the training view)

CREATE OR REPLACE MODEL airsafe.arima_plus
OPTIONS (
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'ts',
  time_series_data_col = 'pm25',
  data_frequency = 'HOURLY',
  auto_arima = TRUE
) AS
SELECT ts, pm25
FROM airsafe.pm25_train;

CREATE OR REPLACE MODEL airsafe.arima_plus_xreg
OPTIONS (
  model_type = 'ARIMA_PLUS_XREG',
  time_series_timestamp_col = 'ts',
  time_series_data_col = 'pm25'
) AS
SELECT ts, pm25, temp_c, rh_pct, wind_kmh, pressure_hpa, precip_mm
FROM airsafe.pm25_train;