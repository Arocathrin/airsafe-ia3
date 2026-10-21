-- ETL: clean view + chronological train/test split
-- ts is IST wall-clock time (stored as TIMESTAMP, treated as local hour).

CREATE OR REPLACE VIEW airsafe.pm25_clean AS
SELECT
  ts_ist AS ts,
  pm25,
  temp_c,
  rh_pct,
  wind_kmh,
  pressure_hpa,
  precip_mm
FROM airsafe.history_raw_12235796
WHERE pm25 IS NOT NULL   -- drop missing / stuck hours
  AND pm25 > 0           -- 0.0 readings are treated as sensor faults
  AND temp_c IS NOT NULL;

-- Training: everything before 7 Oct 2026. Test: the last ~7 days (chronological holdout).
CREATE OR REPLACE VIEW airsafe.pm25_train AS
SELECT * FROM airsafe.pm25_clean
WHERE ts < TIMESTAMP '2026-10-07 00:00:00';

CREATE OR REPLACE VIEW airsafe.pm25_test AS
SELECT * FROM airsafe.pm25_clean
WHERE ts >= TIMESTAMP '2026-10-07 00:00:00';

SELECT
  (SELECT COUNT(*) FROM airsafe.pm25_clean) AS clean_rows,
  (SELECT COUNT(*) FROM airsafe.pm25_train) AS train_rows,
  (SELECT COUNT(*) FROM airsafe.pm25_test)  AS test_rows,
  (SELECT MAX(ts) FROM airsafe.pm25_train)  AS train_ends,
  (SELECT MIN(ts) FROM airsafe.pm25_test)   AS test_starts,
  (SELECT MAX(ts) FROM airsafe.pm25_test)   AS test_ends;