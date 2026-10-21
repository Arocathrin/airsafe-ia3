"""AirSafe pipeline (Cloud Function, HTTP trigger).

OpenAQ + Open-Meteo -> raw JSON in GCS -> clean/validate -> BigQuery MERGE
-> retrain + forecast ONLY when new observations arrived -> log the run.

Env vars: OPENAQ_API_KEY, SENSOR_ID, LAT, LON, BUCKET, (optional) HOURS_BACK
Local dry run (fetch + clean only, no cloud writes):  set DRY_RUN=1 & python main.py
Force a retrain on a deployed function: add ?force=1 to the URL.
"""
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import requests

OPENAQ = "https://api.openaq.org/v3"
METEO = "https://api.open-meteo.com/v1/forecast"
IST = timezone(timedelta(hours=5, minutes=30))
HOURLY_VARS = "temperature_2m,relative_humidity_2m,wind_speed_10m,surface_pressure,precipitation"

# ---------------------------------------------------------------- SQL
MERGE_SQL = """
MERGE airsafe.history_raw_12235796 T
USING airsafe.stg_hourly S
ON T.ts_ist = S.ts_ist
WHEN MATCHED AND T.pm25 IS NULL
     AND IFNULL(T.pm25_flag, '') NOT IN ('stuck', 'outlier', 'negative')
     AND S.pm25 IS NOT NULL THEN
  UPDATE SET pm25 = S.pm25, pm25_flag = S.pm25_flag
WHEN NOT MATCHED THEN
  INSERT (ts_ist, pm25, temp_c, rh_pct, wind_kmh, pressure_hpa, precip_mm, pm25_flag)
  VALUES (S.ts_ist, S.pm25, S.temp_c, CAST(ROUND(S.rh_pct) AS INT64),
          S.wind_kmh, S.pressure_hpa, S.precip_mm, S.pm25_flag)
"""

RETRAIN_SQL = """
CREATE OR REPLACE MODEL airsafe.arima_final
OPTIONS (model_type = 'ARIMA_PLUS', time_series_timestamp_col = 'ts',
         time_series_data_col = 'pm25', data_frequency = 'HOURLY', auto_arima = TRUE) AS
SELECT ts, pm25 FROM airsafe.pm25_clean
"""

FORECAST_SQL = """
CREATE OR REPLACE TABLE airsafe.forecast_latest AS
SELECT f.forecast_timestamp AS ts,
       GREATEST(f.forecast_value, 0) AS pm25_pred,
       GREATEST(f.prediction_interval_lower_bound, 0) AS pm25_lo,
       f.prediction_interval_upper_bound AS pm25_hi,
       g.level, g.category, g.general_advice, g.sensitive_advice
FROM ML.FORECAST(MODEL airsafe.arima_final, STRUCT(48 AS horizon, 0.9 AS confidence_level)) AS f
JOIN airsafe.advisory_guidelines g
  ON GREATEST(f.forecast_value, 0) >= g.min_pm25
 AND GREATEST(f.forecast_value, 0) <  g.max_pm25
"""

ANOMALY_SQL = """
CREATE OR REPLACE TABLE airsafe.anomaly_history AS
SELECT ts, pm25, lower_bound, upper_bound, anomaly_probability
FROM ML.DETECT_ANOMALIES(MODEL airsafe.arima_final, STRUCT(0.95 AS anomaly_prob_threshold))
WHERE is_anomaly
"""

LOG_FORECAST_SQL = """
INSERT INTO airsafe.forecast_log
SELECT CURRENT_TIMESTAMP(), (SELECT MAX(ts) FROM airsafe.pm25_clean),
       ts, pm25_pred, pm25_lo, pm25_hi, category
FROM airsafe.forecast_latest
"""


# ---------------------------------------------------------------- fetch
def fetch_pm25(sensor_id, hours_back):
    now = datetime.now(timezone.utc)
    r = requests.get(
        f"{OPENAQ}/sensors/{sensor_id}/hours",
        headers={"X-API-Key": os.environ["OPENAQ_API_KEY"]},
        params={
            "datetime_from": (now - timedelta(hours=hours_back)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "datetime_to": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "limit": 1000,
        },
        timeout=60,
    )
    r.raise_for_status()
    return r.json().get("results", [])


def fetch_weather(lat, lon):
    r = requests.get(
        METEO,
        params={"latitude": lat, "longitude": lon, "hourly": HOURLY_VARS,
                "past_days": 35, "forecast_days": 2, "timezone": "Asia/Kolkata"},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------- transform
def transform(pm_rows, weather):
    """Hourly IST grid up to the last PM2.5 hour, with validation flags."""
    by_hour = defaultdict(list)
    for x in pm_rows:
        if x.get("value") is None:
            continue
        end = datetime.fromisoformat(x["period"]["datetimeTo"]["local"]).astimezone(IST)
        by_hour[end.replace(minute=0, second=0, microsecond=0, tzinfo=None)].append(x["value"])
    if not by_hour:
        return []
    lo, hi = min(by_hour), max(by_hour)

    w = weather["hourly"]
    rows = []
    for i, t in enumerate(w["time"]):
        h = datetime.fromisoformat(t)
        if lo <= h <= hi:
            vals = by_hour.get(h)
            rows.append({
                "ts_ist": h.strftime("%Y-%m-%d %H:%M:%S"),
                "pm25": sum(vals) / len(vals) if vals else None,
                "temp_c": w["temperature_2m"][i],
                "rh_pct": w["relative_humidity_2m"][i],
                "wind_kmh": w["wind_speed_10m"][i],
                "pressure_hpa": w["surface_pressure"][i],
                "precip_mm": w["precipitation"][i],
                "pm25_flag": "",
            })

    def flush(run):  # 12+ identical hours in a row = stuck sensor
        if len(run) >= 12:
            for r in run:
                r["pm25"], r["pm25_flag"] = None, "stuck"

    run, prev = [], None
    for r in rows:
        v = r["pm25"]
        if v is not None and v < 0:
            r["pm25"], r["pm25_flag"], v = None, "negative", None
        elif v is not None and v > 1000:
            r["pm25"], r["pm25_flag"], v = None, "outlier", None
        if v is not None and v == prev:
            run.append(r)
        else:
            flush(run)
            run = [r] if v is not None else []
        prev = v
    flush(run)
    for r in rows:
        if r["pm25"] is None and not r["pm25_flag"]:
            r["pm25_flag"] = "missing"
    return rows


# ---------------------------------------------------------------- run
def run(dry=False, force=False):
    pm = fetch_pm25(os.environ["SENSOR_ID"], int(os.environ.get("HOURS_BACK", 720)))
    wx = fetch_weather(os.environ["LAT"], os.environ["LON"])
    rows = transform(pm, wx)
    usable = sum(r["pm25"] is not None for r in rows)
    summary = {"api_rows": len(pm), "hourly_rows": len(rows), "usable_hours": usable}
    if dry:
        print(summary)
        print("last 3 rows:", json.dumps(rows[-3:], indent=1))
        return summary

    from google.cloud import bigquery, storage

    now = datetime.now(timezone.utc)
    path = f"raw/{now:%Y/%m/%d}/{now:%H%M}.json"
    storage.Client().bucket(os.environ["BUCKET"]).blob(path).upload_from_string(
        json.dumps({"ingested_at": now.isoformat(), "sensor_id": os.environ["SENSOR_ID"],
                    "pm25": pm, "weather": wx}),
        content_type="application/json",
    )

    bq = bigquery.Client()

    def q(sql):
        return list(bq.query(sql, location="US").result())

    before = q("SELECT MAX(ts) AS m FROM airsafe.pm25_clean")[0].m
    if rows:
        schema = [bigquery.SchemaField("ts_ist", "TIMESTAMP")] + [
            bigquery.SchemaField(c, "FLOAT") for c in
            ("pm25", "temp_c", "rh_pct", "wind_kmh", "pressure_hpa", "precip_mm")
        ] + [bigquery.SchemaField("pm25_flag", "STRING")]
        bq.load_table_from_json(
            rows, "airsafe.stg_hourly", location="US",
            job_config=bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE"),
        ).result()
        q(MERGE_SQL)
    after = q("SELECT MAX(ts) AS m FROM airsafe.pm25_clean")[0].m

    retrained = force or after != before
    if retrained:
        for sql in (RETRAIN_SQL, FORECAST_SQL, ANOMALY_SQL, LOG_FORECAST_SQL):
            q(sql)

    last_obs = f"TIMESTAMP('{after}')" if after else "NULL"
    q(f"INSERT INTO airsafe.pipeline_runs VALUES (CURRENT_TIMESTAMP(), {len(pm)}, "
      f"{len(rows)}, {last_obs}, {str(retrained).upper()}, 'ok')")

    summary.update({"raw_file": path, "last_observation": str(after), "retrained": retrained})
    return summary


def pipeline(request):
    """HTTP entry point (Cloud Scheduler calls this every hour)."""
    return run(force=request.args.get("force") == "1"), 200


if __name__ == "__main__":
    run(dry=True)