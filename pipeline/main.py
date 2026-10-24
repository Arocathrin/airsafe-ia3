"""AirSafe pipeline (Cloud Function, HTTP trigger).

OpenAQ + Open-Meteo -> raw JSON in GCS -> clean/validate -> BigQuery MERGE
-> retrain + forecast ONLY when new observations arrived -> log the run.

Env vars: OPENAQ_API_KEY, SENSOR_ID, LAT, LON, BUCKET, (optional) HOURS_BACK
Local dry run (fetch + clean only, no cloud writes):  set DRY_RUN=1 & python main.py
Force a retrain on a deployed function: add ?force=1 to the URL.
"""
import json
import logging
import math
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import requests

OPENAQ = "https://api.openaq.org/v3"
METEO = "https://api.open-meteo.com/v1/forecast"
METEO_AQ = "https://air-quality-api.open-meteo.com/v1/air-quality"
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

CREATE_ESTIMATES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS airsafe.pm25_estimates (
  ts TIMESTAMP,
  pm25_estimate FLOAT64,
  fetched_at TIMESTAMP,
  source STRING,
  data_kind STRING,
  latitude FLOAT64,
  longitude FLOAT64
)
"""

MERGE_ESTIMATES_SQL = """
MERGE airsafe.pm25_estimates T
USING airsafe.stg_pm25_estimates S
ON T.ts = S.ts AND T.source = S.source
WHEN MATCHED THEN
  UPDATE SET pm25_estimate = S.pm25_estimate,
             fetched_at = S.fetched_at,
             data_kind = IF(T.data_kind = 'historical' AND S.data_kind = 'forecast', T.data_kind, S.data_kind),
             latitude = S.latitude,
             longitude = S.longitude
WHEN NOT MATCHED THEN
  INSERT (ts, pm25_estimate, fetched_at, source, data_kind, latitude, longitude)
  VALUES (S.ts, S.pm25_estimate, S.fetched_at, S.source, S.data_kind, S.latitude, S.longitude)
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


def fetch_pm25_estimates(lat, lon):
    """Fetch hourly PM2.5 model estimates from Open-Meteo Air Quality API."""
    r = requests.get(
        METEO_AQ,
        params={"latitude": lat, "longitude": lon, "hourly": "pm2_5",
                "past_days": 2, "forecast_days": 2, "timezone": "Asia/Kolkata"},
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


def parse_pm25_estimates(data, lat, lon, fetched_at=None):
    """Parse Open-Meteo air quality response into clean estimate records.

    Skips null, non-finite, and negative values.
    Labels values as 'historical' or 'forecast' with source='Open-Meteo Air Quality API'.
    Preserves supplied latitude and longitude.
    """
    if fetched_at is None:
        fetched_at = datetime.now(timezone.utc)
    elif fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)

    fetched_at_utc = fetched_at.astimezone(timezone.utc)
    fetched_at_ist = fetched_at.astimezone(IST)
    fetched_str = fetched_at_utc.strftime("%Y-%m-%d %H:%M:%S+00:00")

    lat_val = float(lat)
    lon_val = float(lon)

    hourly = (data or {}).get("hourly", {})
    times = hourly.get("time", [])
    values = hourly.get("pm2_5", [])

    dedup = {}
    for t_str, raw_val in zip(times, values):
        if raw_val is None:
            continue
        try:
            val = float(raw_val)
        except (ValueError, TypeError):
            continue
        if not math.isfinite(val) or val < 0:
            continue

        try:
            dt = datetime.fromisoformat(t_str)
        except (ValueError, TypeError):
            continue

        if dt.tzinfo is None:
            dt_ist = dt.replace(tzinfo=IST)
        else:
            dt_ist = dt.astimezone(IST)

        data_kind = "historical" if dt_ist <= fetched_at_ist else "forecast"
        ts_str = dt_ist.isoformat()

        source_name = "Open-Meteo Air Quality API"
        key = (ts_str, source_name)
        if key in dedup and dedup[key]["data_kind"] == "historical" and data_kind == "forecast":
            continue

        dedup[key] = {
            "ts": ts_str,
            "pm25_estimate": val,
            "fetched_at": fetched_str,
            "source": source_name,
            "data_kind": data_kind,
            "latitude": lat_val,
            "longitude": lon_val,
        }
    return list(dedup.values())


# ---------------------------------------------------------------- run
def run(dry=False, force=False):
    pm = fetch_pm25(os.environ["SENSOR_ID"], int(os.environ.get("HOURS_BACK", 720)))
    wx = fetch_weather(os.environ["LAT"], os.environ["LON"])
    rows = transform(pm, wx)
    usable = sum(r["pm25"] is not None for r in rows)
    summary = {"api_rows": len(pm), "hourly_rows": len(rows), "usable_hours": usable}

    now_utc = datetime.now(timezone.utc)
    estimate_rows = []
    try:
        raw_aq = fetch_pm25_estimates(os.environ["LAT"], os.environ["LON"])
        estimate_rows = parse_pm25_estimates(raw_aq, os.environ["LAT"], os.environ["LON"], fetched_at=now_utc)
        summary["estimate_rows"] = len(estimate_rows)
    except Exception as e:
        logging.warning("Open-Meteo air quality ingestion failed: %s", e)
        summary["estimate_rows"] = 0
        summary["estimate_error"] = str(e)

    if dry:
        print(summary)
        print("last 3 rows:", json.dumps(rows[-3:], indent=1))
        if estimate_rows:
            print("last 3 estimate rows:", json.dumps(estimate_rows[-3:], indent=1))
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

    if estimate_rows:
        try:
            est_schema = [
                bigquery.SchemaField("ts", "TIMESTAMP"),
                bigquery.SchemaField("pm25_estimate", "FLOAT"),
                bigquery.SchemaField("fetched_at", "TIMESTAMP"),
                bigquery.SchemaField("source", "STRING"),
                bigquery.SchemaField("data_kind", "STRING"),
                bigquery.SchemaField("latitude", "FLOAT"),
                bigquery.SchemaField("longitude", "FLOAT"),
            ]
            bq.load_table_from_json(
                estimate_rows, "airsafe.stg_pm25_estimates", location="US",
                job_config=bigquery.LoadJobConfig(schema=est_schema, write_disposition="WRITE_TRUNCATE"),
            ).result()
            q(CREATE_ESTIMATES_TABLE_SQL)
            q(MERGE_ESTIMATES_SQL)
        except Exception as e:
            logging.warning("Writing Open-Meteo estimates to BigQuery failed: %s", e)

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