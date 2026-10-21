"""Backfill history: OpenAQ hourly PM2.5 + Open-Meteo archive weather -> airsafe_history.csv

Windows cmd:
  pip install pandas requests
  set OPENAQ_API_KEY=your_key
  set SENSOR_ID=<id>
  set LAT=13.0827
  set LON=80.2707
  python backfill.py            (default: last 365 days of available data)

Output columns: ts_ist, pm25, temp_c, rh_pct, wind_kmh, pressure_hpa, precip_mm, pm25_flag
All timestamps are the hour (IST) in which the PM2.5 reading period ends.
"""
import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

OPENAQ = "https://api.openaq.org/v3"
ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
HEADERS = {"X-API-Key": os.environ["OPENAQ_API_KEY"]}
SENSOR_ID = os.environ["SENSOR_ID"]
LAT, LON = os.environ["LAT"], os.environ["LON"]
DAYS = int(os.environ.get("DAYS", 365))
CHUNK_DAYS = 30  # 30 days * 24 h = 720 rows, under the 1000-row page limit


def fetch_pm25() -> pd.DataFrame:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=DAYS)
    rows, cur = [], start
    while cur < end:
        nxt = min(cur + timedelta(days=CHUNK_DAYS), end)
        r = requests.get(
            f"{OPENAQ}/sensors/{SENSOR_ID}/hours",
            headers=HEADERS,
            params={
                "datetime_from": cur.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "datetime_to": nxt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "limit": 1000,
            },
            timeout=60,
        )
        r.raise_for_status()
        for x in r.json().get("results", []):
            rows.append(
                {
                    "ts_ist": x["period"]["datetimeTo"]["local"],
                    "pm25": x["value"],
                    "pct_complete": x.get("coverage", {}).get("percentComplete"),
                }
            )
        print(f"  {cur:%Y-%m-%d} -> {nxt:%Y-%m-%d}: total rows {len(rows)}")
        cur = nxt
        time.sleep(1.2)  # stay well under OpenAQ rate limits
    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("No PM2.5 rows returned. Try another SENSOR_ID.")
    # local string like 2026-09-18T20:00:00+05:30 -> naive IST hour
    ts = pd.to_datetime(df["ts_ist"], utc=True).dt.tz_convert("Asia/Kolkata")
    df["ts_ist"] = ts.dt.tz_localize(None).dt.floor("h")
    return df.groupby("ts_ist", as_index=False).agg(pm25=("pm25", "mean"))


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("ts_ist").drop_duplicates("ts_ist")
    # reindex to a complete hourly grid so gaps are explicit
    full = pd.DataFrame({"ts_ist": pd.date_range(df.ts_ist.min(), df.ts_ist.max(), freq="h")})
    df = full.merge(df, on="ts_ist", how="left")
    df["pm25_flag"] = ""
    df.loc[df.pm25 < 0, ["pm25", "pm25_flag"]] = [None, "negative"]
    df.loc[df.pm25 > 1000, ["pm25", "pm25_flag"]] = [None, "outlier"]
    # sensor stuck: identical value 12+ hours in a row
    run = (df.pm25 != df.pm25.shift()).cumsum()
    stuck = df.groupby(run).pm25.transform("size") >= 12
    df.loc[stuck & df.pm25.notna(), "pm25_flag"] = "stuck"
    df.loc[df.pm25_flag == "stuck", "pm25"] = None
    df["pm25_flag"] = df["pm25_flag"].where(df.pm25.notna() | (df.pm25_flag != ""), "missing")
    # fill only short gaps (<=3 h)
    df["pm25"] = df["pm25"].interpolate(limit=3)
    return df


def fetch_weather(start: str, end: str) -> pd.DataFrame:
    r = requests.get(
        ARCHIVE,
        params={
            "latitude": LAT,
            "longitude": LON,
            "start_date": start,
            "end_date": end,
            "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,surface_pressure,precipitation",
            "timezone": "Asia/Kolkata",
        },
        timeout=60,
    )
    r.raise_for_status()
    h = r.json()["hourly"]
    w = pd.DataFrame(h)
    w["time"] = pd.to_datetime(w["time"])
    return w.rename(
        columns={
            "time": "ts_ist",
            "temperature_2m": "temp_c",
            "relative_humidity_2m": "rh_pct",
            "wind_speed_10m": "wind_kmh",
            "surface_pressure": "pressure_hpa",
            "precipitation": "precip_mm",
        }
    )


if __name__ == "__main__":
    print("Fetching PM2.5 ...")
    pm = clean(fetch_pm25())
    print("Fetching weather ...")
    wx = fetch_weather(pm.ts_ist.min().strftime("%Y-%m-%d"), pm.ts_ist.max().strftime("%Y-%m-%d"))
    out = pm.merge(wx, on="ts_ist", how="left")
    fname = f"airsafe_history_{SENSOR_ID}.csv"
    out.to_csv(fname, index=False)
    n = len(out)
    print(f"\nSaved {fname}: {n} hourly rows")
    print(f"USABLE PM2.5: {100 * out.pm25.notna().mean():.1f}%   STUCK-removed: {(out.pm25_flag == 'stuck').sum()} h")
    print(f"Range: {out.ts_ist.min()} -> {out.ts_ist.max()}")
    print(f"PM2.5 missing: {out.pm25.isna().sum()} ({100 * out.pm25.isna().mean():.1f}%)")
    print("Flags:\n", out.pm25_flag.value_counts().to_string())
    print(out.pm25.describe().round(1).to_string())