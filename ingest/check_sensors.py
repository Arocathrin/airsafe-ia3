"""Check which sensor IDs have recent, complete hourly PM2.5 data (last 30 days).

Usage (Windows cmd):
  set OPENAQ_API_KEY=your_key
  python check_sensors.py 14319 12235653 12235531 15136 12235796 36018 39321 12236299
"""
import os
import sys
from datetime import datetime, timedelta, timezone

import requests

BASE = "https://api.openaq.org/v3"
HEADERS = {"X-API-Key": os.environ["OPENAQ_API_KEY"]}
DAYS = 30


def check(sensor_id: str):
    now = datetime.now(timezone.utc)
    r = requests.get(
        f"{BASE}/sensors/{sensor_id}/hours",
        headers=HEADERS,
        params={
            "datetime_from": (now - timedelta(days=DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "datetime_to": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "limit": 1000,
        },
        timeout=30,
    )
    if r.status_code != 200:
        print(f"{sensor_id}: HTTP {r.status_code}")
        return
    rows = r.json().get("results", [])
    if not rows:
        print(f"{sensor_id}: NO DATA in last {DAYS} days")
        return
    last = rows[-1].get("period", {}).get("datetimeTo", {}).get("utc")
    expected = DAYS * 24
    pct = 100 * len(rows) / expected
    print(f"{sensor_id}: {len(rows)} rows ({pct:.0f}% of {expected}h), last = {last}")


if __name__ == "__main__":
    for sid in sys.argv[1:]:
        check(sid)