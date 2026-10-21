"""Find PM2.5 sensors near a point using OpenAQ v3.

Usage:
  export OPENAQ_API_KEY=your_key
  python find_stations.py 13.0827 80.2707      # lat lon
Pick a sensor with recent data and few gaps, then use its sensor id as SENSOR_ID.
"""
import os
import sys
import requests

BASE = "https://api.openaq.org/v3"
HEADERS = {"X-API-Key": os.environ["OPENAQ_API_KEY"]}


def main(lat: float, lon: float, radius_m: int = 25000):
    r = requests.get(
        f"{BASE}/locations",
        headers=HEADERS,
        params={
            "coordinates": f"{lat},{lon}",
            "radius": radius_m,  # OpenAQ caps radius at 25000 m
            "parameters_id": 2,  # 2 = PM2.5
            "limit": 20,
        },
        timeout=30,
    )
    r.raise_for_status()
    for loc in r.json().get("results", []):
        last = (loc.get("datetimeLast") or {}).get("utc")
        first = (loc.get("datetimeFirst") or {}).get("utc")
        print(f"\nLOCATION {loc['id']}: {loc['name']}  ({first} -> {last})")
        for s in loc.get("sensors", []):
            p = s["parameter"]["name"]
            if p == "pm25":
                print(f"   pm25 SENSOR_ID = {s['id']}")


if __name__ == "__main__":
    main(float(sys.argv[1]), float(sys.argv[2]))