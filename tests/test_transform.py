"""Unit tests for pipeline/main.py transform(): hourly alignment, weather join,
and the data-quality rules (negative, outlier, stuck sensor, missing hours)."""
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipeline"))
import main  # noqa: E402


def make(values, start="2026-10-01T00:00:00+05:30"):
    """Build fake OpenAQ rows (one per hour) and a matching Open-Meteo payload."""
    t0 = datetime.fromisoformat(start)
    pm, times = [], []
    for i, v in enumerate(values):
        end = t0 + timedelta(hours=i)
        pm.append({"value": v, "period": {"datetimeTo": {"local": end.isoformat()}}})
        times.append(end.replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M"))
    n = len(values)
    wx = {"hourly": {
        "time": times,
        "temperature_2m": [30.0] * n,
        "relative_humidity_2m": [70.0] * n,
        "wind_speed_10m": [8.0] * n,
        "surface_pressure": [1008.0] * n,
        "precipitation": [0.0] * n,
    }}
    return pm, wx


def test_empty_input_returns_no_rows():
    assert main.transform([], {"hourly": {"time": []}}) == []


def test_weather_is_joined_and_missing_hour_is_flagged():
    pm, wx = make([10.0, None, 14.0])  # middle hour has no PM2.5
    rows = main.transform(pm, wx)
    assert len(rows) == 3
    assert rows[0]["pm25"] == 10.0 and rows[0]["temp_c"] == 30.0
    assert rows[1]["pm25"] is None and rows[1]["pm25_flag"] == "missing"
    assert rows[2]["pm25"] == 14.0


def test_negative_and_outlier_values_are_removed():
    pm, wx = make([10.0, -5.0, 2000.0, 12.0])
    rows = main.transform(pm, wx)
    assert rows[1]["pm25"] is None and rows[1]["pm25_flag"] == "negative"
    assert rows[2]["pm25"] is None and rows[2]["pm25_flag"] == "outlier"
    assert rows[0]["pm25"] == 10.0 and rows[3]["pm25"] == 12.0


def test_stuck_sensor_run_of_12_is_removed():
    pm, wx = make([20.0] * 12 + [25.0])
    rows = main.transform(pm, wx)
    assert all(r["pm25"] is None and r["pm25_flag"] == "stuck" for r in rows[:12])
    assert rows[12]["pm25"] == 25.0


def test_run_of_11_identical_values_is_kept():
    pm, wx = make([20.0] * 11 + [25.0])
    rows = main.transform(pm, wx)
    assert all(r["pm25"] == 20.0 for r in rows[:11])


def test_timestamps_use_ist_hour_format():
    pm, wx = make([10.0])
    rows = main.transform(pm, wx)
    assert rows[0]["ts_ist"] == "2026-10-01 00:00:00"