"""Unit tests for pipeline/main.py transform(): hourly alignment, weather join,
and the data-quality rules (negative, outlier, stuck sensor, missing hours)."""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import requests

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


# ---------------------------------------------------------------- Open-Meteo tests
def test_parse_pm25_estimates_valid():
    raw_payload = {
        "hourly": {
            "time": ["2026-10-24T10:00", "2026-10-24T11:00"],
            "pm2_5": [24.5, 30.2],
        }
    }
    t_fetch = datetime(2026, 10, 24, 12, 0, 0, tzinfo=timezone.utc)
    rows = main.parse_pm25_estimates(raw_payload, 13.0827, 80.2707, fetched_at=t_fetch)
    assert len(rows) == 2
    assert rows[0]["pm25_estimate"] == 24.5
    assert rows[0]["source"] == "Open-Meteo Air Quality API"
    assert rows[0]["latitude"] == 13.0827
    assert rows[0]["longitude"] == 80.2707
    assert rows[0]["fetched_at"] == "2026-10-24 12:00:00+00:00"
    assert rows[0]["ts"] == "2026-10-24T10:00:00+05:30"
    assert rows[1]["pm25_estimate"] == 30.2
    assert rows[1]["ts"] == "2026-10-24T11:00:00+05:30"


def test_parse_pm25_estimates_timezone_preservation():
    """Verify timestamps loaded into BigQuery preserve the actual instant and offset."""
    raw_payload = {
        "hourly": {
            "time": ["2026-10-24T10:00"],
            "pm2_5": [25.0],
        }
    }
    t_fetch = datetime(2026, 10, 24, 12, 0, 0, tzinfo=timezone.utc)
    rows = main.parse_pm25_estimates(raw_payload, 13.0827, 80.2707, fetched_at=t_fetch)
    assert len(rows) == 1
    ts_str = rows[0]["ts"]
    assert "+05:30" in ts_str
    parsed = datetime.fromisoformat(ts_str)
    assert parsed.tzinfo is not None
    # 10:00 IST (+5:30) must equal 04:30 UTC - preserving the exact instant in time
    assert parsed.astimezone(timezone.utc) == datetime(2026, 10, 24, 4, 30, tzinfo=timezone.utc)


def test_parse_pm25_estimates_filters_invalid():
    raw_payload = {
        "hourly": {
            "time": [
                "2026-10-24T01:00",
                "2026-10-24T02:00",
                "2026-10-24T03:00",
                "2026-10-24T04:00",
                "2026-10-24T05:00",
                "2026-10-24T06:00",
                "2026-10-24T07:00",
                "2026-10-24T08:00",
                "2026-10-24T09:00",
            ],
            "pm2_5": [
                None,
                -15.0,
                -0.001,
                float("nan"),
                float("inf"),
                float("-inf"),
                "not_a_number",
                42.0,
                0.0,
            ],
        }
    }
    rows = main.parse_pm25_estimates(raw_payload, 13.0827, 80.2707)
    assert len(rows) == 2
    assert rows[0]["pm25_estimate"] == 42.0
    assert rows[1]["pm25_estimate"] == 0.0


def test_parse_pm25_estimates_data_kind_labels():
    # fetched_at at 10:00 UTC = 15:30 IST (+5:30)
    t_fetch = datetime(2026, 10, 24, 10, 0, 0, tzinfo=timezone.utc)
    raw_payload = {
        "hourly": {
            "time": [
                "2026-10-24T14:00",  # past
                "2026-10-24T15:00",  # past
                "2026-10-24T16:00",  # future
                "2026-10-24T17:00",  # future
            ],
            "pm2_5": [20.0, 22.0, 25.0, 28.0],
        }
    }
    rows = main.parse_pm25_estimates(raw_payload, 13.0827, 80.2707, fetched_at=t_fetch)
    assert len(rows) == 4
    by_kind = {r["ts"]: r["data_kind"] for r in rows}
    assert by_kind["2026-10-24T14:00:00+05:30"] == "historical"
    assert by_kind["2026-10-24T15:00:00+05:30"] == "historical"
    assert by_kind["2026-10-24T16:00:00+05:30"] == "forecast"
    assert by_kind["2026-10-24T17:00:00+05:30"] == "forecast"


def test_parse_pm25_estimates_deduplication():
    raw_payload = {
        "hourly": {
            "time": ["2026-10-24T10:00", "2026-10-24T10:00"],
            "pm2_5": [24.5, 26.0],
        }
    }
    t_fetch = datetime(2026, 10, 24, 12, 0, 0, tzinfo=timezone.utc)
    rows = main.parse_pm25_estimates(raw_payload, 13.0827, 80.2707, fetched_at=t_fetch)
    assert len(rows) == 1
    assert rows[0]["ts"] == "2026-10-24T10:00:00+05:30"


def test_forecast_to_historical_reclassification():
    """Verify that when an hour transitions from future to past across runs,
    its classification moves from forecast to historical."""
    target_hour = "2026-10-24T12:00"
    payload = {"hourly": {"time": [target_hour], "pm2_5": [35.0]}}

    # Run 1: fetched_at at 05:00 UTC (10:30 IST) -> 12:00 IST is in the future
    t_run1 = datetime(2026, 10, 24, 5, 0, 0, tzinfo=timezone.utc)
    rows_run1 = main.parse_pm25_estimates(payload, 13.0827, 80.2707, fetched_at=t_run1)
    assert len(rows_run1) == 1
    assert rows_run1[0]["data_kind"] == "forecast"

    # Run 2: fetched_at at 10:00 UTC (15:30 IST) -> 12:00 IST is now in the past
    t_run2 = datetime(2026, 10, 24, 10, 0, 0, tzinfo=timezone.utc)
    rows_run2 = main.parse_pm25_estimates(payload, 13.0827, 80.2707, fetched_at=t_run2)
    assert len(rows_run2) == 1
    assert rows_run2[0]["data_kind"] == "historical"
    # Same timestamp key
    assert rows_run1[0]["ts"] == rows_run2[0]["ts"]


def test_merge_sql_deduplication_and_reclassification_logic():
    """Verify MERGE key matches only on (ts, source) so reclassification updates in-place."""
    sql = main.MERGE_ESTIMATES_SQL
    assert "ON T.ts = S.ts AND T.source = S.source" in sql
    assert "ON T.ts = S.ts AND T.source = S.source AND T.data_kind = S.data_kind" not in sql
    assert "data_kind = IF(T.data_kind = 'historical' AND S.data_kind = 'forecast', T.data_kind, S.data_kind)" in sql


def test_create_estimates_table_sql_exists():
    """Verify DDL exists and defines all required columns."""
    ddl = main.CREATE_ESTIMATES_TABLE_SQL
    assert "CREATE TABLE IF NOT EXISTS airsafe.pm25_estimates" in ddl
    for col in ("ts", "pm25_estimate", "fetched_at", "source", "data_kind", "latitude", "longitude"):
        assert col in ddl


def test_parse_pm25_estimates_handles_empty():
    assert main.parse_pm25_estimates(None, 13.0827, 80.2707) == []
    assert main.parse_pm25_estimates({}, 13.0827, 80.2707) == []
    assert main.parse_pm25_estimates({"hourly": {}}, 13.0827, 80.2707) == []


def test_fetch_pm25_estimates_failure_raises(monkeypatch):
    def mock_get(*args, **kwargs):
        raise requests.exceptions.ConnectionError("Connection refused")

    monkeypatch.setattr(requests, "get", mock_get)
    with pytest.raises(requests.exceptions.RequestException):
        main.fetch_pm25_estimates(13.0827, 80.2707)


def test_pipeline_run_continues_on_open_meteo_failure(monkeypatch):
    monkeypatch.setenv("SENSOR_ID", "12235796")
    monkeypatch.setenv("LAT", "13.0827")
    monkeypatch.setenv("LON", "80.2707")

    pm, wx = make([15.0, 18.0])
    monkeypatch.setattr(main, "fetch_pm25", lambda s, h: pm)
    monkeypatch.setattr(main, "fetch_weather", lambda lat, lon: wx)

    def mock_fail_estimates(lat, lon):
        raise requests.exceptions.HTTPError("503 Server Error")

    monkeypatch.setattr(main, "fetch_pm25_estimates", mock_fail_estimates)

    # Should NOT raise, and OpenAQ pipeline should succeed
    summary = main.run(dry=True)
    assert summary["hourly_rows"] == 2
    assert summary["usable_hours"] == 2
    assert summary["estimate_rows"] == 0
    assert "estimate_error" in summary


def test_pipeline_run_succeeds_with_open_meteo(monkeypatch):
    monkeypatch.setenv("SENSOR_ID", "12235796")
    monkeypatch.setenv("LAT", "13.0827")
    monkeypatch.setenv("LON", "80.2707")

    pm, wx = make([15.0, 18.0])
    monkeypatch.setattr(main, "fetch_pm25", lambda s, h: pm)
    monkeypatch.setattr(main, "fetch_weather", lambda lat, lon: wx)

    mock_aq = {
        "hourly": {
            "time": ["2026-10-24T10:00", "2026-10-24T11:00"],
            "pm2_5": [22.0, 24.0],
        }
    }
    monkeypatch.setattr(main, "fetch_pm25_estimates", lambda lat, lon: mock_aq)

    summary = main.run(dry=True)
    assert summary["usable_hours"] == 2
    assert summary["estimate_rows"] == 2
