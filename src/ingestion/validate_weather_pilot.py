import csv
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import requests


ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
MODEL_NAME = "era5"
TIMEZONE = "Asia/Kolkata"
START_DATE = "2024-02-01"
END_DATE = "2024-02-29"
EXPECTED_HOURS = 696  # 2024 is a leap year: 29 * 24 = 696

STATIONS = [
    ("site_5106", "Vile Parle West", 19.10861, 72.83622),
    ("site_5399", "Chakala-Andheri East", 19.11074, 72.86084),
    ("site_5102", "Malad West", 19.19709, 72.82204),
]

VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "wind_direction_10m",
    "precipitation",
    "surface_pressure",
    "cloud_cover",
]

PHYSICAL_BOUNDS = {
    "temperature_2m": (0.0, 50.0),
    "relative_humidity_2m": (0.0, 100.0),
    "wind_speed_10m": (0.0, 200.0),
    "wind_direction_10m": (0.0, 360.0),
    "precipitation": (0.0, 500.0),
    "surface_pressure": (900.0, 1060.0),
    "cloud_cover": (0.0, 100.0),
}

AQ_PARQUET_PATH = Path("data/processed/measurement_quality/filtered_measurements_2019_2026.parquet")
OUTPUT_DIR = Path("data/processed/weather_pilot")


def build_expected_feb_2024_hours() -> list[datetime]:
    """Build all 696 expected naive IST hourly timestamps for February 2024."""
    start = datetime(2024, 2, 1, 0, 0)
    return [start + timedelta(hours=i) for i in range(EXPECTED_HOURS)]


def fetch_station_weather(
    station_id: str, station_name: str, lat: float, lon: float
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Fetch February 2024 ERA5 hourly weather for one station."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "hourly": ",".join(VARIABLES),
        "timezone": TIMEZONE,
        "models": MODEL_NAME,
    }
    t0 = time.perf_counter()
    response = requests.get(ARCHIVE_URL, params=params, timeout=30)
    duration_sec = round(time.perf_counter() - t0, 3)
    retrieval_utc = datetime.now(timezone.utc).isoformat()
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, dict) or "hourly" not in payload:
        raise ValueError(f"Unexpected Open-Meteo response structure for {station_id}")

    meta = {
        "station_id": station_id,
        "station_name": station_name,
        "requested_lat": lat,
        "requested_lon": lon,
        "grid_lat": payload.get("latitude"),
        "grid_lon": payload.get("longitude"),
        "elevation_m": payload.get("elevation"),
        "model": MODEL_NAME,
        "timezone": payload.get("timezone"),
        "utc_offset_seconds": payload.get("utc_offset_seconds"),
        "http_status": response.status_code,
        "request_duration_sec": duration_sec,
        "retrieval_timestamp_utc": retrieval_utc,
    }
    return payload, meta


def validate_station_weather(
    station_id: str, payload: dict[str, Any], expected_hours: list[datetime]
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Validate timestamps, nulls, duplicates, and physical ranges for one station."""
    hourly = payload["hourly"]
    time_strs = hourly.get("time", [])
    parsed_times = [datetime.fromisoformat(t) for t in time_strs]

    expected_set = set(expected_hours)
    actual_set = set(parsed_times)
    missing_ts = sorted(expected_set - actual_set)
    dup_count = len(parsed_times) - len(actual_set)

    null_counts = {
        var: sum(1 for v in hourly.get(var, []) if v is None) for var in VARIABLES
    }
    range_summary: dict[str, Any] = {}
    out_of_range_flags: dict[str, int] = {}

    for var in VARIABLES:
        vals = [float(v) for v in hourly.get(var, []) if v is not None]
        lo, hi = PHYSICAL_BOUNDS[var]
        oob = sum(1 for v in vals if v < lo or v > hi)
        out_of_range_flags[var] = oob
        range_summary[var] = {
            "min": round(min(vals), 2) if vals else None,
            "max": round(max(vals), 2) if vals else None,
            "out_of_bounds_count": oob,
        }

    weather_rows: list[dict[str, Any]] = []
    for idx, ts in enumerate(parsed_times):
        row: dict[str, Any] = {
            "station_id": station_id,
            "timestamp_ist": ts,
            "grid_lat": payload.get("latitude"),
            "grid_lon": payload.get("longitude"),
            "model": MODEL_NAME,
        }
        for var in VARIABLES:
            row[var] = hourly[var][idx]
        weather_rows.append(row)

    report = {
        "station_id": station_id,
        "expected_rows": len(expected_hours),
        "actual_rows": len(parsed_times),
        "missing_timestamps_count": len(missing_ts),
        "duplicate_timestamps_count": dup_count,
        "null_counts": null_counts,
        "range_summary": range_summary,
    }
    return weather_rows, report


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    expected_hours = build_expected_feb_2024_hours()

    all_weather_rows: list[dict[str, Any]] = []
    station_meta_rows: list[dict[str, Any]] = []
    station_reports: list[dict[str, Any]] = []
    units: dict[str, str] = {}

    for sid, sname, lat, lon in STATIONS:
        payload, meta = fetch_station_weather(sid, sname, lat, lon)
        if not units:
            units = payload.get("hourly_units", {})
        w_rows, w_report = validate_station_weather(sid, payload, expected_hours)
        all_weather_rows.extend(w_rows)
        station_meta_rows.append(meta)
        station_reports.append(w_report)
        print(
            f"Weather {sid}: HTTP {meta['http_status']} in {meta['request_duration_sec']}s, "
            f"grid=({meta['grid_lat']}, {meta['grid_lon']}), rows={w_report['actual_rows']}/{w_report['expected_rows']}, "
            f"nulls={sum(w_report['null_counts'].values())}"
        )

    # Check shared grid cells
    grid_cells: dict[tuple[Any, Any], list[str]] = {}
    for m in station_meta_rows:
        grid_cells.setdefault((m["grid_lat"], m["grid_lon"]), []).append(m["station_id"])
    shared_cells = {str(k): v for k, v in grid_cells.items() if len(v) > 1}
    print("Shared ERA5 grid cells:", shared_cells if shared_cells else "None")

    # Save raw historical weather CSV & Parquet (separate from aligned AQ features)
    weather_csv_path = OUTPUT_DIR / "weather_historical_2024_02.csv"
    csv_weather_rows = [
        {**r, "timestamp_ist": r["timestamp_ist"].strftime("%Y-%m-%d %H:%M:%S")}
        for r in all_weather_rows
    ]
    write_csv(weather_csv_path, csv_weather_rows)

    con = duckdb.connect()
    weather_parquet_path = OUTPUT_DIR / "weather_historical_2024_02.parquet"
    con.execute(
        f"""
        COPY (
            SELECT
                station_id,
                CAST(timestamp_ist AS TIMESTAMP) AS timestamp_ist,
                CAST(grid_lat AS DOUBLE) AS grid_lat,
                CAST(grid_lon AS DOUBLE) AS grid_lon,
                model,
                CAST(temperature_2m AS DOUBLE) AS temperature_2m,
                CAST(relative_humidity_2m AS DOUBLE) AS relative_humidity_2m,
                CAST(wind_speed_10m AS DOUBLE) AS wind_speed_10m,
                CAST(wind_direction_10m AS DOUBLE) AS wind_direction_10m,
                CAST(precipitation AS DOUBLE) AS precipitation,
                CAST(surface_pressure AS DOUBLE) AS surface_pressure,
                CAST(cloud_cover AS DOUBLE) AS cloud_cover
            FROM read_csv_auto('{weather_csv_path.as_posix()}')
        ) TO '{weather_parquet_path.as_posix()}' (FORMAT PARQUET);
        """
    )

    # LEFT JOIN air-quality records (Feb 2024) -> historical weather on (station_id, timestamp_ist)
    join_parquet_path = OUTPUT_DIR / "aq_weather_join_2024_02.parquet"
    con.execute(
        f"""
        COPY (
            WITH aq_feb AS (
                SELECT
                    station_id,
                    parameter_name,
                    collected_at AS timestamp_ist,
                    value AS aq_value,
                    CASE
                        WHEN value < 0 THEN 'confirmed_invalid'
                        ELSE 'raw_retained'
                    END AS qa_flag
                FROM read_parquet('{AQ_PARQUET_PATH.as_posix()}')
                WHERE collected_at >= TIMESTAMP '2024-02-01 00:00:00'
                  AND collected_at < TIMESTAMP '2024-03-01 00:00:00'
            )
            SELECT
                a.station_id,
                a.parameter_name,
                a.timestamp_ist,
                a.aq_value,
                a.qa_flag,
                w.temperature_2m,
                w.relative_humidity_2m,
                w.wind_speed_10m,
                w.wind_direction_10m,
                w.precipitation,
                w.surface_pressure,
                w.cloud_cover,
                CASE WHEN w.timestamp_ist IS NOT NULL THEN 1 ELSE 0 END AS weather_matched
            FROM aq_feb a
            LEFT JOIN read_parquet('{weather_parquet_path.as_posix()}') w
              ON a.station_id = w.station_id
             AND a.timestamp_ist = w.timestamp_ist
            ORDER BY a.station_id, a.parameter_name, a.timestamp_ist
        ) TO '{join_parquet_path.as_posix()}' (FORMAT PARQUET);
        """
    )

    join_stats = con.execute(
        f"""
        SELECT
            station_id,
            parameter_name,
            COUNT(*) AS aq_rows,
            SUM(weather_matched) AS matched_weather_rows,
            COUNT(*) - SUM(weather_matched) AS unmatched_aq_rows,
            ROUND(SUM(weather_matched) * 100.0 / COUNT(*), 2) AS join_coverage_pct,
            COUNT(*) FILTER (
                WHERE weather_matched = 1 AND (
                    temperature_2m IS NULL OR relative_humidity_2m IS NULL OR
                    wind_speed_10m IS NULL OR wind_direction_10m IS NULL OR
                    precipitation IS NULL OR surface_pressure IS NULL OR cloud_cover IS NULL
                )
            ) AS matched_rows_with_missing_weather
        FROM read_parquet('{join_parquet_path.as_posix()}')
        GROUP BY station_id, parameter_name
        ORDER BY station_id, parameter_name;
        """
    ).fetchall()

    join_summary_rows = [
        {
            "station_id": r[0],
            "parameter_name": r[1],
            "aq_rows": int(r[2]),
            "matched_weather_rows": int(r[3]),
            "unmatched_aq_rows": int(r[4]),
            "join_coverage_pct": float(r[5]),
            "matched_rows_with_missing_weather": int(r[6]),
        }
        for r in join_stats
    ]

    write_csv(OUTPUT_DIR / "station_metadata.csv", station_meta_rows)
    write_csv(OUTPUT_DIR / "join_summary.csv", join_summary_rows)

    with (OUTPUT_DIR / "validation_report.json").open("w", encoding="utf-8") as fh:
        json.dump(
            {
                "pilot_month": "2024-02",
                "expected_hours": EXPECTED_HOURS,
                "shared_grid_cells": shared_cells,
                "station_reports": station_reports,
                "join_summary": join_summary_rows,
            },
            fh,
            indent=2,
        )

    manifest = {
        "provider": "open-meteo",
        "endpoint": ARCHIVE_URL,
        "model": MODEL_NAME,
        "pilot_start_date": START_DATE,
        "pilot_end_date": END_DATE,
        "timezone": TIMEZONE,
        "units": units,
        "stations": station_meta_rows,
    }
    with (OUTPUT_DIR / "processing_manifest.json").open("w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)

    print("\n=== AIR-QUALITY + WEATHER LEFT JOIN SUMMARY (2024-02) ===")
    for r in join_summary_rows:
        print(
            f"  {r['station_id']} {r['parameter_name']}: aq={r['aq_rows']}, "
            f"matched={r['matched_weather_rows']}, unmatched={r['unmatched_aq_rows']}, "
            f"coverage={r['join_coverage_pct']}%, missing_weather_after_join={r['matched_rows_with_missing_weather']}"
        )
    print(f"\nSaved pilot outputs to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
