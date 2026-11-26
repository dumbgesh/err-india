import csv
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import requests


PREV_RUNS_URL = "https://previous-runs-api.open-meteo.com/v1/forecast"
TIMEZONE = "Asia/Kolkata"
START_DATE = "2024-02-01"
END_DATE = "2024-02-29"
EXPECTED_HOURS = 696

STATIONS = [
    ("site_5106", "Vile Parle West", 19.10861, 72.83622),
    ("site_5399", "Chakala-Andheri East", 19.11074, 72.86084),
    ("site_5102", "Malad West", 19.19709, 72.82204),
]

BASE_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "wind_direction_10m",
    "precipitation",
    "surface_pressure",
    "cloud_cover",
]

LEAD_OFFSETS = [
    (24, "previous_day1"),
    (48, "previous_day2"),
    (72, "previous_day3"),
]

MODELS_TO_TEST = ["gfs_global", "icon_global"]

ERA5_PARQUET = Path("data/processed/weather_pilot/weather_historical_2024_02.parquet")
OUTPUT_DIR = Path("data/processed/weather_backtest_pilot")


def fetch_previous_runs(
    model: str, station_id: str, lat: float, lon: float
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Fetch Feb 2024 previous-runs forecasts at 24h, 48h, 72h offsets for one model."""
    hourly_vars = [
        f"{var}_{suffix}" for var in BASE_VARS for _, suffix in LEAD_OFFSETS
    ]
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "hourly": ",".join(hourly_vars),
        "timezone": TIMEZONE,
        "models": model,
    }
    t0 = time.perf_counter()
    resp = requests.get(PREV_RUNS_URL, params=params, timeout=30)
    duration_sec = round(time.perf_counter() - t0, 3)
    retrieval_utc = datetime.now(timezone.utc).isoformat()
    resp.raise_for_status()
    payload = resp.json()

    meta = {
        "model": model,
        "station_id": station_id,
        "requested_lat": lat,
        "requested_lon": lon,
        "grid_lat": payload.get("latitude"),
        "grid_lon": payload.get("longitude"),
        "elevation_m": payload.get("elevation"),
        "timezone": payload.get("timezone"),
        "http_status": resp.status_code,
        "duration_sec": duration_sec,
        "retrieval_timestamp_utc": retrieval_utc,
        "has_explicit_issue_time": False,
    }
    return payload, meta


def load_era5_map() -> dict[tuple[str, datetime], dict[str, float]]:
    """Load Feb 2024 ERA5 values from the validated historical weather pilot."""
    con = duckdb.connect()
    rows = con.execute(
        f"""
        SELECT
            station_id, timestamp_ist,
            temperature_2m, relative_humidity_2m, wind_speed_10m,
            wind_direction_10m, precipitation, surface_pressure, cloud_cover
        FROM read_parquet('{ERA5_PARQUET.as_posix()}')
        """
    ).fetchall()
    era5: dict[tuple[str, datetime], dict[str, float]] = {}
    for r in rows:
        era5[(r[0], r[1])] = {
            "temperature_2m": r[2],
            "relative_humidity_2m": r[3],
            "wind_speed_10m": r[4],
            "wind_direction_10m": r[5],
            "precipitation": r[6],
            "surface_pressure": r[7],
            "cloud_cover": r[8],
        }
    return era5


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    era5 = load_era5_map()

    meta_rows: list[dict[str, Any]] = []
    coverage_rows: list[dict[str, Any]] = []
    comparison_rows: list[dict[str, Any]] = []
    units_by_model: dict[str, Any] = {}

    for model in MODELS_TO_TEST:
        for sid, _, lat, lon in STATIONS:
            payload, meta = fetch_previous_runs(model, sid, lat, lon)
            meta_rows.append(meta)
            units_by_model[model] = payload.get("hourly_units", {})
            hourly = payload.get("hourly", {})
            times = [datetime.fromisoformat(t) for t in hourly.get("time", [])]

            for lead_hr, suffix in LEAD_OFFSETS:
                for var in BASE_VARS:
                    col = f"{var}_{suffix}"
                    series = hourly.get(col, [])
                    non_null = sum(1 for x in series if x is not None)
                    missing = len(times) - non_null
                    coverage_rows.append(
                        {
                            "model": model,
                            "station_id": sid,
                            "lead_hours": lead_hr,
                            "variable": var,
                            "expected_hours": EXPECTED_HOURS,
                            "returned_hours": len(times),
                            "non_null_hours": non_null,
                            "missing_hours": missing,
                            "coverage_pct": round(non_null / EXPECTED_HOURS * 100.0, 2),
                        }
                    )

                    # Compare against ERA5
                    diffs: list[float] = []
                    for ts, fc_val in zip(times, series):
                        era5_row = era5.get((sid, ts))
                        if fc_val is not None and era5_row is not None and era5_row.get(var) is not None:
                            if var == "wind_direction_10m":
                                raw_d = abs(float(fc_val) - float(era5_row[var])) % 360.0
                                diffs.append(min(raw_d, 360.0 - raw_d))
                            else:
                                diffs.append(float(fc_val) - float(era5_row[var]))

                    if diffs:
                        mae = sum(abs(d) for d in diffs) / len(diffs)
                        bias = sum(diffs) / len(diffs)
                        rmse = math.sqrt(sum(d * d for d in diffs) / len(diffs))
                    else:
                        mae = bias = rmse = None

                    comparison_rows.append(
                        {
                            "model": model,
                            "station_id": sid,
                            "lead_hours": lead_hr,
                            "variable": var,
                            "paired_hours": len(diffs),
                            "mae_vs_era5": round(mae, 3) if mae is not None else None,
                            "bias_vs_era5": round(bias, 3) if bias is not None else None,
                            "rmse_vs_era5": round(rmse, 3) if rmse is not None else None,
                        }
                    )

    write_csv(OUTPUT_DIR / "backtest_station_metadata.csv", meta_rows)
    write_csv(OUTPUT_DIR / "backtest_coverage.csv", coverage_rows)
    write_csv(OUTPUT_DIR / "backtest_vs_era5.csv", comparison_rows)
    with (OUTPUT_DIR / "backtest_manifest.json").open("w", encoding="utf-8") as fh:
        json.dump(
            {
                "endpoint": PREV_RUNS_URL,
                "period": f"{START_DATE} to {END_DATE}",
                "timezone": TIMEZONE,
                "units_by_model": units_by_model,
                "stations": meta_rows,
            },
            fh,
            indent=2,
        )

    print("=== MODEL GRID & COVERAGE SUMMARY (2024-02, 696 HOURS) ===")
    for m in meta_rows:
        print(
            f"  {m['model']} | {m['station_id']} | HTTP {m['http_status']} ({m['duration_sec']}s) | "
            f"grid=({m['grid_lat']}, {m['grid_lon']}) elev={m['elevation_m']}"
        )

    print("\n=== NON-NULL COVERAGE BY MODEL & LEAD TIME ===")
    for model in MODELS_TO_TEST:
        for lead_hr, _ in LEAD_OFFSETS:
            sub = [r for r in coverage_rows if r["model"] == model and r["lead_hours"] == lead_hr]
            min_cov = min(r["coverage_pct"] for r in sub)
            max_cov = max(r["coverage_pct"] for r in sub)
            print(f"  {model} | {lead_hr}h: coverage={min_cov}%..{max_cov}% across 3 stations x 7 vars")

    print("\n=== MEAN MAE VS ERA5 ACROSS 3 STATIONS (2024-02) ===")
    for model in MODELS_TO_TEST:
        print(f"Model: {model}")
        for var in BASE_VARS:
            line = []
            for lead_hr, _ in LEAD_OFFSETS:
                sub = [
                    r["mae_vs_era5"]
                    for r in comparison_rows
                    if r["model"] == model and r["variable"] == var and r["lead_hours"] == lead_hr and r["mae_vs_era5"] is not None
                ]
                avg_mae = sum(sub) / len(sub) if sub else None
                line.append(f"{lead_hr}h={avg_mae:.2f}" if avg_mae is not None else f"{lead_hr}h=N/A")
            print(f"  {var:<22}: " + ", ".join(line))


if __name__ == "__main__":
    main()
