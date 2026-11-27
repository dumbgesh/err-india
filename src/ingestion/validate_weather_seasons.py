import csv
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

try:
    from src.ingestion.validate_weather_pilot import (
        ARCHIVE_URL,
        MODEL_NAME,
        PHYSICAL_BOUNDS,
        STATIONS,
        TIMEZONE,
        VARIABLES,
        validate_station_weather,
    )
except ImportError:
    from validate_weather_pilot import (
        ARCHIVE_URL,
        MODEL_NAME,
        PHYSICAL_BOUNDS,
        STATIONS,
        TIMEZONE,
        VARIABLES,
        validate_station_weather,
    )


SEASON_WINDOWS = [
    ("Winter", "2024-01-15", "2024-01-21"),
    ("Pre-monsoon", "2024-04-15", "2024-04-21"),
    ("Monsoon", "2024-08-24", "2024-08-30"),
    ("Post-monsoon", "2024-11-11", "2024-11-17"),
]

EXPECTED_UTC_OFFSET_SECONDS = 19800
OUTPUT_DIR = Path("data/processed/weather_season_validation")
MAX_RETRIES = 3


def atomic_write_text(path: Path, content: str) -> None:
    """Write text to a temporary file in the same directory and atomically replace target."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            encoding="utf-8",
            newline="",
            delete=False,
        ) as tmp:
            tmp_path = Path(tmp.name)
            tmp.write(content)
            tmp.flush()
            os.fsync(tmp.fileno())
        os.replace(tmp_path, path)
    except Exception:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise


def atomic_write_json(path: Path, payload: Any) -> None:
    """Atomically write formatted JSON."""
    atomic_write_text(path, json.dumps(payload, indent=2) + "\n")


def atomic_write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    """Atomically write rows to CSV."""
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            encoding="utf-8",
            newline="",
            delete=False,
        ) as tmp:
            tmp_path = Path(tmp.name)
            writer = csv.DictWriter(tmp, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
            tmp.flush()
            os.fsync(tmp.fileno())
        os.replace(tmp_path, path)
    except Exception:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise


def build_window_hours(start_date: str, end_date: str) -> list[datetime]:
    """Build expected naive IST hourly timestamps for [start_date, end_date] inclusive."""
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23)
    hours: list[datetime] = []
    cur = start_dt
    while cur <= end_dt:
        hours.append(cur)
        cur += timedelta(hours=1)
    return hours


def validate_response_structure(payload: Any) -> None:
    """Validate response structure, Asia/Kolkata timezone, and hourly array lengths."""
    if not isinstance(payload, dict) or not isinstance(payload.get("hourly"), dict):
        raise ValueError("Unexpected Open-Meteo response structure: missing 'hourly' object.")

    tz = payload.get("timezone")
    utc_offset = payload.get("utc_offset_seconds")
    if tz != TIMEZONE or utc_offset != EXPECTED_UTC_OFFSET_SECONDS:
        raise ValueError(
            f"Unexpected timezone metadata: timezone={tz!r}, utc_offset_seconds={utc_offset!r} "
            f"(expected {TIMEZONE!r}, {EXPECTED_UTC_OFFSET_SECONDS})"
        )

    hourly = payload["hourly"]
    time_arr = hourly.get("time")
    if not isinstance(time_arr, list):
        raise ValueError("Unexpected Open-Meteo response: 'hourly.time' is not a list.")

    expected_len = len(time_arr)
    for var in VARIABLES:
        var_arr = hourly.get(var)
        if not isinstance(var_arr, list) or len(var_arr) != expected_len:
            actual_len = len(var_arr) if isinstance(var_arr, list) else type(var_arr).__name__
            raise ValueError(
                f"Hourly array length mismatch for '{var}': got {actual_len}, expected {expected_len}."
            )


def fetch_with_retries(
    station_id: str,
    station_name: str,
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Fetch ERA5 weather for one station-period with bounded retries and full error capture."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(VARIABLES),
        "timezone": TIMEZONE,
        "models": MODEL_NAME,
    }

    last_error: str | None = None
    last_http_status: int | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        t0 = time.perf_counter()
        try:
            resp = requests.get(ARCHIVE_URL, params=params, timeout=30)
            duration_sec = round(time.perf_counter() - t0, 3)
            retrieval_utc = datetime.now(timezone.utc).isoformat()
            last_http_status = resp.status_code

            if resp.status_code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                last_error = f"HTTP {resp.status_code}"
                time.sleep(1.5 * attempt)
                continue

            resp.raise_for_status()
            payload = resp.json()
            validate_response_structure(payload)

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
                "http_status": resp.status_code,
                "attempts": attempt,
                "request_duration_sec": duration_sec,
                "retrieval_timestamp_utc": retrieval_utc,
                "status": "ok",
                "error": None,
            }
            return payload, meta

        except requests.exceptions.HTTPError as exc:
            if exc.response is not None:
                last_http_status = exc.response.status_code
            last_error = str(exc)
            if attempt < MAX_RETRIES:
                time.sleep(1.5 * attempt)

        except (requests.exceptions.RequestException, ValueError) as exc:
            resp_obj = getattr(exc, "response", None)
            if resp_obj is not None:
                last_http_status = resp_obj.status_code
            last_error = str(exc)
            if attempt < MAX_RETRIES:
                time.sleep(1.5 * attempt)

    meta = {
        "station_id": station_id,
        "station_name": station_name,
        "requested_lat": lat,
        "requested_lon": lon,
        "grid_lat": None,
        "grid_lon": None,
        "elevation_m": None,
        "model": MODEL_NAME,
        "timezone": TIMEZONE,
        "utc_offset_seconds": None,
        "http_status": last_http_status,
        "attempts": MAX_RETRIES,
        "request_duration_sec": None,
        "retrieval_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "status": "failed",
        "error": last_error,
    }
    return None, meta


def verify_naive_ist_strings(time_strs: list[str]) -> int:
    """Verify all returned timestamps parse as naive datetimes (no tz offset)."""
    non_naive = 0
    for t in time_strs:
        dt = datetime.fromisoformat(t)
        if dt.tzinfo is not None:
            non_naive += 1
    return non_naive


def summarize_precipitation(weather_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute min, max, mean, total precipitation, and wet-hour count (0.0 is valid, not missing)."""
    vals = [float(r["precipitation"]) for r in weather_rows if r["precipitation"] is not None]
    if not vals:
        return {
            "precip_non_null": 0,
            "precip_min_mm": None,
            "precip_max_mm": None,
            "precip_mean_mm": None,
            "precip_total_mm": None,
            "wet_hours_gt_0": 0,
        }
    return {
        "precip_non_null": len(vals),
        "precip_min_mm": round(min(vals), 3),
        "precip_max_mm": round(max(vals), 3),
        "precip_mean_mm": round(sum(vals) / len(vals), 4),
        "precip_total_mm": round(sum(vals), 2),
        "wet_hours_gt_0": sum(1 for v in vals if v > 0.0),
    }


def build_summary_row(
    season: str,
    start_date: str,
    end_date: str,
    station_id: str,
    station_name: str,
    status: str,
    http_status: int | None,
    attempts: int,
    expected_rows: int,
    actual_rows: int,
    missing_timestamps: int,
    duplicate_timestamps: int,
    non_naive_timestamps: int | None,
    total_nulls: int | None,
    total_out_of_bounds: int | None,
    grid_lat: float | None,
    grid_lon: float | None,
    elevation_m: float | None,
    precip: dict[str, Any],
) -> dict[str, Any]:
    """Build a schema-consistent summary row for both successful and failed combinations."""
    return {
        "season": season,
        "start_date": start_date,
        "end_date": end_date,
        "station_id": station_id,
        "station_name": station_name,
        "status": status,
        "http_status": http_status,
        "attempts": attempts,
        "expected_rows": expected_rows,
        "actual_rows": actual_rows,
        "missing_timestamps": missing_timestamps,
        "duplicate_timestamps": duplicate_timestamps,
        "non_naive_timestamps": non_naive_timestamps,
        "total_nulls": total_nulls,
        "total_out_of_bounds": total_out_of_bounds,
        "grid_lat": grid_lat,
        "grid_lon": grid_lon,
        "elevation_m": elevation_m,
        "precip_min_mm": precip.get("precip_min_mm"),
        "precip_max_mm": precip.get("precip_max_mm"),
        "precip_mean_mm": precip.get("precip_mean_mm"),
        "precip_total_mm": precip.get("precip_total_mm"),
        "wet_hours_gt_0": precip.get("wet_hours_gt_0", 0),
    }


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    summary_rows: list[dict[str, Any]] = []
    manifest_requests: list[dict[str, Any]] = []
    units: dict[str, str] = {}
    grid_cells: dict[tuple[Any, Any], set[str]] = {}
    detailed_reports: list[dict[str, Any]] = []

    print("Running multi-season ERA5 weather validation (4 periods x 3 stations = 12 combinations)...")

    for season, start_date, end_date in SEASON_WINDOWS:
        expected_hours = build_window_hours(start_date, end_date)
        for sid, sname, lat, lon in STATIONS:
            payload, meta = fetch_with_retries(sid, sname, lat, lon, start_date, end_date)
            meta["season"] = season
            meta["start_date"] = start_date
            meta["end_date"] = end_date
            manifest_requests.append(meta)

            if payload is None:
                print(f"  FAILED: {season} | {sid} -> {meta['error']}")
                summary_rows.append(
                    build_summary_row(
                        season=season,
                        start_date=start_date,
                        end_date=end_date,
                        station_id=sid,
                        station_name=sname,
                        status="failed",
                        http_status=meta["http_status"],
                        attempts=meta["attempts"],
                        expected_rows=len(expected_hours),
                        actual_rows=0,
                        missing_timestamps=len(expected_hours),
                        duplicate_timestamps=0,
                        non_naive_timestamps=None,
                        total_nulls=None,
                        total_out_of_bounds=None,
                        grid_lat=None,
                        grid_lon=None,
                        elevation_m=None,
                        precip={},
                    )
                )
                continue

            if not units:
                units = payload.get("hourly_units", {})

            grid_cells.setdefault((meta["grid_lat"], meta["grid_lon"]), set()).add(sid)
            non_naive = verify_naive_ist_strings(payload["hourly"].get("time", []))
            w_rows, w_report = validate_station_weather(sid, payload, expected_hours)
            precip = summarize_precipitation(w_rows)

            total_nulls = sum(w_report["null_counts"].values())
            total_oob = sum(
                v["out_of_bounds_count"] for v in w_report["range_summary"].values()
            )

            print(
                f"  OK: {season:<12} | {sid} | rows={w_report['actual_rows']}/{w_report['expected_rows']} | "
                f"nulls={total_nulls} | oob={total_oob} | precip_total={precip['precip_total_mm']}mm "
                f"(wet_hrs={precip['wet_hours_gt_0']})"
            )

            summary_rows.append(
                build_summary_row(
                    season=season,
                    start_date=start_date,
                    end_date=end_date,
                    station_id=sid,
                    station_name=sname,
                    status="ok",
                    http_status=meta["http_status"],
                    attempts=meta["attempts"],
                    expected_rows=w_report["expected_rows"],
                    actual_rows=w_report["actual_rows"],
                    missing_timestamps=w_report["missing_timestamps_count"],
                    duplicate_timestamps=w_report["duplicate_timestamps_count"],
                    non_naive_timestamps=non_naive,
                    total_nulls=total_nulls,
                    total_out_of_bounds=total_oob,
                    grid_lat=meta["grid_lat"],
                    grid_lon=meta["grid_lon"],
                    elevation_m=meta["elevation_m"],
                    precip=precip,
                )
            )
            detailed_reports.append(
                {
                    "season": season,
                    "start_date": start_date,
                    "end_date": end_date,
                    "meta": meta,
                    "validation": w_report,
                    "precipitation": precip,
                }
            )

    shared_cells = {str(k): sorted(v) for k, v in grid_cells.items() if len(v) > 1}
    print("Shared ERA5 grid cells:", shared_cells if shared_cells else "None")

    atomic_write_csv(OUTPUT_DIR / "validation_summary.csv", summary_rows)
    atomic_write_json(
        OUTPUT_DIR / "validation_summary.json",
        {
            "model": MODEL_NAME,
            "timezone": TIMEZONE,
            "shared_grid_cells": shared_cells,
            "combinations": summary_rows,
            "details": detailed_reports,
        },
    )
    atomic_write_json(
        OUTPUT_DIR / "processing_manifest.json",
        {
            "provider": "open-meteo",
            "endpoint": ARCHIVE_URL,
            "model": MODEL_NAME,
            "timezone": TIMEZONE,
            "utc_offset_seconds": EXPECTED_UTC_OFFSET_SECONDS,
            "timestamp_convention": "naive IST (Asia/Kolkata, UTC+05:30)",
            "units": units,
            "physical_bounds": PHYSICAL_BOUNDS,
            "requests": manifest_requests,
        },
    )

    md_lines = [
        "# Multi-Season ERA5 Historical Weather Validation Report",
        "",
        f"- **Model:** `{MODEL_NAME}` (`{ARCHIVE_URL}`)",
        f"- **Timezone:** `{TIMEZONE}` (naive IST timestamps)",
        f"- **Shared Grid Cells:** `{shared_cells}`",
        "",
        "| Season | Station | Status | Rows | Missing/Dups/Nulls/OOB | Grid Cell | Precip Total (mm) | Precip Max (mm) | Wet Hours |",
        "|---|---|---|---:|---|---|---:|---:|---:|",
    ]
    for r in summary_rows:
        md_lines.append(
            f"| {r['season']} | {r['station_id']} | {r['status']} | "
            f"{r['actual_rows']}/{r['expected_rows']} | "
            f"{r['missing_timestamps']}/{r['duplicate_timestamps']}/{r['total_nulls']}/{r['total_out_of_bounds']} | "
            f"({r['grid_lat']}, {r['grid_lon']}) | {r['precip_total_mm']} | {r['precip_max_mm']} | {r['wet_hours_gt_0']} |"
        )
    atomic_write_text(OUTPUT_DIR / "season_validation_report.md", "\n".join(md_lines) + "\n")

    failed_count = sum(1 for r in summary_rows if r["status"] != "ok")
    print(f"\nSaved seasonal validation outputs to {OUTPUT_DIR}/ (failed={failed_count})")
    return 1 if failed_count > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
