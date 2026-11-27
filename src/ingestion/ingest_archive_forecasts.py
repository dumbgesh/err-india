import argparse
import csv
import hashlib
import json
import os
import sys
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import requests

try:
    from src.ingestion.validate_weather_pilot import PHYSICAL_BOUNDS, STATIONS, TIMEZONE, VARIABLES
    from src.ingestion.validate_weather_seasons import (
        EXPECTED_UTC_OFFSET_SECONDS,
        atomic_write_csv,
        atomic_write_json,
        build_window_hours,
        verify_naive_ist_strings,
    )
    from src.ingestion.validate_forecast_backtest import LEAD_OFFSETS, PREV_RUNS_URL
except ImportError:
    from validate_weather_pilot import PHYSICAL_BOUNDS, STATIONS, TIMEZONE, VARIABLES
    from validate_weather_seasons import (
        EXPECTED_UTC_OFFSET_SECONDS,
        atomic_write_csv,
        atomic_write_json,
        build_window_hours,
        verify_naive_ist_strings,
    )
    from validate_forecast_backtest import LEAD_OFFSETS, PREV_RUNS_URL


MODEL_NAME = "icon_global"
SOURCE_NAME = "open-meteo-previous-runs"
START_DATE = "2024-01-01"
END_DATE = "2026-11-24"
PROBE_START_DATE = "2024-01-01"
PROBE_END_DATE = "2024-01-03"

RAW_BASE_DIR = Path("data/raw/weather_forecasts_archive/icon_global")
MANIFEST_PATH = Path("data/processed/weather_forecasts_archive/manifests/processing_manifest.json")
UNITS_PATH = Path("data/processed/weather_forecasts_archive/manifests/units.json")
VALIDATION_DIR = Path("data/processed/weather_forecasts_archive/validation")

MAX_RETRIES = 4
REQUEST_TIMEOUT_SEC = 45
INTER_REQUEST_SLEEP_SEC = 0.4

# Deduplicated ICON Global 0.125-degree grid cells covering the 3 Mumbai stations
ICON_GRID_CELLS = [
    {
        "grid_id": "grid_19.125_72.875",
        "grid_lat": 19.125,
        "grid_lon": 72.875,
        "station_ids": ["site_5106", "site_5399"],
    },
    {
        "grid_id": "grid_19.25_72.875",
        "grid_lat": 19.25,
        "grid_lon": 72.875,
        "station_ids": ["site_5102"],
    },
]

ALL_21_HOURLY_VARS = [
    f"{var}_{suffix}" for var in VARIABLES for _, suffix in LEAD_OFFSETS
]

EXPECTED_PARQUET_SCHEMA = [
    ("valid_timestamp_ist", "TIMESTAMP"),
    ("lead_hours_nominal", "INTEGER"),
    ("lead_offset_tag", "VARCHAR"),
    ("grid_id", "VARCHAR"),
    ("grid_lat", "DOUBLE"),
    ("grid_lon", "DOUBLE"),
    ("temperature_2m", "DOUBLE"),
    ("relative_humidity_2m", "DOUBLE"),
    ("wind_speed_10m", "DOUBLE"),
    ("wind_direction_10m", "DOUBLE"),
    ("precipitation", "DOUBLE"),
    ("surface_pressure", "DOUBLE"),
    ("cloud_cover", "DOUBLE"),
    ("model", "VARCHAR"),
    ("source", "VARCHAR"),
]


def build_monthly_chunks(
    start_date: str = START_DATE, end_date: str = END_DATE
) -> list[dict[str, Any]]:
    """Build monthly [start_date, end_date] chunks from start_date through end_date."""
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()

    chunks: list[dict[str, Any]] = []
    y, m = start_dt.year, start_dt.month
    while (y, m) <= (end_dt.year, end_dt.month):
        c_start = start_dt if (y, m) == (start_dt.year, start_dt.month) else date(y, m, 1)
        if (y, m) == (end_dt.year, end_dt.month):
            c_end = end_dt
        elif m == 12:
            c_end = date(y, 12, 31)
        else:
            c_end = date(y, m + 1, 1) - timedelta(days=1)

        expected_valid_hours = ((c_end - c_start).days + 1) * 24
        chunks.append(
            {
                "ym": f"{y:04d}-{m:02d}",
                "year": y,
                "month": m,
                "start_date": c_start.strftime("%Y-%m-%d"),
                "end_date": c_end.strftime("%Y-%m-%d"),
                "expected_valid_hours": expected_valid_hours,
                "expected_parquet_rows": expected_valid_hours * len(LEAD_OFFSETS),
            }
        )
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return chunks


def build_station_grid_mapping() -> list[dict[str, Any]]:
    """Build station-to-grid mapping table for ICON Global."""
    station_lookup = {sid: (sname, lat, lon) for sid, sname, lat, lon in STATIONS}
    elevations = {"site_5106": 10.0, "site_5399": 21.0, "site_5102": 9.0}
    rows: list[dict[str, Any]] = []
    for cell in ICON_GRID_CELLS:
        for sid in cell["station_ids"]:
            sname, req_lat, req_lon = station_lookup[sid]
            rows.append(
                {
                    "station_id": sid,
                    "station_name": sname,
                    "requested_lat": req_lat,
                    "requested_lon": req_lon,
                    "grid_id": cell["grid_id"],
                    "grid_lat": cell["grid_lat"],
                    "grid_lon": cell["grid_lon"],
                    "elevation_m": elevations.get(sid),
                    "model": MODEL_NAME,
                }
            )
    return rows


def compute_sha256(path: Path) -> str:
    """Compute SHA-256 hex digest of a file."""
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_previous_runs_response(payload: Any) -> None:
    """Validate Previous Model Runs response structure, timezone, and all 21 array lengths."""
    if not isinstance(payload, dict) or not isinstance(payload.get("hourly"), dict):
        raise ValueError("Unexpected response structure: missing 'hourly' object.")

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
        raise ValueError("Unexpected response: 'hourly.time' is not a list.")

    expected_len = len(time_arr)
    for var_col in ALL_21_HOURLY_VARS:
        arr = hourly.get(var_col)
        if not isinstance(arr, list) or len(arr) != expected_len:
            actual = len(arr) if isinstance(arr, list) else type(arr).__name__
            raise ValueError(
                f"Hourly array length mismatch for '{var_col}': got {actual}, expected {expected_len}."
            )


def fetch_previous_runs_chunk(
    grid_id: str,
    grid_lat: float,
    grid_lon: float,
    start_date: str,
    end_date: str,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Fetch one ICON Global Previous Model Runs chunk with bounded retries."""
    params = {
        "latitude": grid_lat,
        "longitude": grid_lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(ALL_21_HOURLY_VARS),
        "timezone": TIMEZONE,
        "models": MODEL_NAME,
    }

    last_error: str | None = None
    last_http_status: int | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        t0 = time.perf_counter()
        try:
            resp = requests.get(PREV_RUNS_URL, params=params, timeout=REQUEST_TIMEOUT_SEC)
            duration_sec = round(time.perf_counter() - t0, 3)
            retrieval_utc = datetime.now(timezone.utc).isoformat()
            last_http_status = resp.status_code

            if resp.status_code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                last_error = f"HTTP {resp.status_code}"
                time.sleep(1.5 * attempt)
                continue

            resp.raise_for_status()
            payload = resp.json()
            validate_previous_runs_response(payload)

            ret_lat = payload.get("latitude")
            ret_lon = payload.get("longitude")
            if ret_lat != grid_lat or ret_lon != grid_lon:
                raise ValueError(
                    f"Returned grid cell ({ret_lat}, {ret_lon}) != expected ({grid_lat}, {grid_lon})"
                )

            meta = {
                "grid_id": grid_id,
                "grid_lat": ret_lat,
                "grid_lon": ret_lon,
                "elevation_m": payload.get("elevation"),
                "http_status": resp.status_code,
                "attempts": attempt,
                "request_duration_sec": duration_sec,
                "retrieval_timestamp_utc": retrieval_utc,
                "has_explicit_issue_time": False,
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
        "grid_id": grid_id,
        "grid_lat": grid_lat,
        "grid_lon": grid_lon,
        "elevation_m": None,
        "http_status": last_http_status,
        "attempts": MAX_RETRIES,
        "request_duration_sec": None,
        "retrieval_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "has_explicit_issue_time": False,
        "error": last_error,
    }
    return None, meta


def reshape_to_long_and_validate(
    grid_id: str,
    payload: dict[str, Any],
    expected_hours: list[datetime],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Validate valid_timestamp_ist completeness, reshape wide 21-column response into
    long format (3 rows per valid timestamp), preserve nulls, and check physical bounds.
    """
    hourly = payload["hourly"]
    time_strs = hourly["time"]
    non_naive = verify_naive_ist_strings(time_strs)
    parsed_times = [datetime.fromisoformat(t) for t in time_strs]

    expected_set = set(expected_hours)
    actual_set = set(parsed_times)
    missing_cnt = len(expected_set - actual_set)
    dup_cnt = len(parsed_times) - len(actual_set)

    long_rows: list[dict[str, Any]] = []
    lead_completeness: dict[str, dict[str, int]] = {}
    oob_counts: dict[str, int] = {v: 0 for v in VARIABLES}

    for lead_hours, suffix in LEAD_OFFSETS:
        lead_completeness[f"{lead_hours}h"] = {v: 0 for v in VARIABLES}

    for idx, ts in enumerate(parsed_times):
        for lead_hours, suffix in LEAD_OFFSETS:
            row: dict[str, Any] = {
                "valid_timestamp_ist": ts,
                "lead_hours_nominal": lead_hours,
                "lead_offset_tag": suffix,
            }
            for var in VARIABLES:
                val = hourly[f"{var}_{suffix}"][idx]
                row[var] = val
                if val is None:
                    lead_completeness[f"{lead_hours}h"][var] += 1
                else:
                    fval = float(val)
                    lo, hi = PHYSICAL_BOUNDS[var]
                    if fval < lo or fval > hi:
                        oob_counts[var] += 1
            long_rows.append(row)

    report = {
        "expected_valid_hours": len(expected_hours),
        "actual_valid_hours": len(parsed_times),
        "long_rows": len(long_rows),
        "missing_timestamps": missing_cnt,
        "duplicate_timestamps": dup_cnt,
        "non_naive_timestamps": non_naive,
        "null_counts_by_lead": lead_completeness,
        "total_null_cells": sum(
            c for lead_dict in lead_completeness.values() for c in lead_dict.values()
        ),
        "out_of_bounds_by_var": oob_counts,
        "total_out_of_bounds": sum(oob_counts.values()),
    }
    return long_rows, report


def write_long_parquet_atomically(
    parquet_path: Path,
    grid_id: str,
    grid_lat: float,
    grid_lon: float,
    long_rows: list[dict[str, Any]],
) -> str:
    """Atomically write long-format forecast rows to Parquet and return SHA-256."""
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_csv: Path | None = None
    tmp_pq: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=parquet_path.parent,
            prefix=f".{parquet_path.stem}.",
            suffix=".csv.tmp",
            encoding="utf-8",
            newline="",
            delete=False,
        ) as fh:
            tmp_csv = Path(fh.name)
            fieldnames = ["valid_timestamp_ist", "lead_hours_nominal", "lead_offset_tag"] + VARIABLES
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for r in long_rows:
                out = {
                    "valid_timestamp_ist": r["valid_timestamp_ist"].strftime("%Y-%m-%d %H:%M:%S"),
                    "lead_hours_nominal": r["lead_hours_nominal"],
                    "lead_offset_tag": r["lead_offset_tag"],
                }
                for v in VARIABLES:
                    out[v] = r[v]
                writer.writerow(out)
            fh.flush()
            os.fsync(fh.fileno())

        tmp_pq = parquet_path.with_name(f".{parquet_path.name}.{os.getpid()}.tmp")
        con = duckdb.connect()
        try:
            con.execute(
                f"""
                COPY (
                    SELECT
                        CAST(valid_timestamp_ist AS TIMESTAMP) AS valid_timestamp_ist,
                        CAST(lead_hours_nominal AS INTEGER) AS lead_hours_nominal,
                        CAST(lead_offset_tag AS VARCHAR) AS lead_offset_tag,
                        CAST('{grid_id}' AS VARCHAR) AS grid_id,
                        CAST({grid_lat} AS DOUBLE) AS grid_lat,
                        CAST({grid_lon} AS DOUBLE) AS grid_lon,
                        CAST(temperature_2m AS DOUBLE) AS temperature_2m,
                        CAST(relative_humidity_2m AS DOUBLE) AS relative_humidity_2m,
                        CAST(wind_speed_10m AS DOUBLE) AS wind_speed_10m,
                        CAST(wind_direction_10m AS DOUBLE) AS wind_direction_10m,
                        CAST(precipitation AS DOUBLE) AS precipitation,
                        CAST(surface_pressure AS DOUBLE) AS surface_pressure,
                        CAST(cloud_cover AS DOUBLE) AS cloud_cover,
                        CAST('{MODEL_NAME}' AS VARCHAR) AS model,
                        CAST('{SOURCE_NAME}' AS VARCHAR) AS source
                    FROM read_csv(
                        '{tmp_csv.as_posix()}',
                        header=true,
                        columns={{
                            'valid_timestamp_ist': 'VARCHAR',
                            'lead_hours_nominal': 'INTEGER',
                            'lead_offset_tag': 'VARCHAR',
                            'temperature_2m': 'DOUBLE',
                            'relative_humidity_2m': 'DOUBLE',
                            'wind_speed_10m': 'DOUBLE',
                            'wind_direction_10m': 'DOUBLE',
                            'precipitation': 'DOUBLE',
                            'surface_pressure': 'DOUBLE',
                            'cloud_cover': 'DOUBLE'
                        }},
                        nullstr=''
                    )
                    ORDER BY valid_timestamp_ist, lead_hours_nominal
                ) TO '{tmp_pq.as_posix()}' (FORMAT PARQUET);
                """
            )
        finally:
            con.close()

        os.replace(tmp_pq, parquet_path)
        return compute_sha256(parquet_path)
    finally:
        if tmp_csv is not None and tmp_csv.exists():
            tmp_csv.unlink(missing_ok=True)
        if tmp_pq is not None and tmp_pq.exists():
            tmp_pq.unlink(missing_ok=True)


def verify_existing_chunk(
    chunk_key: str,
    manifest: dict[str, Any],
    parquet_path: Path,
    grid_id: str,
    start_date: str,
    end_date: str,
    expected_parquet_rows: int,
) -> tuple[bool, str]:
    """Eight-point checkpoint verification before skipping an existing monthly forecast chunk."""
    entry = manifest.get("chunks", {}).get(chunk_key)
    if not isinstance(entry, dict) or entry.get("validation_status") != "ok":
        return False, "manifest status is not ok"

    if not parquet_path.exists():
        return False, "parquet file missing"

    expected_sha = entry.get("sha256")
    if not expected_sha:
        return False, "missing sha256 in manifest"

    if compute_sha256(parquet_path) != expected_sha:
        return False, "sha256 checksum mismatch"

    con = duckdb.connect()
    try:
        desc = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{parquet_path.as_posix()}')"
        ).fetchall()
        actual_schema = [(r[0], r[1]) for r in desc]
        if actual_schema != EXPECTED_PARQUET_SCHEMA:
            return False, f"schema mismatch: {actual_schema}"

        stats = con.execute(
            f"""
            SELECT
                COUNT(*),
                MIN(valid_timestamp_ist),
                MAX(valid_timestamp_ist),
                LIST(DISTINCT grid_id),
                LIST(DISTINCT model),
                LIST(DISTINCT lead_hours_nominal ORDER BY lead_hours_nominal)
            FROM read_parquet('{parquet_path.as_posix()}')
            """
        ).fetchone()
    except duckdb.Error as exc:
        return False, f"corrupt or unreadable parquet: {exc}"
    finally:
        con.close()

    row_count, min_ts, max_ts, grid_ids, models, leads = stats
    if int(row_count) != expected_parquet_rows:
        return False, f"row count mismatch: {row_count} != {expected_parquet_rows}"

    exp_min = datetime.strptime(start_date, "%Y-%m-%d")
    exp_max = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23)
    if min_ts != exp_min or max_ts != exp_max:
        return False, f"timestamp boundary mismatch: {min_ts}..{max_ts} != {exp_min}..{exp_max}"

    if grid_ids != [grid_id] or models != [MODEL_NAME] or leads != [24, 48, 72]:
        return False, f"metadata mismatch: {grid_ids}, {models}, {leads}"

    return True, "verified"


def load_manifest(manifest_path: Path) -> dict[str, Any]:
    if manifest_path.exists():
        with manifest_path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
            if isinstance(data, dict) and isinstance(data.get("chunks"), dict):
                return data
    return {
        "provider": SOURCE_NAME,
        "endpoint": PREV_RUNS_URL,
        "model": MODEL_NAME,
        "timezone": TIMEZONE,
        "utc_offset_seconds": EXPECTED_UTC_OFFSET_SECONDS,
        "period_start": START_DATE,
        "period_end": END_DATE,
        "has_explicit_issue_time": False,
        "chunks": {},
    }


def run_january_probe(validation_dir: Path = VALIDATION_DIR) -> int:
    """Probe January 1-3, 2024 for both ICON Global grid cells before full ingestion."""
    validation_dir.mkdir(parents=True, exist_ok=True)
    expected_hours = build_window_hours(PROBE_START_DATE, PROBE_END_DATE)
    probe_results: list[dict[str, Any]] = []
    any_failed = False

    print(f"Running January 1-3, 2024 probe ({len(expected_hours)} valid hours per cell)...")
    for cell in ICON_GRID_CELLS:
        grid_id = cell["grid_id"]
        payload, meta = fetch_previous_runs_chunk(
            grid_id, cell["grid_lat"], cell["grid_lon"], PROBE_START_DATE, PROBE_END_DATE
        )
        if payload is None:
            any_failed = True
            probe_results.append({"grid_id": grid_id, "status": "failed", "meta": meta})
            print(f"  PROBE FAILED: {grid_id} -> {meta['error']}")
            continue

        _, report = reshape_to_long_and_validate(grid_id, payload, expected_hours)
        if report["missing_timestamps"] > 0 or report["duplicate_timestamps"] > 0 or report["non_naive_timestamps"] > 0:
            any_failed = True
            status = "failed"
        else:
            status = "ok"

        probe_results.append(
            {"grid_id": grid_id, "status": status, "meta": meta, "report": report}
        )
        print(
            f"  PROBE {status.upper()}: {grid_id} | valid_hours={report['actual_valid_hours']}/{report['expected_valid_hours']} | "
            f"null_cells={report['total_null_cells']}/{report['long_rows'] * len(VARIABLES)} | oob={report['total_out_of_bounds']}"
        )

    atomic_write_json(validation_dir / "january_2024_probe.json", {"results": probe_results})
    return 1 if any_failed else 0


def run_ingestion(
    raw_base_dir: Path = RAW_BASE_DIR,
    manifest_path: Path = MANIFEST_PATH,
    units_path: Path = UNITS_PATH,
    validation_dir: Path = VALIDATION_DIR,
    sleep_sec: float = INTER_REQUEST_SLEEP_SEC,
) -> int:
    """Execute resumable monthly ICON Global Previous Model Runs ingestion."""
    raw_base_dir.mkdir(parents=True, exist_ok=True)
    validation_dir.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    atomic_write_csv(raw_base_dir / "station_grid_mapping.csv", build_station_grid_mapping())

    manifest = load_manifest(manifest_path)
    chunks = build_monthly_chunks()
    summary_rows: list[dict[str, Any]] = []
    skipped_count = 0

    print(
        f"Archived ICON Global ingestion: {len(ICON_GRID_CELLS)} grid cells x {len(chunks)} months "
        f"= {len(ICON_GRID_CELLS) * len(chunks)} chunks ({START_DATE} to {END_DATE})"
    )

    for cell in ICON_GRID_CELLS:
        grid_id = cell["grid_id"]
        grid_lat = cell["grid_lat"]
        grid_lon = cell["grid_lon"]

        for chunk in chunks:
            ym = chunk["ym"]
            c_start = chunk["start_date"]
            c_end = chunk["end_date"]
            exp_hours = chunk["expected_valid_hours"]
            exp_rows = chunk["expected_parquet_rows"]
            chunk_key = f"{grid_id}|{ym}"
            pq_path = raw_base_dir / grid_id / f"{ym}.parquet"

            ok, _ = verify_existing_chunk(
                chunk_key, manifest, pq_path, grid_id, c_start, c_end, exp_rows
            )
            if ok:
                skipped_count += 1
                entry = manifest["chunks"][chunk_key]
                summary_rows.append(
                    {
                        "grid_id": grid_id,
                        "ym": ym,
                        "validation_status": entry["validation_status"],
                        "skipped_verified": True,
                        "expected_valid_hours": exp_hours,
                        "long_rows": entry.get("long_rows"),
                        "total_null_cells": entry.get("total_null_cells"),
                        "total_out_of_bounds": entry.get("total_out_of_bounds"),
                        "http_status": entry.get("http_status"),
                        "attempts": entry.get("attempts"),
                    }
                )
                print(f"  SKIP (verified): {chunk_key}")
                continue

            payload, meta = fetch_previous_runs_chunk(grid_id, grid_lat, grid_lon, c_start, c_end)
            if payload is None:
                manifest["chunks"][chunk_key] = {
                    **meta,
                    "ym": ym,
                    "expected_valid_hours": exp_hours,
                    "long_rows": 0,
                    "total_null_cells": None,
                    "total_out_of_bounds": None,
                    "sha256": None,
                    "validation_status": "failed",
                }
                atomic_write_json(manifest_path, manifest)
                summary_rows.append(
                    {
                        "grid_id": grid_id,
                        "ym": ym,
                        "validation_status": "failed",
                        "skipped_verified": False,
                        "expected_valid_hours": exp_hours,
                        "long_rows": 0,
                        "total_null_cells": None,
                        "total_out_of_bounds": None,
                        "http_status": meta["http_status"],
                        "attempts": meta["attempts"],
                    }
                )
                print(f"  FAILED: {chunk_key} -> {meta['error']}")
                continue

            if not units_path.exists() and payload.get("hourly_units"):
                atomic_write_json(units_path, payload["hourly_units"])

            expected_hour_list = build_window_hours(c_start, c_end)
            long_rows, report = reshape_to_long_and_validate(grid_id, payload, expected_hour_list)

            if (
                report["missing_timestamps"] > 0
                or report["duplicate_timestamps"] > 0
                or report["non_naive_timestamps"] > 0
            ):
                val_status = "failed"
                sha = None
                err_msg = (
                    f"Temporal validation failed: missing={report['missing_timestamps']}, "
                    f"dups={report['duplicate_timestamps']}, non_naive={report['non_naive_timestamps']}"
                )
            else:
                sha = write_long_parquet_atomically(pq_path, grid_id, grid_lat, grid_lon, long_rows)
                if report["total_out_of_bounds"] > 0:
                    val_status = "review_required"
                    err_msg = f"Out-of-bounds values detected: {report['total_out_of_bounds']}"
                else:
                    val_status = "ok"
                    err_msg = None

            manifest["chunks"][chunk_key] = {
                **meta,
                "ym": ym,
                "start_date": c_start,
                "end_date": c_end,
                **report,
                "parquet_path": pq_path.as_posix(),
                "sha256": sha,
                "validation_status": val_status,
                "error": err_msg or meta["error"],
            }
            atomic_write_json(manifest_path, manifest)

            summary_rows.append(
                {
                    "grid_id": grid_id,
                    "ym": ym,
                    "validation_status": val_status,
                    "skipped_verified": False,
                    "expected_valid_hours": exp_hours,
                    "long_rows": report["long_rows"],
                    "total_null_cells": report["total_null_cells"],
                    "total_out_of_bounds": report["total_out_of_bounds"],
                    "http_status": meta["http_status"],
                    "attempts": meta["attempts"],
                }
            )
            print(
                f"  {val_status.upper()}: {chunk_key} | long_rows={report['long_rows']}/{exp_rows} | "
                f"null_cells={report['total_null_cells']} | oob={report['total_out_of_bounds']}"
            )
            if sleep_sec > 0:
                time.sleep(sleep_sec)

    atomic_write_csv(validation_dir / "validation_summary.csv", summary_rows)
    atomic_write_json(
        validation_dir / "validation_report.json",
        {
            "period": f"{START_DATE} to {END_DATE}",
            "total_chunks": len(summary_rows),
            "skipped_verified": skipped_count,
            "ok_chunks": sum(1 for r in summary_rows if r["validation_status"] == "ok"),
            "review_required_chunks": sum(
                1 for r in summary_rows if r["validation_status"] == "review_required"
            ),
            "failed_chunks": sum(1 for r in summary_rows if r["validation_status"] == "failed"),
            "chunks": summary_rows,
        },
    )

    bad = sum(1 for r in summary_rows if r["validation_status"] != "ok")
    return 1 if bad > 0 else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Archived ICON Global forecast ingestion.")
    parser.add_argument("--probe-only", action="store_true", help="Run only the Jan 1-3, 2024 probe.")
    args = parser.parse_args()
    if args.probe_only:
        return run_january_probe()
    return run_ingestion()


if __name__ == "__main__":
    sys.exit(main())
