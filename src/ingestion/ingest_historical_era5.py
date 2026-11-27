import hashlib
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
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
    from src.ingestion.validate_weather_seasons import (
        EXPECTED_UTC_OFFSET_SECONDS,
        atomic_write_csv,
        atomic_write_json,
        build_window_hours,
        validate_response_structure,
        verify_naive_ist_strings,
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
    from validate_weather_seasons import (
        EXPECTED_UTC_OFFSET_SECONDS,
        atomic_write_csv,
        atomic_write_json,
        build_window_hours,
        validate_response_structure,
        verify_naive_ist_strings,
    )


SOURCE_NAME = "open-meteo"
START_DATE = "2019-06-01"
END_DATE = "2026-11-24"
AQ_EXPORT_CUTOFF_IST = "2026-11-24 12:24:03"
AQ_EXPORT_CUTOFF_UTC = "2026-11-24T06:54:03+00:00"

RAW_BASE_DIR = Path("data/raw/weather_historical/era5")
MANIFEST_PATH = Path("data/processed/weather_historical/manifests/processing_manifest.json")
UNITS_PATH = Path("data/processed/weather_historical/manifests/units.json")
VALIDATION_DIR = Path("data/processed/weather_historical/validation")

MAX_RETRIES = 4
REQUEST_TIMEOUT_SEC = 45
INTER_REQUEST_SLEEP_SEC = 0.5

# Deduplicated ERA5 0.25-degree grid cells covering the 3 Mumbai stations
GRID_CELLS = [
    {
        "grid_id": "grid_19.0_73.0",
        "grid_lat": 19.0,
        "grid_lon": 73.0,
        "station_ids": ["site_5106", "site_5399"],
    },
    {
        "grid_id": "grid_19.25_73.0",
        "grid_lat": 19.25,
        "grid_lon": 73.0,
        "station_ids": ["site_5102"],
    },
]

EXPECTED_PARQUET_SCHEMA = [
    ("timestamp_ist", "TIMESTAMP"),
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


def build_yearly_chunks(
    start_date: str = START_DATE, end_date: str = END_DATE
) -> list[dict[str, Any]]:
    """Build yearly [start, end] date chunks between start_date and end_date inclusive."""
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()

    chunks: list[dict[str, Any]] = []
    for year in range(start_dt.year, end_dt.year + 1):
        c_start = start_dt if year == start_dt.year else datetime(year, 1, 1).date()
        c_end = end_dt if year == end_dt.year else datetime(year, 12, 31).date()
        expected_hours = ((c_end - c_start).days + 1) * 24
        chunks.append(
            {
                "year": year,
                "start_date": c_start.strftime("%Y-%m-%d"),
                "end_date": c_end.strftime("%Y-%m-%d"),
                "expected_hours": expected_hours,
            }
        )
    return chunks


def build_station_grid_mapping() -> list[dict[str, Any]]:
    """Build station-to-grid mapping table preserving requested and grid coordinates."""
    station_lookup = {sid: (sname, lat, lon) for sid, sname, lat, lon in STATIONS}
    elevations = {"site_5106": 10.0, "site_5399": 21.0, "site_5102": 9.0}
    rows: list[dict[str, Any]] = []
    for cell in GRID_CELLS:
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


def load_manifest(manifest_path: Path) -> dict[str, Any]:
    """Load existing manifest or initialize a fresh structure."""
    if manifest_path.exists():
        with manifest_path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
            if isinstance(data, dict) and isinstance(data.get("chunks"), dict):
                return data
    return {
        "provider": SOURCE_NAME,
        "endpoint": ARCHIVE_URL,
        "model": MODEL_NAME,
        "timezone": TIMEZONE,
        "utc_offset_seconds": EXPECTED_UTC_OFFSET_SECONDS,
        "period_start": START_DATE,
        "period_end": END_DATE,
        "aq_export_cutoff_ist": AQ_EXPORT_CUTOFF_IST,
        "aq_export_cutoff_utc": AQ_EXPORT_CUTOFF_UTC,
        "chunks": {},
    }


def chunk_parquet_path(raw_base_dir: Path, grid_id: str, year: int) -> Path:
    return raw_base_dir / grid_id / f"{year}.parquet"


def verify_existing_chunk(
    chunk_key: str,
    manifest: dict[str, Any],
    parquet_path: Path,
    grid_id: str,
    start_date: str,
    end_date: str,
    expected_hours: int,
) -> tuple[bool, str]:
    """
    Eight-point checkpoint verification before skipping an existing chunk:
    1. Manifest status is 'ok'.
    2. Parquet file exists.
    3. Parquet is readable by DuckDB.
    4. Schema matches EXPECTED_PARQUET_SCHEMA.
    5. Row count matches expected_hours.
    6. MIN/MAX timestamps match chunk boundaries.
    7. Distinct grid_id and model match.
    8. File SHA-256 checksum matches manifest.
    """
    entry = manifest.get("chunks", {}).get(chunk_key)
    if not isinstance(entry, dict) or entry.get("validation_status") != "ok":
        return False, "manifest status is not ok"

    if not parquet_path.exists():
        return False, "parquet file missing"

    expected_sha = entry.get("sha256")
    if not expected_sha:
        return False, "missing sha256 in manifest"

    actual_sha = compute_sha256(parquet_path)
    if actual_sha != expected_sha:
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
                MIN(timestamp_ist),
                MAX(timestamp_ist),
                LIST(DISTINCT grid_id),
                LIST(DISTINCT model)
            FROM read_parquet('{parquet_path.as_posix()}')
            """
        ).fetchone()
    except duckdb.Error as exc:
        return False, f"corrupt or unreadable parquet: {exc}"
    finally:
        con.close()

    row_count, min_ts, max_ts, grid_ids, models = stats
    if int(row_count) != expected_hours:
        return False, f"row count mismatch: {row_count} != {expected_hours}"

    exp_min = datetime.strptime(start_date, "%Y-%m-%d")
    exp_max = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23)
    if min_ts != exp_min or max_ts != exp_max:
        return False, f"timestamp boundary mismatch: {min_ts}..{max_ts} != {exp_min}..{exp_max}"

    if grid_ids != [grid_id] or models != [MODEL_NAME]:
        return False, f"grid_id/model mismatch: {grid_ids}, {models}"

    return True, "verified"


def fetch_chunk_with_retries(
    grid_id: str,
    grid_lat: float,
    grid_lon: float,
    start_date: str,
    end_date: str,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Fetch one yearly ERA5 grid-cell chunk with bounded retries and backoff."""
    params = {
        "latitude": grid_lat,
        "longitude": grid_lon,
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
            resp = requests.get(ARCHIVE_URL, params=params, timeout=REQUEST_TIMEOUT_SEC)
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
        "error": last_error,
    }
    return None, meta


def write_parquet_atomically(
    parquet_path: Path,
    grid_id: str,
    grid_lat: float,
    grid_lon: float,
    weather_rows: list[dict[str, Any]],
) -> str:
    """Write hourly weather rows to Parquet via temporary CSV/Parquet and os.replace."""
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
            import csv

            tmp_csv = Path(fh.name)
            fieldnames = ["timestamp_ist"] + VARIABLES
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for r in weather_rows:
                row = {"timestamp_ist": r["timestamp_ist"].strftime("%Y-%m-%d %H:%M:%S")}
                for v in VARIABLES:
                    row[v] = r[v]
                writer.writerow(row)
            fh.flush()
            os.fsync(fh.fileno())

        tmp_pq = parquet_path.with_name(f".{parquet_path.name}.{os.getpid()}.tmp")
        con = duckdb.connect()
        try:
            con.execute(
                f"""
                COPY (
                    SELECT
                        CAST(timestamp_ist AS TIMESTAMP) AS timestamp_ist,
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
                            'timestamp_ist': 'VARCHAR',
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
                    ORDER BY timestamp_ist
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


def run_ingestion(
    raw_base_dir: Path = RAW_BASE_DIR,
    manifest_path: Path = MANIFEST_PATH,
    units_path: Path = UNITS_PATH,
    validation_dir: Path = VALIDATION_DIR,
    sleep_sec: float = INTER_REQUEST_SLEEP_SEC,
) -> int:
    """Execute resumable, grid-deduplicated historical ERA5 ingestion."""
    raw_base_dir.mkdir(parents=True, exist_ok=True)
    validation_dir.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    atomic_write_csv(raw_base_dir / "station_grid_mapping.csv", build_station_grid_mapping())

    manifest = load_manifest(manifest_path)
    chunks = build_yearly_chunks()
    summary_rows: list[dict[str, Any]] = []
    skipped_count = 0

    print(
        f"Historical ERA5 ingestion: {len(GRID_CELLS)} grid cells x {len(chunks)} yearly chunks "
        f"= {len(GRID_CELLS) * len(chunks)} total chunks ({START_DATE} to {END_DATE})"
    )

    for cell in GRID_CELLS:
        grid_id = cell["grid_id"]
        grid_lat = cell["grid_lat"]
        grid_lon = cell["grid_lon"]

        for chunk in chunks:
            year = chunk["year"]
            c_start = chunk["start_date"]
            c_end = chunk["end_date"]
            exp_hours = chunk["expected_hours"]
            chunk_key = f"{grid_id}|{year}"
            pq_path = chunk_parquet_path(raw_base_dir, grid_id, year)

            ok, reason = verify_existing_chunk(
                chunk_key=chunk_key,
                manifest=manifest,
                parquet_path=pq_path,
                grid_id=grid_id,
                start_date=c_start,
                end_date=c_end,
                expected_hours=exp_hours,
            )
            if ok:
                skipped_count += 1
                entry = manifest["chunks"][chunk_key]
                summary_rows.append(
                    {
                        "grid_id": grid_id,
                        "year": year,
                        "start_date": c_start,
                        "end_date": c_end,
                        "validation_status": entry["validation_status"],
                        "skipped_verified": True,
                        "expected_hours": exp_hours,
                        "actual_rows": entry.get("actual_rows"),
                        "missing_timestamps": entry.get("missing_timestamps"),
                        "duplicate_timestamps": entry.get("duplicate_timestamps"),
                        "total_nulls": entry.get("total_nulls"),
                        "total_out_of_bounds": entry.get("total_out_of_bounds"),
                        "http_status": entry.get("http_status"),
                        "attempts": entry.get("attempts"),
                    }
                )
                print(f"  SKIP (verified): {chunk_key}")
                continue

            payload, meta = fetch_chunk_with_retries(grid_id, grid_lat, grid_lon, c_start, c_end)
            if payload is None:
                manifest["chunks"][chunk_key] = {
                    **meta,
                    "year": year,
                    "start_date": c_start,
                    "end_date": c_end,
                    "expected_hours": exp_hours,
                    "actual_rows": 0,
                    "missing_timestamps": exp_hours,
                    "duplicate_timestamps": 0,
                    "total_nulls": None,
                    "total_out_of_bounds": None,
                    "sha256": None,
                    "validation_status": "failed",
                }
                atomic_write_json(manifest_path, manifest)
                summary_rows.append(
                    {
                        "grid_id": grid_id,
                        "year": year,
                        "start_date": c_start,
                        "end_date": c_end,
                        "validation_status": "failed",
                        "skipped_verified": False,
                        "expected_hours": exp_hours,
                        "actual_rows": 0,
                        "missing_timestamps": exp_hours,
                        "duplicate_timestamps": 0,
                        "total_nulls": None,
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
            non_naive = verify_naive_ist_strings(payload["hourly"].get("time", []))
            w_rows, w_report = validate_station_weather(grid_id, payload, expected_hour_list)

            missing_cnt = w_report["missing_timestamps_count"]
            dup_cnt = w_report["duplicate_timestamps_count"]
            total_nulls = sum(w_report["null_counts"].values())
            total_oob = sum(v["out_of_bounds_count"] for v in w_report["range_summary"].values())

            if missing_cnt > 0 or dup_cnt > 0 or non_naive > 0:
                val_status = "failed"
                sha = None
                err_msg = (
                    f"Temporal validation failed: missing={missing_cnt}, "
                    f"dups={dup_cnt}, non_naive={non_naive}"
                )
            else:
                sha = write_parquet_atomically(pq_path, grid_id, grid_lat, grid_lon, w_rows)
                if total_oob > 0:
                    val_status = "review_required"
                    err_msg = f"Out-of-bounds values detected: {total_oob}"
                else:
                    val_status = "ok"
                    err_msg = None

            manifest["chunks"][chunk_key] = {
                **meta,
                "year": year,
                "start_date": c_start,
                "end_date": c_end,
                "expected_hours": exp_hours,
                "actual_rows": w_report["actual_rows"],
                "missing_timestamps": missing_cnt,
                "duplicate_timestamps": dup_cnt,
                "non_naive_timestamps": non_naive,
                "null_counts": w_report["null_counts"],
                "total_nulls": total_nulls,
                "range_summary": w_report["range_summary"],
                "total_out_of_bounds": total_oob,
                "parquet_path": pq_path.as_posix(),
                "sha256": sha,
                "validation_status": val_status,
                "error": err_msg or meta["error"],
            }
            atomic_write_json(manifest_path, manifest)

            summary_rows.append(
                {
                    "grid_id": grid_id,
                    "year": year,
                    "start_date": c_start,
                    "end_date": c_end,
                    "validation_status": val_status,
                    "skipped_verified": False,
                    "expected_hours": exp_hours,
                    "actual_rows": w_report["actual_rows"],
                    "missing_timestamps": missing_cnt,
                    "duplicate_timestamps": dup_cnt,
                    "total_nulls": total_nulls,
                    "total_out_of_bounds": total_oob,
                    "http_status": meta["http_status"],
                    "attempts": meta["attempts"],
                }
            )
            print(
                f"  {val_status.upper()}: {chunk_key} | rows={w_report['actual_rows']}/{exp_hours} | "
                f"nulls={total_nulls} | oob={total_oob}"
            )
            if sleep_sec > 0:
                time.sleep(sleep_sec)

    atomic_write_csv(validation_dir / "validation_summary.csv", summary_rows)
    atomic_write_json(
        validation_dir / "validation_report.json",
        {
            "period": f"{START_DATE} to {END_DATE}",
            "aq_export_cutoff_ist": AQ_EXPORT_CUTOFF_IST,
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

    bad_chunks = sum(1 for r in summary_rows if r["validation_status"] != "ok")
    return 1 if bad_chunks > 0 else 0


if __name__ == "__main__":
    sys.exit(run_ingestion())
