import csv
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import requests

try:
    from src.ingestion.validate_duckdb_month import (
        API_BASELINE,
        POLLUTANTS,
        STATIONS,
        build_month_hours,
        compute_longest_missing_streak,
        create_authenticated_duckdb_connection,
        load_api_key,
    )
except ImportError:
    from validate_duckdb_month import (
        API_BASELINE,
        POLLUTANTS,
        STATIONS,
        build_month_hours,
        compute_longest_missing_streak,
        create_authenticated_duckdb_connection,
        load_api_key,
    )


FILES_INDEX_URL = "https://airquality.xkdr.org/v1/files"
FILES_BASE_URL = "https://airquality.xkdr.org/v1/files/"
TARGET_YEAR = 2024
OUTPUT_DIR = Path("data/processed/duckdb_2024_pilot")

# Validated August 2024 full-month unique valid-hour counts from validate_duckdb_month.py
VALIDATED_AUG_2024_HOURS = {
    ("site_5106", "PM2.5"): 678,
    ("site_5106", "PM10"): 672,
    ("site_5399", "PM2.5"): 0,
    ("site_5399", "PM10"): 0,
    ("site_5102", "PM2.5"): 646,
    ("site_5102", "PM10"): 682,
}


def fetch_2024_partition_metadata(api_key: str) -> list[dict[str, Any]]:
    """Fetch 2024 monthly measurement Parquet metadata from XKDR /v1/files."""
    headers = {"Authorization": f"Bearer {api_key}"}
    response = requests.get(FILES_INDEX_URL, headers=headers, timeout=30)
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, dict) or not isinstance(payload.get("files"), list):
        raise ValueError("Unexpected /v1/files response structure.")

    partitions = [
        item
        for item in payload["files"]
        if isinstance(item, dict)
        and item.get("kind") == "measurements"
        and item.get("year") == TARGET_YEAR
    ]
    partitions.sort(key=lambda item: int(item["month"]))
    return partitions


def create_safe_duckdb_connection(api_key: str) -> duckdb.DuckDBPyConnection:
    """Reuse validated DuckDB connection setup, loading httpfs cleanly."""
    con = create_authenticated_duckdb_connection(api_key)
    return con


def fetch_month_rows(
    con: duckdb.DuckDBPyConnection, partition_key: str
) -> list[tuple[Any, ...]]:
    """Query the 3 target stations and 2 pollutants from one remote Parquet partition."""
    parquet_url = f"{FILES_BASE_URL}{partition_key}"
    query = f"""
    SELECT
        station_id,
        parameter_name,
        collected_at,
        value
    FROM read_parquet('{parquet_url}')
    WHERE station_id IN ('site_5106', 'site_5399', 'site_5102')
      AND parameter_name IN ('PM2.5', 'PM10')
    ORDER BY station_id, parameter_name, collected_at;
    """
    return con.execute(query).fetchall()


def verify_naive_ist(ts: datetime, station_id: str, pollutant: str) -> None:
    """Assert that collected_at is a naive IST datetime without timezone offset."""
    if not isinstance(ts, datetime):
        raise TypeError(f"Expected datetime for {station_id} {pollutant}, got {type(ts)}")
    if ts.tzinfo is not None:
        raise ValueError(
            f"Expected naive IST timestamp for {station_id} {pollutant}, got {ts.isoformat()}"
        )


def analyze_station_pollutant_month(
    rows: list[tuple[Any, ...]],
    station_id: str,
    station_name: str,
    pollutant: str,
    year: int,
    month: int,
) -> dict[str, Any]:
    """Compute availability, cadence, and gap metrics for one station-pollutant-month."""
    expected_hours = build_month_hours(year, month)
    expected_hour_count = len(expected_hours)
    month_start = expected_hours[0]
    month_end_exclusive = expected_hours[-1] + timedelta(hours=1)

    sub = []
    for r in rows:
        if r[0] == station_id and r[1] == pollutant:
            ts = r[2]
            verify_naive_ist(ts, station_id, pollutant)
            if month_start <= ts < month_end_exclusive:
                sub.append(r)

    raw_records = len(sub)
    valid_rows = [
        r for r in sub if isinstance(r[3], (int, float)) and not isinstance(r[3], bool)
    ]
    valid_records = len(valid_rows)
    null_records = raw_records - valid_records

    all_timestamps = [r[2] for r in sub]
    unique_timestamps = sorted(set(all_timestamps))
    duplicate_timestamps = raw_records - len(unique_timestamps)

    # Track hours with any raw row separately from hours with valid numeric values
    raw_hour_counts: dict[datetime, int] = {}
    valid_hourly_buckets: dict[datetime, list[float]] = {}
    for _, _, ts, val in sub:
        hr = ts.replace(minute=0, second=0, microsecond=0)
        raw_hour_counts[hr] = raw_hour_counts.get(hr, 0) + 1
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            valid_hourly_buckets.setdefault(hr, []).append(float(val))

    unique_raw_hours = len(raw_hour_counts)
    valid_observed_hours = set(valid_hourly_buckets.keys())
    unique_valid_hours = len(valid_observed_hours)

    if raw_hour_counts:
        min_obs_per_hour = min(raw_hour_counts.values())
        max_obs_per_hour = max(raw_hour_counts.values())
    else:
        min_obs_per_hour = None
        max_obs_per_hour = None

    if len(unique_timestamps) >= 2:
        deltas_min = sorted(
            int((unique_timestamps[i] - unique_timestamps[i - 1]).total_seconds() // 60)
            for i in range(1, len(unique_timestamps))
        )
        min_interval_min = deltas_min[0]
        median_interval_min = deltas_min[len(deltas_min) // 2]
        max_interval_min = deltas_min[-1]
    else:
        min_interval_min = None
        median_interval_min = None
        max_interval_min = None

    lead_gap, internal_gap, trail_gap, longest_gap = compute_longest_missing_streak(
        expected_hours, valid_observed_hours
    )

    first_obs = unique_timestamps[0] if unique_timestamps else None
    last_obs = unique_timestamps[-1] if unique_timestamps else None

    if valid_hourly_buckets:
        hourly_means = [sum(vals) / len(vals) for vals in valid_hourly_buckets.values()]
        mean_conc = sum(hourly_means) / len(hourly_means)
    else:
        mean_conc = None

    if raw_records == 0:
        status = "no_records"
    elif valid_records == 0:
        status = "null_only"
    elif unique_valid_hours == expected_hour_count:
        status = "active_complete"
    else:
        status = "active_partial"

    availability_pct = (
        unique_valid_hours / expected_hour_count * 100.0 if expected_hour_count else 0.0
    )

    return {
        "year": year,
        "month": month,
        "ym": f"{year:04d}-{month:02d}",
        "station_id": station_id,
        "station_name": station_name,
        "pollutant": pollutant,
        "expected_hours": expected_hour_count,
        "raw_records": raw_records,
        "valid_records": valid_records,
        "null_records": null_records,
        "unique_timestamps": len(unique_timestamps),
        "duplicate_timestamps": duplicate_timestamps,
        "unique_raw_hours": unique_raw_hours,
        "unique_valid_hours": unique_valid_hours,
        "availability_pct": availability_pct,
        "min_obs_per_hour": min_obs_per_hour,
        "max_obs_per_hour": max_obs_per_hour,
        "min_interval_min": min_interval_min,
        "median_interval_min": median_interval_min,
        "max_interval_min": max_interval_min,
        "leading_missing_hours": lead_gap if valid_observed_hours else None,
        "internal_missing_hours": internal_gap if valid_observed_hours else None,
        "trailing_missing_hours": trail_gap if valid_observed_hours else None,
        "longest_missing_hours": longest_gap,
        "first_observation": first_obs.strftime("%Y-%m-%d %H:%M") if first_obs else "N/A",
        "last_observation": last_obs.strftime("%Y-%m-%d %H:%M") if last_obs else "N/A",
        "mean_concentration": mean_conc,
        "status": status,
    }


def validate_august_2024(
    aug_rows: list[tuple[Any, ...]], aug_metrics: list[dict[str, Any]]
) -> list[str]:
    """Reconcile August 2024 metrics against validate_duckdb_month.py and API baseline."""
    issues: list[str] = []

    for m in aug_metrics:
        key = (m["station_id"], m["pollutant"])
        expected_aug_hrs = VALIDATED_AUG_2024_HOURS[key]
        if m["unique_valid_hours"] != expected_aug_hrs:
            issues.append(
                f"August full-month mismatch for {key}: "
                f"got {m['unique_valid_hours']}, expected {expected_aug_hrs}"
            )

    win_start = datetime(2024, 8, 24, 0, 0)
    win_end_excl = datetime(2024, 8, 31, 0, 0)

    for station_id, _ in STATIONS:
        for pollutant in POLLUTANTS:
            sub = [
                r
                for r in aug_rows
                if r[0] == station_id
                and r[1] == pollutant
                and win_start <= r[2] < win_end_excl
                and isinstance(r[3], (int, float))
                and not isinstance(r[3], bool)
            ]
            buckets: dict[datetime, list[float]] = {}
            for _, _, ts, val in sub:
                hr = ts.replace(minute=0, second=0, microsecond=0)
                buckets.setdefault(hr, []).append(float(val))
            hrs = len(buckets)
            mean_val = (
                sum(sum(v) / len(v) for v in buckets.values()) / hrs if hrs else None
            )
            api_hrs, api_mean = API_BASELINE[(station_id, pollutant)]
            if hrs != api_hrs:
                issues.append(
                    f"Aug 24-30 hours mismatch for ({station_id}, {pollutant}): "
                    f"got {hrs}, API baseline {api_hrs}"
                )
            if (mean_val is None) != (api_mean is None) or (
                mean_val is not None
                and api_mean is not None
                and abs(mean_val - api_mean) > 0.02
            ):
                issues.append(
                    f"Aug 24-30 mean mismatch for ({station_id}, {pollutant}): "
                    f"got {mean_val}, API baseline {api_mean}"
                )

    return issues


def save_outputs(
    partition_statuses: list[dict[str, Any]],
    metrics: list[dict[str, Any]],
    validation_issues: list[str],
) -> None:
    """Save CSV and JSON outputs under data/processed/duckdb_2024_pilot/."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = OUTPUT_DIR / "monthly_metrics_2024.csv"
    if metrics:
        fieldnames = list(metrics[0].keys())
        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in metrics:
                out_row = dict(row)
                out_row["availability_pct"] = round(row["availability_pct"], 2)
                if row["mean_concentration"] is not None:
                    out_row["mean_concentration"] = round(row["mean_concentration"], 2)
                writer.writerow(out_row)

    summary_path = OUTPUT_DIR / "summary_2024.json"
    summary_payload = {
        "year": TARGET_YEAR,
        "stations": [{"station_id": s, "station_name": n} for s, n in STATIONS],
        "pollutants": POLLUTANTS,
        "partitions_processed": partition_statuses,
        "validation_passed": len(validation_issues) == 0,
        "validation_issues": validation_issues,
        "zero_observation_months": [
            {
                "ym": m["ym"],
                "station_id": m["station_id"],
                "station_name": m["station_name"],
                "pollutant": m["pollutant"],
                "status": m["status"],
            }
            for m in metrics
            if m["raw_records"] == 0
        ],
    }
    with summary_path.open("w", encoding="utf-8") as fh:
        json.dump(summary_payload, fh, indent=2)


def print_reports(
    partition_statuses: list[dict[str, Any]],
    metrics: list[dict[str, Any]],
    validation_issues: list[str],
) -> None:
    """Print all required tables and summaries."""
    print("\n=== 1. MONTHLY PARTITION PROCESSING & RECONCILIATION ===")
    for p in partition_statuses:
        print(
            f"  {p['ym']}: status={p['status']}, archive_rows={p['archive_rows']:,}, "
            f"bytes={p['bytes']:,}, filtered_rows={p['filtered_rows']:,}"
        )

    print("\n=== 2. STATION-WISE MONTHLY AVAILABILITY (% OF CALENDAR HOURS WITH VALID VALUES) ===")
    months = [f"2024-{m:02d}" for m in range(1, 13)]
    lookup = {(m["station_id"], m["pollutant"], m["ym"]): m for m in metrics}
    header = f"{'Station':<28} | {'Pollutant':<9} | " + " | ".join(f"{m[5:]:>6}" for m in months) + f" | {'Mean':>7}"
    sep = "-" * len(header)
    print(sep)
    print(header)
    print(sep)
    for sid, sname in STATIONS:
        label = f"{sname} ({sid})"
        for pol in POLLUTANTS:
            vals = [lookup[(sid, pol, ym)]["availability_pct"] for ym in months]
            mean_v = sum(vals) / len(vals)
            cells = " | ".join(f"{v:>5.1f}%" for v in vals)
            print(f"{label:<28} | {pol:<9} | {cells} | {mean_v:>6.2f}%")
        print(sep)

    print("\n=== 3. POLLUTANT-WISE ANNUAL COMPARISON (8,784 CALENDAR HOURS IN 2024 LEAP YEAR) ===")
    for pol in POLLUTANTS:
        print(f"Pollutant: {pol}")
        for sid, sname in STATIONS:
            sub = [m for m in metrics if m["station_id"] == sid and m["pollutant"] == pol]
            tot_raw = sum(m["raw_records"] for m in sub)
            tot_valid = sum(m["valid_records"] for m in sub)
            tot_hrs = sum(m["unique_valid_hours"] for m in sub)
            tot_exp = sum(m["expected_hours"] for m in sub)
            zero_m = sum(1 for m in sub if m["raw_records"] == 0)
            pct = tot_hrs / tot_exp * 100.0
            print(
                f"  {sname} ({sid}): valid_hours={tot_hrs}/{tot_exp} ({pct:.2f}%), "
                f"raw={tot_raw}, valid={tot_valid}, zero_obs_months={zero_m}/12"
            )

    print("\n=== 4. MONTHLY RECORD-COUNT & CADENCE SUMMARY ===")
    for sid, sname in STATIONS:
        for pol in POLLUTANTS:
            sub = [m for m in metrics if m["station_id"] == sid and m["pollutant"] == pol]
            dups = sum(m["duplicate_timestamps"] for m in sub)
            nulls = sum(m["null_records"] for m in sub)
            max_oph = max((m["max_obs_per_hour"] for m in sub if m["max_obs_per_hour"] is not None), default=0)
            min_gap = min((m["min_interval_min"] for m in sub if m["min_interval_min"] is not None), default=None)
            med_gaps = sorted(m["median_interval_min"] for m in sub if m["median_interval_min"] is not None)
            max_gap = max((m["max_interval_min"] for m in sub if m["max_interval_min"] is not None), default=None)
            max_missing_streak = max(m["longest_missing_hours"] for m in sub)
            print(
                f"  {sname} ({sid}) {pol}: nulls={nulls}, dups={dups}, "
                f"max_obs_per_hr={max_oph}, min_int_min={min_gap}, "
                f"median_ints_min={set(med_gaps) if med_gaps else 'N/A'}, "
                f"max_int_min={max_gap}, longest_single_month_gap_hr={max_missing_streak}"
            )

    print("\n=== 5. ZERO-OBSERVATION MONTHS ===")
    zeros = [m for m in metrics if m["raw_records"] == 0]
    if not zeros:
        print("  None.")
    else:
        for m in zeros:
            print(f"  {m['ym']} | {m['station_name']} ({m['station_id']}) | {m['pollutant']} | status={m['status']}")

    print("\n=== 6. VALIDATION CHECKS ===")
    if not validation_issues:
        print("  ALL VALIDATION CHECKS PASSED (August 2024 full-month & Aug 24-30 API baseline matched).")
    else:
        for issue in validation_issues:
            print(f"  DISCREPANCY: {issue}")


def main() -> None:
    """Run the 2024 annual historical data-quality pilot."""
    try:
        api_key = load_api_key()
        partitions = fetch_2024_partition_metadata(api_key)
        if len(partitions) != 12:
            raise ValueError(f"Expected 12 monthly partitions for 2024, found {len(partitions)}.")

        con = create_safe_duckdb_connection(api_key)
        partition_statuses: list[dict[str, Any]] = []
        all_metrics: list[dict[str, Any]] = []
        aug_rows: list[tuple[Any, ...]] = []
        aug_metrics: list[dict[str, Any]] = []

        print("Running 2024 annual DuckDB remote Parquet pilot (12 monthly partitions)...")
        for part in partitions:
            year = int(part["year"])
            month = int(part["month"])
            ym = f"{year:04d}-{month:02d}"
            rows = fetch_month_rows(con, part["key"])

            partition_statuses.append(
                {
                    "ym": ym,
                    "key": part["key"],
                    "archive_rows": int(part["rows"]),
                    "bytes": int(part["bytes"]),
                    "filtered_rows": len(rows),
                    "status": "ok",
                }
            )
            print(f"  Processed {ym}: filtered_rows={len(rows):,} (archive_rows={int(part['rows']):,})")

            month_metrics = []
            for station_id, station_name in STATIONS:
                for pollutant in POLLUTANTS:
                    m = analyze_station_pollutant_month(
                        rows=rows,
                        station_id=station_id,
                        station_name=station_name,
                        pollutant=pollutant,
                        year=year,
                        month=month,
                    )
                    all_metrics.append(m)
                    month_metrics.append(m)

            if month == 8:
                aug_rows = rows
                aug_metrics = month_metrics

        validation_issues = validate_august_2024(aug_rows, aug_metrics)
        save_outputs(partition_statuses, all_metrics, validation_issues)
        print_reports(partition_statuses, all_metrics, validation_issues)
        print(f"\nSaved outputs to {OUTPUT_DIR}/")

    except requests.exceptions.HTTPError as error:
        status_code = error.response.status_code if error.response is not None else "unknown"
        print(f"HTTP error (status {status_code}): {error}")

    except requests.exceptions.RequestException as error:
        print(f"Network error: {error}")

    except duckdb.Error as error:
        print(f"DuckDB query error: {error}")

    except (ValueError, KeyError, TypeError) as error:
        print(f"Configuration or data error: {error}")


if __name__ == "__main__":
    main()
