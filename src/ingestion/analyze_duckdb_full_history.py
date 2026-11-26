import csv
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import requests

try:
    from src.ingestion.analyze_duckdb_2024 import (
        FILES_INDEX_URL,
        analyze_station_pollutant_month,
        fetch_month_rows,
    )
    from src.ingestion.validate_duckdb_month import (
        POLLUTANTS,
        STATIONS,
        build_month_hours,
        create_authenticated_duckdb_connection,
        load_api_key,
    )
except ImportError:
    from analyze_duckdb_2024 import (
        FILES_INDEX_URL,
        analyze_station_pollutant_month,
        fetch_month_rows,
    )
    from validate_duckdb_month import (
        POLLUTANTS,
        STATIONS,
        build_month_hours,
        create_authenticated_duckdb_connection,
        load_api_key,
    )


START_YM = (2019, 6)
END_YM = (2026, 11)
OUTPUT_DIR = Path("data/processed/duckdb_full_history")
CHECKPOINT_DIR = OUTPUT_DIR / "checkpoints"
MANIFEST_PATH = OUTPUT_DIR / "processing_manifest.json"
PILOT_2024_CSV = Path("data/processed/duckdb_2024_pilot/monthly_metrics_2024.csv")


def generate_expected_months(
    start_ym: tuple[int, int], end_ym: tuple[int, int]
) -> list[tuple[int, int]]:
    """Generate ordered (year, month) tuples from start_ym through end_ym inclusive."""
    months: list[tuple[int, int]] = []
    y, m = start_ym
    while (y, m) <= end_ym:
        months.append((y, m))
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return months


def fetch_partition_index(api_key: str) -> dict[tuple[int, int], dict[str, Any]]:
    """Fetch available measurement Parquet partitions from /v1/files."""
    headers = {"Authorization": f"Bearer {api_key}"}
    response = requests.get(FILES_INDEX_URL, headers=headers, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or not isinstance(payload.get("files"), list):
        raise ValueError("Unexpected /v1/files response structure.")

    index: dict[tuple[int, int], dict[str, Any]] = {}
    for item in payload["files"]:
        if isinstance(item, dict) and item.get("kind") == "measurements":
            index[(int(item["year"]), int(item["month"]))] = item
    return index


def load_manifest() -> dict[str, Any]:
    """Load existing processing manifest or initialize a new one."""
    if MANIFEST_PATH.exists():
        with MANIFEST_PATH.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
            if isinstance(data, dict) and isinstance(data.get("partitions"), dict):
                return data
    return {
        "start_ym": f"{START_YM[0]:04d}-{START_YM[1]:02d}",
        "end_ym": f"{END_YM[0]:04d}-{END_YM[1]:02d}",
        "partitions": {},
    }


def save_manifest(manifest: dict[str, Any]) -> None:
    """Persist processing manifest atomically."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with MANIFEST_PATH.open("w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)


def checkpoint_path_for(ym: str) -> Path:
    return CHECKPOINT_DIR / f"{ym}.json"


def save_month_checkpoint(
    ym: str,
    month_metrics: list[dict[str, Any]],
    valid_hours_map: dict[tuple[str, str], list[str]],
) -> None:
    """Save one month's metrics and valid observed hours for resumability."""
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "ym": ym,
        "metrics": month_metrics,
        "valid_hours": {
            f"{sid}|{pol}": hours for (sid, pol), hours in valid_hours_map.items()
        },
    }
    with checkpoint_path_for(ym).open("w", encoding="utf-8") as fh:
        json.dump(payload, fh)


def extract_valid_hours_by_pair(
    rows: list[tuple[Any, ...]], year: int, month: int
) -> dict[tuple[str, str], list[str]]:
    """Extract sorted unique valid-value hourly ISO strings per (station_id, pollutant)."""
    expected_hours = build_month_hours(year, month)
    start_dt = expected_hours[0]
    end_excl = expected_hours[-1] + timedelta(hours=1)

    buckets: dict[tuple[str, str], set[datetime]] = {
        (sid, pol): set() for sid, _ in STATIONS for pol in POLLUTANTS
    }
    for sid, pol, ts, val in rows:
        if (sid, pol) in buckets and start_dt <= ts < end_excl:
            if isinstance(val, (int, float)) and not isinstance(val, bool):
                buckets[(sid, pol)].add(ts.replace(minute=0, second=0, microsecond=0))

    return {
        pair: [dt.strftime("%Y-%m-%dT%H:%M:%S") for dt in sorted(dts)]
        for pair, dts in buckets.items()
    }


def find_longest_gap(
    start_dt: datetime, end_incl: datetime, observed_hours: set[datetime]
) -> dict[str, Any]:
    """Find the longest consecutive missing-hour streak between start_dt and end_incl."""
    longest_len = 0
    longest_start: datetime | None = None
    longest_end: datetime | None = None

    cur_len = 0
    cur_start: datetime | None = None

    cur = start_dt
    while cur <= end_incl:
        if cur not in observed_hours:
            if cur_len == 0:
                cur_start = cur
            cur_len += 1
            if cur_len > longest_len:
                longest_len = cur_len
                longest_start = cur_start
                longest_end = cur
        else:
            cur_len = 0
            cur_start = None
        cur += timedelta(hours=1)

    return {
        "gap_hours": longest_len,
        "gap_start": longest_start.strftime("%Y-%m-%d %H:%M") if longest_start else "N/A",
        "gap_end": longest_end.strftime("%Y-%m-%d %H:%M") if longest_end else "N/A",
    }


def compute_cross_month_continuity(
    expected_months: list[tuple[int, int]],
    all_valid_hours: dict[tuple[str, str], set[datetime]],
    all_metrics: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Compute cross-month gaps and station-pollutant historical summaries."""
    first_month_hours = build_month_hours(*expected_months[0])
    last_month_hours = build_month_hours(*expected_months[-1])
    window_start = first_month_hours[0]
    window_end = last_month_hours[-1]
    total_window_hours = int((window_end - window_start).total_seconds() // 3600) + 1

    gap_rows: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []

    for sid, sname in STATIONS:
        for pol in POLLUTANTS:
            obs = all_valid_hours[(sid, pol)]
            sub_metrics = [
                m for m in all_metrics if m["station_id"] == sid and m["pollutant"] == pol
            ]
            total_raw = sum(m["raw_records"] for m in sub_metrics)
            total_valid = sum(m["valid_records"] for m in sub_metrics)
            total_null = sum(m["null_records"] for m in sub_metrics)
            total_dups = sum(m["duplicate_timestamps"] for m in sub_metrics)
            zero_months = sum(1 for m in sub_metrics if m["raw_records"] == 0)
            null_only_months = sum(1 for m in sub_metrics if m["status"] == "null_only")

            full_gap = find_longest_gap(window_start, window_end, obs)

            if obs:
                first_obs = min(obs)
                last_obs = max(obs)
                active_span_hours = int((last_obs - first_obs).total_seconds() // 3600) + 1
                active_avail_pct = len(obs) / active_span_hours * 100.0
                internal_gap = find_longest_gap(first_obs, last_obs, obs)
                first_str = first_obs.strftime("%Y-%m-%d %H:%M")
                last_str = last_obs.strftime("%Y-%m-%d %H:%M")
            else:
                active_span_hours = 0
                active_avail_pct = 0.0
                internal_gap = {"gap_hours": 0, "gap_start": "N/A", "gap_end": "N/A"}
                first_str = "N/A"
                last_str = "N/A"

            full_avail_pct = len(obs) / total_window_hours * 100.0

            summary_rows.append(
                {
                    "station_id": sid,
                    "station_name": sname,
                    "pollutant": pol,
                    "full_window_hours": total_window_hours,
                    "observed_valid_hours": len(obs),
                    "full_window_availability_pct": round(full_avail_pct, 2),
                    "first_observed_hour": first_str,
                    "last_observed_hour": last_str,
                    "active_span_hours": active_span_hours,
                    "active_period_availability_pct": round(active_avail_pct, 2),
                    "total_raw_records": total_raw,
                    "total_valid_records": total_valid,
                    "total_null_records": total_null,
                    "total_duplicate_timestamps": total_dups,
                    "zero_record_months": zero_months,
                    "null_only_months": null_only_months,
                }
            )

            gap_rows.append(
                {
                    "station_id": sid,
                    "station_name": sname,
                    "pollutant": pol,
                    "full_window_longest_gap_hours": full_gap["gap_hours"],
                    "full_window_gap_start": full_gap["gap_start"],
                    "full_window_gap_end": full_gap["gap_end"],
                    "active_internal_longest_gap_hours": internal_gap["gap_hours"],
                    "active_internal_gap_start": internal_gap["gap_start"],
                    "active_internal_gap_end": internal_gap["gap_end"],
                }
            )

    return gap_rows, summary_rows


def validate_against_2024_pilot(all_metrics: list[dict[str, Any]]) -> list[str]:
    """Validate 2024 subset against data/processed/duckdb_2024_pilot/monthly_metrics_2024.csv."""
    if not PILOT_2024_CSV.exists():
        return [f"2024 pilot CSV not found at {PILOT_2024_CSV}"]

    with PILOT_2024_CSV.open("r", encoding="utf-8") as fh:
        pilot_rows = list(csv.DictReader(fh))

    pilot_lookup = {
        (r["ym"], r["station_id"], r["pollutant"]): r for r in pilot_rows
    }
    full_2024 = [m for m in all_metrics if m["year"] == 2024]

    issues: list[str] = []
    if len(full_2024) != len(pilot_rows):
        issues.append(
            f"2024 row count mismatch: full={len(full_2024)} vs pilot={len(pilot_rows)}"
        )

    for m in full_2024:
        key = (m["ym"], m["station_id"], m["pollutant"])
        p = pilot_lookup.get(key)
        if not p:
            issues.append(f"Missing 2024 pilot row for {key}")
            continue
        for field in ("raw_records", "valid_records", "null_records", "unique_valid_hours", "duplicate_timestamps"):
            if int(m[field]) != int(p[field]):
                issues.append(
                    f"2024 mismatch for {key} field {field}: full={m[field]} vs pilot={p[field]}"
                )
    return issues


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            out = dict(r)
            if "availability_pct" in out and isinstance(out["availability_pct"], float):
                out["availability_pct"] = round(out["availability_pct"], 2)
            if "mean_concentration" in out and isinstance(out["mean_concentration"], float):
                out["mean_concentration"] = round(out["mean_concentration"], 2)
            writer.writerow(out)


def main() -> None:
    """Run resumable full historical continuity analysis (2019-06 to 2026-11)."""
    try:
        api_key = load_api_key()
        expected_months = generate_expected_months(START_YM, END_YM)
        partition_index = fetch_partition_index(api_key)
        manifest = load_manifest()
        con = create_authenticated_duckdb_connection(api_key)

        print(
            f"Full history continuity analysis: {len(expected_months)} expected months "
            f"({START_YM[0]:04d}-{START_YM[1]:02d} to {END_YM[0]:04d}-{END_YM[1]:02d})"
        )

        for year, month in expected_months:
            ym = f"{year:04d}-{month:02d}"
            existing = manifest["partitions"].get(ym)
            if existing and existing.get("status") == "ok" and checkpoint_path_for(ym).exists():
                continue

            part_meta = partition_index.get((year, month))
            if not part_meta:
                manifest["partitions"][ym] = {
                    "ym": ym,
                    "status": "unavailable",
                    "error": "Partition not listed in /v1/files",
                }
                save_manifest(manifest)
                print(f"  {ym}: UNAVAILABLE (not in /v1/files)")
                continue

            try:
                rows = fetch_month_rows(con, part_meta["key"])
                month_metrics = [
                    analyze_station_pollutant_month(
                        rows=rows,
                        station_id=sid,
                        station_name=sname,
                        pollutant=pol,
                        year=year,
                        month=month,
                    )
                    for sid, sname in STATIONS
                    for pol in POLLUTANTS
                ]
                valid_hours_map = extract_valid_hours_by_pair(rows, year, month)
                save_month_checkpoint(ym, month_metrics, valid_hours_map)
                manifest["partitions"][ym] = {
                    "ym": ym,
                    "key": part_meta["key"],
                    "archive_rows": int(part_meta["rows"]),
                    "bytes": int(part_meta["bytes"]),
                    "filtered_rows": len(rows),
                    "status": "ok",
                }
                save_manifest(manifest)
                print(f"  {ym}: OK (filtered_rows={len(rows):,})")

            except (duckdb.Error, ValueError, KeyError, TypeError) as err:
                manifest["partitions"][ym] = {
                    "ym": ym,
                    "key": part_meta["key"],
                    "status": "failed",
                    "error": str(err),
                }
                save_manifest(manifest)
                print(f"  {ym}: FAILED ({err})")

        # Aggregate all completed checkpoints in chronological order
        all_metrics: list[dict[str, Any]] = []
        all_valid_hours: dict[tuple[str, str], set[datetime]] = {
            (sid, pol): set() for sid, _ in STATIONS for pol in POLLUTANTS
        }
        failed_parts = [
            p for p in manifest["partitions"].values() if p.get("status") == "failed"
        ]
        unavailable_parts = [
            p for p in manifest["partitions"].values() if p.get("status") == "unavailable"
        ]
        ok_parts = [
            p for p in manifest["partitions"].values() if p.get("status") == "ok"
        ]

        for year, month in expected_months:
            ym = f"{year:04d}-{month:02d}"
            cp = checkpoint_path_for(ym)
            if cp.exists():
                with cp.open("r", encoding="utf-8") as fh:
                    data = json.load(fh)
                all_metrics.extend(data["metrics"])
                for pair_key, iso_list in data["valid_hours"].items():
                    sid, pol = pair_key.split("|", 1)
                    all_valid_hours[(sid, pol)].update(
                        datetime.fromisoformat(s) for s in iso_list
                    )

        gap_rows, summary_rows = compute_cross_month_continuity(
            expected_months, all_valid_hours, all_metrics
        )
        validation_issues = validate_against_2024_pilot(all_metrics)

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        write_csv(OUTPUT_DIR / "monthly_metrics.csv", all_metrics)
        write_csv(OUTPUT_DIR / "historical_gaps.csv", gap_rows)
        write_csv(OUTPUT_DIR / "station_pollutant_summary.csv", summary_rows)

        total_filtered = sum(p.get("filtered_rows", 0) for p in ok_parts)
        summary_json = {
            "window": f"{START_YM[0]:04d}-{START_YM[1]:02d} to {END_YM[0]:04d}-{END_YM[1]:02d}",
            "expected_months": len(expected_months),
            "partitions_ok": len(ok_parts),
            "partitions_failed": len(failed_parts),
            "partitions_unavailable": len(unavailable_parts),
            "total_filtered_records": total_filtered,
            "validation_passed": len(validation_issues) == 0,
            "validation_issues": validation_issues,
            "station_pollutant_summary": summary_rows,
            "historical_gaps": gap_rows,
        }
        with (OUTPUT_DIR / "summary.json").open("w", encoding="utf-8") as fh:
            json.dump(summary_json, fh, indent=2)

        print("\n=== FULL HISTORY SUMMARY ===")
        print(
            f"Partitions: ok={len(ok_parts)}, failed={len(failed_parts)}, "
            f"unavailable={len(unavailable_parts)} | total_filtered_records={total_filtered:,}"
        )
        print("Validation against 2024 pilot:", "PASSED" if not validation_issues else f"ISSUES: {validation_issues}")
        for s in summary_rows:
            print(
                f"  {s['station_name']} ({s['station_id']}) {s['pollutant']}: "
                f"full={s['full_window_availability_pct']}% ({s['observed_valid_hours']}/{s['full_window_hours']}), "
                f"active={s['active_period_availability_pct']}% ({s['first_observed_hour']}..{s['last_observed_hour']}), "
                f"zero_months={s['zero_record_months']}, nulls={s['total_null_records']}, dups={s['total_duplicate_timestamps']}"
            )
        print("\n=== CROSS-MONTH LONGEST GAPS ===")
        for g in gap_rows:
            print(
                f"  {g['station_name']} ({g['station_id']}) {g['pollutant']}: "
                f"full_max={g['full_window_longest_gap_hours']}h ({g['full_window_gap_start']}..{g['full_window_gap_end']}), "
                f"active_internal_max={g['active_internal_longest_gap_hours']}h ({g['active_internal_gap_start']}..{g['active_internal_gap_end']})"
            )

    except requests.exceptions.HTTPError as err:
        status_code = err.response.status_code if err.response is not None else "unknown"
        print(f"HTTP error (status {status_code}): {err}")
    except requests.exceptions.RequestException as err:
        print(f"Network error: {err}")
    except duckdb.Error as err:
        print(f"DuckDB error: {err}")
    except (ValueError, KeyError, TypeError) as err:
        print(f"Configuration or data error: {err}")


if __name__ == "__main__":
    main()
