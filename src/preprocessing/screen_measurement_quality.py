import csv
import json
import math
import os
import statistics
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
from dotenv import load_dotenv


MANIFEST_PATH = Path("data/processed/duckdb_full_history/processing_manifest.json")
OUTPUT_DIR = Path("data/processed/measurement_quality")
LOCAL_CACHE_PARQUET = OUTPUT_DIR / "filtered_measurements_2019_2026.parquet"
FILES_BASE_URL = "https://airquality.xkdr.org/v1/files/"

STATIONS = [
    ("site_5106", "Vile Parle West"),
    ("site_5399", "Chakala-Andheri East"),
    ("site_5102", "Malad West"),
]
POLLUTANTS = ["PM2.5", "PM10"]

# Season mapping for Mumbai (meteorological context)
MONTH_TO_SEASON = {
    12: "Winter",
    1: "Winter",
    2: "Winter",
    3: "Pre-Monsoon",
    4: "Pre-Monsoon",
    5: "Pre-Monsoon",
    6: "Monsoon",
    7: "Monsoon",
    8: "Monsoon",
    9: "Monsoon",
    10: "Post-Monsoon",
    11: "Post-Monsoon",
}


def load_api_key() -> str:
    """Load XKDR_API_KEY from .env without printing or exposing it."""
    load_dotenv()
    api_key = os.getenv("XKDR_API_KEY")
    if not api_key:
        raise ValueError("XKDR_API_KEY not found. Check your .env file.")
    return api_key


def load_or_fetch_measurements() -> list[tuple[str, str, datetime, float | None]]:
    """
    Load the 234,645 filtered rows from local cache if present; otherwise query
    only the non-empty partitions listed in processing_manifest.json via DuckDB httpfs.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if LOCAL_CACHE_PARQUET.exists():
        con = duckdb.connect()
        rows = con.execute(
            f"""
            SELECT station_id, parameter_name, collected_at, value
            FROM read_parquet('{LOCAL_CACHE_PARQUET.as_posix()}')
            ORDER BY station_id, parameter_name, collected_at
            """
        ).fetchall()
        return rows

    if not MANIFEST_PATH.exists():
        raise FileNotFoundError(
            f"Processing manifest not found at {MANIFEST_PATH}. Run full-history analysis first."
        )

    with MANIFEST_PATH.open("r", encoding="utf-8") as fh:
        manifest = json.load(fh)

    urls = [
        f"{FILES_BASE_URL}{part['key']}"
        for part in manifest["partitions"].values()
        if part.get("status") == "ok" and int(part.get("filtered_rows", 0)) > 0
    ]

    api_key = load_api_key()
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    escaped_key = api_key.replace("'", "''")
    con.execute(
        f"""
        CREATE OR REPLACE SECRET xkdr_http (
            TYPE HTTP,
            SCOPE 'https://airquality.xkdr.org/v1/files/',
            EXTRA_HTTP_HEADERS MAP {{'Authorization': 'Bearer {escaped_key}'}}
        );
        """
    )

    url_list_sql = ", ".join(f"'{u}'" for u in sorted(urls))
    con.execute(
        f"""
        COPY (
            SELECT station_id, parameter_name, collected_at, value
            FROM read_parquet([{url_list_sql}])
            WHERE station_id IN ('site_5106', 'site_5399', 'site_5102')
              AND parameter_name IN ('PM2.5', 'PM10')
            ORDER BY station_id, parameter_name, collected_at
        ) TO '{LOCAL_CACHE_PARQUET.as_posix()}' (FORMAT PARQUET);
        """
    )

    rows = con.execute(
        f"""
        SELECT station_id, parameter_name, collected_at, value
        FROM read_parquet('{LOCAL_CACHE_PARQUET.as_posix()}')
        ORDER BY station_id, parameter_name, collected_at
        """
    ).fetchall()
    return rows


def percentile(sorted_vals: list[float], p: float) -> float:
    """Compute linear-interpolated percentile p in [0, 100] from a sorted list."""
    if not sorted_vals:
        raise ValueError("Cannot compute percentile of empty list.")
    n = len(sorted_vals)
    if n == 1:
        return sorted_vals[0]
    idx = (p / 100.0) * (n - 1)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    frac = idx - lo
    return sorted_vals[lo] + frac * (sorted_vals[hi] - sorted_vals[lo])


def compute_mad(sorted_vals: list[float], med: float) -> float:
    """Compute Median Absolute Deviation (MAD)."""
    abs_devs = sorted(abs(v - med) for v in sorted_vals)
    return percentile(abs_devs, 50.0)


def analyze_series(
    sid: str,
    sname: str,
    pol: str,
    series: list[tuple[datetime, float | None]],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Run basic stats, extreme-value screening, and flatline detection for one series."""
    total_obs = len(series)
    valid_pairs = [
        (ts, float(val))
        for ts, val in series
        if isinstance(val, (int, float)) and not isinstance(val, bool)
    ]
    null_count = total_obs - len(valid_pairs)
    vals = [v for _, v in valid_pairs]
    sorted_vals = sorted(vals)

    neg_count = sum(1 for v in vals if v < 0)
    zero_count = sum(1 for v in vals if v == 0)
    unique_count = len(set(vals))

    mean_v = statistics.fmean(vals)
    med_v = percentile(sorted_vals, 50.0)
    std_v = statistics.stdev(vals) if len(vals) >= 2 else 0.0
    p1 = percentile(sorted_vals, 1.0)
    p5 = percentile(sorted_vals, 5.0)
    p25 = percentile(sorted_vals, 25.0)
    p75 = percentile(sorted_vals, 75.0)
    p95 = percentile(sorted_vals, 95.0)
    p99 = percentile(sorted_vals, 99.0)
    iqr = p75 - p25
    mad = compute_mad(sorted_vals, med_v)

    upper_iqr_15 = p75 + 1.5 * iqr
    upper_iqr_30 = p75 + 3.0 * iqr

    iqr_15_high = 0
    iqr_30_high = 0
    robust_z_high = 0
    anomalies: list[dict[str, Any]] = []

    for ts, v in valid_pairs:
        rz = (0.6745 * (v - med_v) / mad) if mad > 0 else 0.0
        if v < 0:
            anomalies.append(
                {
                    "station_id": sid,
                    "station_name": sname,
                    "pollutant": pol,
                    "timestamp": ts.strftime("%Y-%m-%d %H:%M"),
                    "value": v,
                    "prev_value": "",
                    "delta_1h": "",
                    "flag_type": "confirmed_invalid_negative",
                    "robust_z": round(rz, 2),
                }
            )
        if v > upper_iqr_15:
            iqr_15_high += 1
        if v > upper_iqr_30:
            iqr_30_high += 1
        if rz > 3.5:
            robust_z_high += 1

    # Consecutive 1-hour step changes (do not compare across missing-hour gaps)
    step_deltas: list[tuple[datetime, float, float, float]] = []
    for i in range(1, len(valid_pairs)):
        ts_prev, v_prev = valid_pairs[i - 1]
        ts_curr, v_curr = valid_pairs[i]
        if (ts_curr - ts_prev) == timedelta(hours=1):
            delta = v_curr - v_prev
            step_deltas.append((ts_curr, v_curr, v_prev, delta))

    abs_deltas = sorted(abs(d[3]) for d in step_deltas)
    med_abs_delta = percentile(abs_deltas, 50.0) if abs_deltas else 0.0
    mad_abs_delta = compute_mad(abs_deltas, med_abs_delta) if abs_deltas else 0.0
    p75_delta = percentile(abs_deltas, 75.0) if abs_deltas else 0.0
    p25_delta = percentile(abs_deltas, 25.0) if abs_deltas else 0.0
    delta_iqr_thresh = p75_delta + 3.0 * (p75_delta - p25_delta)
    delta_mad_thresh = med_abs_delta + 5.0 * mad_abs_delta if mad_abs_delta > 0 else delta_iqr_thresh
    jump_thresh = max(delta_iqr_thresh, delta_mad_thresh)

    spike_1h_count = 0
    drop_1h_count = 0
    for ts_curr, v_curr, v_prev, delta in step_deltas:
        if abs(delta) > jump_thresh:
            if delta > 0:
                spike_1h_count += 1
                ftype = "suspicious_1h_spike"
            else:
                drop_1h_count += 1
                ftype = "suspicious_1h_drop"
            anomalies.append(
                {
                    "station_id": sid,
                    "station_name": sname,
                    "pollutant": pol,
                    "timestamp": ts_curr.strftime("%Y-%m-%d %H:%M"),
                    "value": round(v_curr, 2),
                    "prev_value": round(v_prev, 2),
                    "delta_1h": round(delta, 2),
                    "flag_type": ftype,
                    "robust_z": round((0.6745 * (v_curr - med_v) / mad) if mad > 0 else 0.0, 2),
                }
            )

    # Top 10 highest extreme values for inspection
    top_extremes = sorted(valid_pairs, key=lambda x: x[1], reverse=True)[:10]
    for ts, v in top_extremes:
        rz = (0.6745 * (v - med_v) / mad) if mad > 0 else 0.0
        anomalies.append(
            {
                "station_id": sid,
                "station_name": sname,
                "pollutant": pol,
                "timestamp": ts.strftime("%Y-%m-%d %H:%M"),
                "value": round(v, 2),
                "prev_value": "",
                "delta_1h": "",
                "flag_type": "top_extreme_high",
                "robust_z": round(rz, 2),
            }
        )

    # Flatline runs across consecutive 1-hour steps
    flatlines: list[dict[str, Any]] = []
    if valid_pairs:
        run_start_ts, run_val = valid_pairs[0]
        run_len = 1
        for i in range(1, len(valid_pairs)):
            ts_prev, _ = valid_pairs[i - 1]
            ts_curr, v_curr = valid_pairs[i]
            if (ts_curr - ts_prev) == timedelta(hours=1) and v_curr == run_val:
                run_len += 1
            else:
                if run_len >= 2:
                    flatlines.append(
                        {
                            "station_id": sid,
                            "station_name": sname,
                            "pollutant": pol,
                            "repeated_value": round(run_val, 2),
                            "run_length": run_len,
                            "duration_hours": run_len,
                            "start_timestamp": run_start_ts.strftime("%Y-%m-%d %H:%M"),
                            "end_timestamp": ts_prev.strftime("%Y-%m-%d %H:%M"),
                            "category": "ordinary_short_repeat" if run_len <= 3 else "long_flatline_ge4h",
                        }
                    )
                run_start_ts, run_val = ts_curr, v_curr
                run_len = 1
        if run_len >= 2:
            flatlines.append(
                {
                    "station_id": sid,
                    "station_name": sname,
                    "pollutant": pol,
                    "repeated_value": round(run_val, 2),
                    "run_length": run_len,
                    "duration_hours": run_len,
                    "start_timestamp": run_start_ts.strftime("%Y-%m-%d %H:%M"),
                    "end_timestamp": valid_pairs[-1][0].strftime("%Y-%m-%d %H:%M"),
                    "category": "ordinary_short_repeat" if run_len <= 3 else "long_flatline_ge4h",
                }
            )

    summary = {
        "station_id": sid,
        "station_name": sname,
        "pollutant": pol,
        "total_observations": total_obs,
        "null_values": null_count,
        "negative_values": neg_count,
        "zero_values": zero_count,
        "unique_values": unique_count,
        "min": round(sorted_vals[0], 2),
        "max": round(sorted_vals[-1], 2),
        "mean": round(mean_v, 2),
        "median": round(med_v, 2),
        "std_dev": round(std_v, 2),
        "p1": round(p1, 2),
        "p5": round(p5, 2),
        "p25": round(p25, 2),
        "p75": round(p75, 2),
        "p95": round(p95, 2),
        "p99": round(p99, 2),
        "iqr_1_5_upper_fence": round(upper_iqr_15, 2),
        "iqr_3_0_upper_fence": round(upper_iqr_30, 2),
        "count_above_iqr_1_5": iqr_15_high,
        "count_above_iqr_3_0": iqr_30_high,
        "count_robust_z_gt_3_5": robust_z_high,
        "step_1h_jump_threshold": round(jump_thresh, 2),
        "count_1h_spikes": spike_1h_count,
        "count_1h_drops": drop_1h_count,
        "flatline_runs_ge_2h": len(flatlines),
        "flatline_runs_ge_4h": sum(1 for f in flatlines if f["run_length"] >= 4),
        "flatline_runs_ge_6h": sum(1 for f in flatlines if f["run_length"] >= 6),
        "max_flatline_hours": max((f["run_length"] for f in flatlines), default=0),
    }

    return summary, anomalies, flatlines


def build_temporal_summary(
    rows: list[tuple[str, str, datetime, float | None]]
) -> list[dict[str, Any]]:
    """Compute hour-of-day, day-of-week, seasonal, and annual summaries."""
    buckets: dict[tuple[str, str, str, str], list[float]] = {}
    for sid, pol, ts, val in rows:
        if not isinstance(val, (int, float)) or isinstance(val, bool):
            continue
        v = float(val)
        groups = [
            ("hour_of_day", f"{ts.hour:02d}"),
            ("day_of_week", ts.strftime("%a")),
            ("season", MONTH_TO_SEASON[ts.month]),
            ("year", f"{ts.year:04d}"),
            ("year_month", f"{ts.year:04d}-{ts.month:02d}"),
        ]
        for gtype, gval in groups:
            buckets.setdefault((sid, pol, gtype, gval), []).append(v)

    temporal_rows: list[dict[str, Any]] = []
    for (sid, pol, gtype, gval), vals in sorted(buckets.items()):
        svals = sorted(vals)
        temporal_rows.append(
            {
                "station_id": sid,
                "pollutant": pol,
                "group_type": gtype,
                "group_value": gval,
                "count": len(vals),
                "zero_count": sum(1 for x in vals if x == 0),
                "mean": round(statistics.fmean(vals), 2),
                "median": round(percentile(svals, 50.0), 2),
                "p95": round(percentile(svals, 95.0), 2),
            }
        )
    return temporal_rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    rows = load_or_fetch_measurements()
    print(f"Loaded {len(rows):,} filtered measurement rows.")

    summaries: list[dict[str, Any]] = []
    all_anomalies: list[dict[str, Any]] = []
    all_flatlines: list[dict[str, Any]] = []

    for sid, sname in STATIONS:
        for pol in POLLUTANTS:
            series = [(ts, val) for s, p, ts, val in rows if s == sid and p == pol]
            summary, anomalies, flatlines = analyze_series(sid, sname, pol, series)
            summaries.append(summary)
            all_anomalies.extend(anomalies)
            all_flatlines.extend(flatlines)

    temporal_rows = build_temporal_summary(rows)

    # Save flatlines >= 3h (to keep CSV focused on notable runs, while counts >=2h are in summary)
    notable_flatlines = sorted(
        [f for f in all_flatlines if f["run_length"] >= 3],
        key=lambda x: (-x["run_length"], x["station_id"], x["pollutant"]),
    )

    write_csv(OUTPUT_DIR / "measurement_summary.csv", summaries)
    write_csv(OUTPUT_DIR / "anomaly_flags.csv", all_anomalies)
    write_csv(OUTPUT_DIR / "flatline_summary.csv", notable_flatlines)
    write_csv(OUTPUT_DIR / "temporal_summary.csv", temporal_rows)

    report = {
        "total_rows_screened": len(rows),
        "measurement_summary": summaries,
        "top_long_flatlines": notable_flatlines[:25],
    }
    with (OUTPUT_DIR / "quality_report.json").open("w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)

    print("\n=== MEASUREMENT DISTRIBUTION SUMMARY ===")
    for s in summaries:
        print(
            f"  {s['station_name']} ({s['station_id']}) {s['pollutant']}: "
            f"n={s['total_observations']:,}, neg={s['negative_values']}, zero={s['zero_values']}, "
            f"min={s['min']}, med={s['median']}, mean={s['mean']}, p95={s['p95']}, p99={s['p99']}, max={s['max']}"
        )
    print("\n=== ANOMALY & FLATLINE SCREENING COUNTS ===")
    for s in summaries:
        print(
            f"  {s['station_name']} ({s['station_id']}) {s['pollutant']}: "
            f">IQR1.5={s['count_above_iqr_1_5']}, >IQR3.0={s['count_above_iqr_3_0']}, "
            f"robustZ>3.5={s['count_robust_z_gt_3_5']}, 1h_spikes={s['count_1h_spikes']}, "
            f"1h_drops={s['count_1h_drops']}, flatlines>=4h={s['flatline_runs_ge_4h']} (max={s['max_flatline_hours']}h)"
        )
    print(f"\nSaved outputs to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
