import csv
import json
import statistics
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb


PARQUET_PATH = Path("data/processed/measurement_quality/filtered_measurements_2019_2026.parquet")
OUTPUT_DIR = Path("data/processed/measurement_quality")


def load_series_map() -> dict[tuple[str, str], dict[datetime, float]]:
    """Load filtered 2019-2026 measurements into {(station_id, pollutant): {ts: val}}."""
    con = duckdb.connect()
    rows = con.execute(
        f"""
        SELECT station_id, parameter_name, collected_at, value
        FROM read_parquet('{PARQUET_PATH.as_posix()}')
        WHERE value IS NOT NULL
        ORDER BY station_id, parameter_name, collected_at
        """
    ).fetchall()

    series_map: dict[tuple[str, str], dict[datetime, float]] = {}
    for sid, pol, ts, val in rows:
        series_map.setdefault((sid, pol), {})[ts] = float(val)
    return series_map


def summarize_window(
    series: dict[datetime, float], start: datetime, end: datetime
) -> dict[str, Any]:
    """Summarize values in [start, end] inclusive."""
    vals = [v for ts, v in sorted(series.items()) if start <= ts <= end]
    if not vals:
        return {"n": 0, "unique": 0, "min": None, "median": None, "max": None, "std": None}
    svals = sorted(vals)
    return {
        "n": len(vals),
        "unique": len(set(vals)),
        "min": round(svals[0], 2),
        "median": round(statistics.median(svals), 2),
        "max": round(svals[-1], 2),
        "std": round(statistics.stdev(vals), 2) if len(vals) >= 2 else 0.0,
    }


def main() -> None:
    series_map = load_series_map()
    investigation_rows: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # A. Vile Parle West (site_5106) PM10 = 985.0 (May–Aug 2022 + overall)
    # ------------------------------------------------------------------
    vp_pm10 = series_map[("site_5106", "PM10")]
    vp_pm25 = series_map[("site_5106", "PM2.5")]
    all_985_pm10 = sorted(ts for ts, v in vp_pm10.items() if v == 985.0)
    all_985_pm25 = sorted(ts for ts, v in vp_pm25.items() if v == 985.0)

    # Group contiguous 1h runs of 985.0 in site_5106 PM10
    runs_985: list[tuple[datetime, datetime, int]] = []
    if all_985_pm10:
        rs = re = all_985_pm10[0]
        rlen = 1
        for ts in all_985_pm10[1:]:
            if ts - re == timedelta(hours=1):
                re = ts
                rlen += 1
            else:
                runs_985.append((rs, re, rlen))
                rs = re = ts
                rlen = 1
        runs_985.append((rs, re, rlen))

    may_aug_start = datetime(2022, 5, 1, 0, 0)
    may_aug_end = datetime(2022, 8, 31, 23, 0)
    may_aug_985 = [ts for ts in all_985_pm10 if may_aug_start <= ts <= may_aug_end]

    # Compare neighbours during the longest 985.0 run (2022-08-15 12:00..2022-08-16 11:00)
    run_peak_s = datetime(2022, 8, 15, 12, 0)
    run_peak_e = datetime(2022, 8, 16, 11, 0)
    a_vp25 = summarize_window(vp_pm25, run_peak_s, run_peak_e)
    a_ch10 = summarize_window(series_map[("site_5399", "PM10")], run_peak_s, run_peak_e)
    a_ma10 = summarize_window(series_map[("site_5102", "PM10")], run_peak_s, run_peak_e)

    print("=== A. site_5106 PM10 == 985.0 ===")
    print(f"Total 985.0 hours in site_5106 PM10: {len(all_985_pm10)} (May-Aug 2022: {len(may_aug_985)})")
    print(f"Total 985.0 hours in site_5106 PM2.5: {len(all_985_pm25)} -> {all_985_pm25}")
    print(f"Total distinct 985.0 runs in PM10: {len(runs_985)} (lengths: {[r[2] for r in runs_985]})")
    for rs, re, rlen in runs_985:
        if rlen >= 2 or (may_aug_start <= rs <= may_aug_end):
            print(f"   Run {rlen}h: {rs} .. {re}")
    print(f"During 24h 985.0 run (2022-08-15 12:00..2022-08-16 11:00):")
    print(f"   site_5106 PM2.5: {a_vp25}")
    print(f"   site_5399 PM10:  {a_ch10}")
    print(f"   site_5102 PM10:  {a_ma10}")

    investigation_rows.append(
        {
            "case_id": "A",
            "station_id": "site_5106",
            "pollutant": "PM10",
            "pattern": "Repeated 985.0 ceiling/saturation runs (May-Aug 2022)",
            "start_ts": str(all_985_pm10[0]) if all_985_pm10 else "N/A",
            "end_ts": str(all_985_pm10[-1]) if all_985_pm10 else "N/A",
            "affected_hours": len(all_985_pm10),
            "neighbour_evidence": f"During 24h 985.0 run: site_5399 PM10={a_ch10}, site_5102 PM10={a_ma10}",
            "co_pollutant_evidence": f"site_5106 PM2.5={a_vp25}; total PM2.5==985.0: {len(all_985_pm25)}",
            "classification": "Strongly suspicious",
            "qa_treatment": "Mask from training targets; retain raw with QA flag",
        }
    )

    # ------------------------------------------------------------------
    # B. Vile Parle West (site_5106) PM2.5 == 81.0 (183h, Feb 2023)
    # ------------------------------------------------------------------
    b_s = datetime(2023, 2, 3, 14, 0)
    b_e = datetime(2023, 2, 11, 4, 0)
    b_vp10 = summarize_window(vp_pm10, b_s, b_e)
    b_ch25 = summarize_window(series_map[("site_5399", "PM2.5")], b_s, b_e)
    b_ma25 = summarize_window(series_map[("site_5102", "PM2.5")], b_s, b_e)
    print("\n=== B. site_5106 PM2.5 == 81.0 (183h, 2023-02-03 14:00..2023-02-11 04:00) ===")
    print(f"   site_5106 PM10:  {b_vp10}")
    print(f"   site_5399 PM2.5: {b_ch25}")
    print(f"   site_5102 PM2.5: {b_ma25}")

    investigation_rows.append(
        {
            "case_id": "B",
            "station_id": "site_5106",
            "pollutant": "PM2.5",
            "pattern": "183h flatline at 81.0 (Feb 2023)",
            "start_ts": str(b_s),
            "end_ts": str(b_e),
            "affected_hours": 183,
            "neighbour_evidence": f"site_5399 PM2.5={b_ch25}, site_5102 PM2.5={b_ma25}",
            "co_pollutant_evidence": f"site_5106 PM10={b_vp10}",
            "classification": "Strongly suspicious",
            "qa_treatment": "Mask from training targets; retain raw with QA flag",
        }
    )

    # ------------------------------------------------------------------
    # C. Chakala-Andheri East (site_5399) PM10 == 31.63 (159h, Jun 2026)
    # ------------------------------------------------------------------
    c_s = datetime(2026, 6, 7, 4, 0)
    c_e = datetime(2026, 6, 13, 18, 0)
    c_ch25 = summarize_window(series_map[("site_5399", "PM2.5")], c_s, c_e)
    c_vp10 = summarize_window(vp_pm10, c_s, c_e)
    c_ma10 = summarize_window(series_map[("site_5102", "PM10")], c_s, c_e)
    print("\n=== C. site_5399 PM10 == 31.63 (159h, 2026-06-07 04:00..2026-06-13 18:00) ===")
    print(f"   site_5399 PM2.5: {c_ch25}")
    print(f"   site_5106 PM10:  {c_vp10} (station ended 2025-08)")
    print(f"   site_5102 PM10:  {c_ma10} (station ended 2025-09)")

    investigation_rows.append(
        {
            "case_id": "C",
            "station_id": "site_5399",
            "pollutant": "PM10",
            "pattern": "159h flatline at 31.63 (Jun 2026)",
            "start_ts": str(c_s),
            "end_ts": str(c_e),
            "affected_hours": 159,
            "neighbour_evidence": "site_5106 and site_5102 have no 2026 records in archive",
            "co_pollutant_evidence": f"site_5399 PM2.5={c_ch25}",
            "classification": "Strongly suspicious",
            "qa_treatment": "Mask from training targets; retain raw with QA flag",
        }
    )

    # ------------------------------------------------------------------
    # D. Malad West (site_5102) simultaneous Mar 2020 flatlines
    # ------------------------------------------------------------------
    d_s = datetime(2020, 3, 7, 11, 0)
    d_e = datetime(2020, 3, 11, 15, 0)
    d_vp25 = summarize_window(vp_pm25, d_s, d_e)
    d_vp10 = summarize_window(vp_pm10, d_s, d_e)
    d_ch25 = summarize_window(series_map[("site_5399", "PM2.5")], d_s, d_e)
    ma_pm25 = series_map[("site_5102", "PM2.5")]
    ma_pm10 = series_map[("site_5102", "PM10")]
    before_25 = [(ts, ma_pm25[ts]) for ts in sorted(ma_pm25) if datetime(2020, 3, 7, 8, 0) <= ts < d_s]
    after_25 = [(ts, ma_pm25[ts]) for ts in sorted(ma_pm25) if d_e < ts <= datetime(2020, 3, 11, 18, 0)]
    before_10 = [(ts, ma_pm10[ts]) for ts in sorted(ma_pm10) if datetime(2020, 3, 7, 10, 0) <= ts < datetime(2020, 3, 7, 13, 0)]
    after_10 = [(ts, ma_pm10[ts]) for ts in sorted(ma_pm10) if datetime(2020, 3, 11, 14, 0) < ts <= datetime(2020, 3, 11, 18, 0)]

    print("\n=== D. site_5102 simultaneous flatlines (Mar 2020) ===")
    print(f"   site_5102 PM2.5 before: {[(str(t), v) for t, v in before_25]}")
    print(f"   site_5102 PM2.5 after:  {[(str(t), v) for t, v in after_25]}")
    print(f"   site_5102 PM10  before: {[(str(t), v) for t, v in before_10]}")
    print(f"   site_5102 PM10  after:  {[(str(t), v) for t, v in after_10]}")
    print(f"   site_5106 PM2.5={d_vp25}, PM10={d_vp10}; site_5399 PM2.5={d_ch25}")

    investigation_rows.append(
        {
            "case_id": "D",
            "station_id": "site_5102",
            "pollutant": "PM2.5 & PM10",
            "pattern": "Simultaneous 101h (PM2.5=22.0) & 98h (PM10=142.0) flatlines (Mar 2020)",
            "start_ts": str(d_s),
            "end_ts": str(d_e),
            "affected_hours": 101,
            "neighbour_evidence": f"site_5106 PM2.5={d_vp25}, site_5106 PM10={d_vp10}",
            "co_pollutant_evidence": "Both PM2.5 and PM10 stuck simultaneously while site_5106 varied normally",
            "classification": "Strongly suspicious",
            "qa_treatment": "Mask from training targets; retain raw with QA flag",
        }
    )

    # ------------------------------------------------------------------
    # E. Chakala-Andheri East (site_5399) 3 exact zero PM2.5 readings
    # ------------------------------------------------------------------
    ch_pm25 = series_map[("site_5399", "PM2.5")]
    ch_pm10 = series_map[("site_5399", "PM10")]
    zeros = sorted(ts for ts, v in ch_pm25.items() if v == 0.0)
    print("\n=== E. site_5399 PM2.5 == 0.0 ===")
    for zts in zeros:
        ctx_25 = [(str(zts + timedelta(hours=d)), ch_pm25.get(zts + timedelta(hours=d))) for d in (-2, -1, 0, 1, 2)]
        ctx_10 = ch_pm10.get(zts)
        vp_ctx = vp_pm25.get(zts)
        ma_ctx = ma_pm25.get(zts)
        print(f"   Zero at {zts}: PM2.5 context={ctx_25}, site_5399 PM10={ctx_10}, site_5106 PM2.5={vp_ctx}, site_5102 PM2.5={ma_ctx}")

    investigation_rows.append(
        {
            "case_id": "E",
            "station_id": "site_5399",
            "pollutant": "PM2.5",
            "pattern": "3 exact 0.0 readings",
            "start_ts": str(zeros[0]) if zeros else "N/A",
            "end_ts": str(zeros[-1]) if zeros else "N/A",
            "affected_hours": len(zeros),
            "neighbour_evidence": f"Neighbouring stations at zero timestamps: {[ (str(z), vp_pm25.get(z), ma_pm25.get(z)) for z in zeros ]}",
            "co_pollutant_evidence": f"site_5399 PM10 at zero timestamps: {[ (str(z), ch_pm10.get(z)) for z in zeros ]}",
            "classification": "Requires further investigation",
            "qa_treatment": "Retain with QA flag; exclude only if calibration metadata confirms below-detection-limit fault",
        }
    )

    # Save targeted_anomaly_investigation.csv
    csv_path = OUTPUT_DIR / "targeted_anomaly_investigation.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(investigation_rows[0].keys()))
        writer.writeheader()
        writer.writerows(investigation_rows)

    # Save qa_policy.json
    qa_policy = {
        "policy_version": "1.0",
        "principle": "Never delete or overwrite raw measurements; attach qa_flag and mask targets during training.",
        "categories": {
            "confirmed_invalid": {
                "criteria": ["value < 0", "missing_or_null_timestamp"],
                "observed_count": 0,
                "treatment": "Mask from training targets; retain raw with QA flag",
            },
            "strongly_suspicious": {
                "criteria": [
                    "consecutive_identical_1h_run >= 6 hours (e.g. 183h, 159h, 101h, 98h)",
                    "repeated_ceiling_saturation_runs (e.g. site_5106 PM10 == 985.0 in May-Aug 2022)",
                ],
                "treatment": "Mask from training targets; retain raw with QA flag",
            },
            "requires_further_investigation": {
                "criteria": [
                    "exact_zero_readings (3 rows in site_5399 PM2.5)",
                    "univariate_statistical_extremes (>3.0xIQR or |robust_z| > 3.5) without flatline",
                    "isolated_1h_spikes_or_drops",
                ],
                "treatment": "Retain with QA flag; cross-check against neighbouring stations and weather before excluding",
            },
            "plausible_environmental_event": {
                "criteria": [
                    "elevated_readings_consistent_across_neighbours_or_winter_post_monsoon_seasonality"
                ],
                "treatment": "Retain for training and evaluation",
            },
        },
    }
    with (OUTPUT_DIR / "qa_policy.json").open("w", encoding="utf-8") as fh:
        json.dump(qa_policy, fh, indent=2)

    # Save investigation_report.md
    report_md = (
        "# Targeted Measurement Anomaly Investigation Report\n\n"
        "Generated by `src/preprocessing/investigate_anomalies.py` from "
        "`data/processed/measurement_quality/filtered_measurements_2019_2026.parquet`.\n\n"
        "| Case | Station | Pollutant | Pattern | Hours | Classification | Recommended QA Treatment |\n"
        "|---|---|---|---|---:|---|---|\n"
    )
    for r in investigation_rows:
        report_md += (
            f"| {r['case_id']} | {r['station_id']} | {r['pollutant']} | "
            f"{r['pattern']} | {r['affected_hours']} | {r['classification']} | {r['qa_treatment']} |\n"
        )
    with (OUTPUT_DIR / "investigation_report.md").open("w", encoding="utf-8") as fh:
        fh.write(report_md)

    print(f"\nSaved targeted outputs to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
