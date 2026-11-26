import os
from datetime import datetime, timedelta
from typing import Any

import duckdb
from dotenv import load_dotenv


PARQUET_URL = (
    "https://airquality.xkdr.org/v1/files/"
    "v1/measurements/year=2024/month=08/data.parquet"
)

STATIONS = [
    ("site_5106", "Vile Parle West"),
    ("site_5399", "Chakala-Andheri East"),
    ("site_5102", "Malad West"),
]
POLLUTANTS = ["PM2.5", "PM10"]

# Verified /v1/measurements?agg=hourly baseline for 2024-08-24 to 2024-08-30
API_BASELINE = {
    ("site_5106", "PM2.5"): (168, 10.55),
    ("site_5106", "PM10"): (168, 49.19),
    ("site_5399", "PM2.5"): (0, None),
    ("site_5399", "PM10"): (0, None),
    ("site_5102", "PM2.5"): (156, 12.90),
    ("site_5102", "PM10"): (165, 47.49),
}


def load_api_key() -> str:
    """Load XKDR_API_KEY from .env without printing or exposing it."""
    load_dotenv()
    api_key = os.getenv("XKDR_API_KEY")
    if not api_key:
        raise ValueError("XKDR_API_KEY not found. Check your .env file.")
    return api_key


def create_authenticated_duckdb_connection(api_key: str) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with httpfs and authenticated XKDR HTTP headers."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

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
    return con


def build_month_hours(year: int, month: int) -> list[datetime]:
    """Generate all expected hourly timestamps for a calendar month."""
    start = datetime(year, month, 1, 0, 0)
    if month == 12:
        end_exclusive = datetime(year + 1, 1, 1, 0, 0)
    else:
        end_exclusive = datetime(year, month + 1, 1, 0, 0)

    hours: list[datetime] = []
    cur = start
    while cur < end_exclusive:
        hours.append(cur)
        cur += timedelta(hours=1)
    return hours


def compute_longest_missing_streak(
    expected_hours: list[datetime], observed_hours: set[datetime]
) -> tuple[int, int, int, int]:
    """
    Compute leading, internal, trailing, and longest consecutive missing hours
    across the full expected window.
    """
    if not observed_hours:
        total = len(expected_hours)
        return 0, 0, 0, total

    first_obs = min(observed_hours)
    last_obs = max(observed_hours)

    leading_missing = sum(1 for h in expected_hours if h < first_obs)
    trailing_missing = sum(1 for h in expected_hours if h > last_obs)
    internal_missing = sum(
        1 for h in expected_hours if first_obs <= h <= last_obs and h not in observed_hours
    )

    longest_streak = 0
    current_streak = 0
    for h in expected_hours:
        if h not in observed_hours:
            current_streak += 1
            if current_streak > longest_streak:
                longest_streak = current_streak
        else:
            current_streak = 0

    return leading_missing, internal_missing, trailing_missing, longest_streak


def fetch_august_2024_rows(con: duckdb.DuckDBPyConnection) -> list[tuple[Any, ...]]:
    """Query only the 3 target stations and 2 pollutants from the remote Parquet file."""
    query = f"""
    SELECT
        station_id,
        parameter_name,
        collected_at,
        value
    FROM read_parquet('{PARQUET_URL}')
    WHERE station_id IN ('site_5106', 'site_5399', 'site_5102')
      AND parameter_name IN ('PM2.5', 'PM10')
    ORDER BY station_id, parameter_name, collected_at;
    """
    return con.execute(query).fetchall()


def analyze_cadence(rows: list[tuple[Any, ...]]) -> None:
    """Inspect raw counts, nulls, duplicates, cadence, and boundary-aware gaps."""
    expected_hours = build_month_hours(2024, 8)  # 744 hours

    print("\n========== AUGUST 2024 RAW CADENCE & COMPLETENESS (744 EXPECTED HOURS) ==========")
    header = (
        f"{'Station':<26} | {'Pollutant':<9} | {'Raw':>4} | {'Nulls':>5} | "
        f"{'UniqTS':>6} | {'Dups':>4} | {'UniqHr':>6} | {'Obs/Hr(min-max)':<15} | "
        f"{'GapMin(min/med/max)':<19} | {'Lead/Int/Trail':<14} | {'MaxGapHr':>8}"
    )
    sep = "-" * len(header)
    print(sep)
    print(header)
    print(sep)

    for station_id, station_name in STATIONS:
        label = f"{station_name} ({station_id})"
        for pollutant in POLLUTANTS:
            sub = [r for r in rows if r[0] == station_id and r[1] == pollutant]
            raw_count = len(sub)
            null_count = sum(1 for r in sub if r[3] is None)
            timestamps = [r[2] for r in sub]
            unique_ts = sorted(set(timestamps))
            dup_count = raw_count - len(unique_ts)

            hour_counts: dict[datetime, int] = {}
            for ts in timestamps:
                hr = ts.replace(minute=0, second=0, microsecond=0)
                hour_counts[hr] = hour_counts.get(hr, 0) + 1

            observed_hours = set(hour_counts.keys())
            if hour_counts:
                obs_range = f"{min(hour_counts.values())}-{max(hour_counts.values())}"
            else:
                obs_range = "N/A"

            if len(unique_ts) >= 2:
                deltas_min = sorted(
                    int((unique_ts[i] - unique_ts[i - 1]).total_seconds() // 60)
                    for i in range(1, len(unique_ts))
                )
                med_min = deltas_min[len(deltas_min) // 2]
                gap_str = f"{deltas_min[0]}/{med_min}/{deltas_min[-1]}"
            else:
                gap_str = "N/A"

            lead, internal, trail, max_streak = compute_longest_missing_streak(
                expected_hours, observed_hours
            )
            lit_str = f"{lead}/{internal}/{trail}" if observed_hours else "EMPTY(744)"

            print(
                f"{label:<26} | {pollutant:<9} | {raw_count:>4} | {null_count:>5} | "
                f"{len(unique_ts):>6} | {dup_count:>4} | {len(observed_hours):>6} | "
                f"{obs_range:<15} | {gap_str:<19} | {lit_str:<14} | {max_streak:>8}"
            )
            if unique_ts:
                print(
                    f"   First: {unique_ts[0].strftime('%Y-%m-%d %H:%M')}  |  "
                    f"Last: {unique_ts[-1].strftime('%Y-%m-%d %H:%M')}"
                )
        print(sep)


def compare_with_baseline(rows: list[tuple[Any, ...]]) -> None:
    """Compare 2024-08-24..2024-08-30 DuckDB metrics with /v1/measurements?agg=hourly."""
    win_start = datetime(2024, 8, 24, 0, 0)
    win_end = datetime(2024, 8, 30, 23, 59, 59)

    print("\n========== BASELINE REPRODUCTION: 2024-08-24 TO 2024-08-30 (168 HOURS) ==========")
    header = (
        f"{'Station':<26} | {'Pollutant':<9} | {'DuckDB Raw':>10} | {'DuckDB Hrs':>10} | "
        f"{'API Hrs':>7} | {'DuckDB Mean':>11} | {'API Mean':>8} | {'Match?':<6}"
    )
    sep = "-" * len(header)
    print(sep)
    print(header)
    print(sep)

    for station_id, station_name in STATIONS:
        label = f"{station_name} ({station_id})"
        for pollutant in POLLUTANTS:
            sub = [
                r
                for r in rows
                if r[0] == station_id
                and r[1] == pollutant
                and win_start <= r[2] <= win_end
            ]
            hourly_buckets: dict[datetime, list[float]] = {}
            for _, _, ts, val in sub:
                hr = ts.replace(minute=0, second=0, microsecond=0)
                if hr not in hourly_buckets:
                    hourly_buckets[hr] = []
                if val is not None:
                    hourly_buckets[hr].append(float(val))

            duck_hours = len(hourly_buckets)
            hourly_means = [
                sum(vals) / len(vals)
                for vals in hourly_buckets.values()
                if vals
            ]
            duck_mean = (
                sum(hourly_means) / len(hourly_means) if hourly_means else None
            )

            api_hours, api_mean = API_BASELINE[(station_id, pollutant)]
            hours_match = duck_hours == api_hours
            mean_match = (
                (duck_mean is None and api_mean is None)
                or (
                    duck_mean is not None
                    and api_mean is not None
                    and abs(duck_mean - api_mean) <= 0.02
                )
            )
            match_str = "YES" if (hours_match and mean_match) else "DIFF"

            duck_mean_str = f"{duck_mean:.2f}" if duck_mean is not None else "N/A"
            api_mean_str = f"{api_mean:.2f}" if api_mean is not None else "N/A"

            print(
                f"{label:<26} | {pollutant:<9} | {len(sub):>10} | "
                f"{f'{duck_hours}/168':>10} | {f'{api_hours}/168':>7} | "
                f"{duck_mean_str:>11} | {api_mean_str:>8} | {match_str:<6}"
            )
        print(sep)


def main() -> None:
    """Run single-month remote DuckDB validation on August 2024."""
    api_key = load_api_key()
    con = create_authenticated_duckdb_connection(api_key)

    print("Querying remote August 2024 Parquet via DuckDB httpfs...")
    rows = fetch_august_2024_rows(con)
    print(f"Fetched {len(rows)} filtered rows (from 6,096,671 total rows in 2024-08).")

    analyze_cadence(rows)
    compare_with_baseline(rows)


if __name__ == "__main__":
    main()
