import os
from datetime import datetime, timedelta
from typing import Any

import requests
from dotenv import load_dotenv


API_URL = "https://airquality.xkdr.org/v1/measurements"

STATIONS = [
    ("site_5106", "Vile Parle West"),
    ("site_5399", "Chakala-Andheri East"),
    ("site_5102", "Malad West"),
]

POLLUTANTS = ["PM2.5", "PM10"]
START_DATE = "2025-08-24"
END_DATE = "2025-08-30"
AGGREGATION = "hourly"


def load_api_key() -> str:
    """Load the XKDR API key from the .env file without exposing it."""
    load_dotenv()
    api_key = os.getenv("XKDR_API_KEY")
    if not api_key:
        raise ValueError("XKDR_API_KEY not found. Check your .env file.")
    return api_key


def build_expected_timestamps(start_date: str, end_date: str) -> set[datetime]:
    """Generate all expected hourly timestamps (inclusive) between start and end dates."""
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23)

    expected = set()
    current = start_dt
    while current <= end_dt:
        expected.add(current)
        current += timedelta(hours=1)
    return expected


def fetch_measurements(
    api_key: str,
    stations: list[tuple[str, str]],
    pollutants: list[str],
    start_date: str,
    end_date: str,
    agg: str,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Fetch hourly measurements for multiple stations and pollutants from XKDR."""
    headers = {"Authorization": f"Bearer {api_key}"}

    params: list[tuple[str, str]] = [
        ("station", station_id) for station_id, _ in stations
    ]
    params.extend(("parameter", pollutant) for pollutant in pollutants)
    params.extend(
        [
            ("start", start_date),
            ("end", end_date),
            ("agg", agg),
        ]
    )

    response = requests.get(
        API_URL,
        headers=headers,
        params=params,
        timeout=100,
    )
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise ValueError("Unexpected API response structure: expected JSON object with 'data' list.")

    return payload["data"], dict(response.headers)


def analyze_station_pollutant(
    rows: list[dict[str, Any]],
    station_id: str,
    station_name: str,
    pollutant: str,
    expected_timestamps: set[datetime],
) -> dict[str, Any]:
    """Compute completeness and concentration metrics for one station and pollutant."""
    matching_rows = [
        row
        for row in rows
        if isinstance(row, dict)
        and row.get("station_id") == station_id
        and row.get("parameter_name") == pollutant
    ]

    parsed_timestamps: list[datetime] = []
    valid_means: list[float] = []

    for row in matching_rows:
        period_start = row.get("period_start")
        if not isinstance(period_start, str):
            raise ValueError(
                f"Unexpected 'period_start' value for {station_id} / {pollutant}: {period_start!r}"
            )
        parsed_timestamps.append(datetime.fromisoformat(period_start))

        mean_val = row.get("mean")
        if isinstance(mean_val, (int, float)) and not isinstance(mean_val, bool):
            valid_means.append(float(mean_val))

    actual_timestamps = set(parsed_timestamps)
    raw_records = len(matching_rows)
    unique_records = len(actual_timestamps)
    duplicate_records = raw_records - unique_records

    expected_count = len(expected_timestamps)
    missing_timestamps = sorted(expected_timestamps - actual_timestamps)

    first_observed = min(actual_timestamps) if actual_timestamps else None
    last_observed = max(actual_timestamps) if actual_timestamps else None

    if last_observed is not None:
        internal_missing = sum(1 for ts in missing_timestamps if ts <= last_observed)
        trailing_missing = sum(1 for ts in missing_timestamps if ts > last_observed)
    else:
        internal_missing = 0
        trailing_missing = 0

    coverage_pct = (unique_records / expected_count * 100.0) if expected_count else 0.0
    avg_concentration = (sum(valid_means) / len(valid_means)) if valid_means else None

    return {
        "station_id": station_id,
        "station_name": station_name,
        "pollutant": pollutant,
        "raw_records": raw_records,
        "unique_records": unique_records,
        "duplicate_records": duplicate_records,
        "expected_records": expected_count,
        "missing_records": len(missing_timestamps),
        "internal_missing": internal_missing,
        "trailing_missing": trailing_missing,
        "coverage_pct": coverage_pct,
        "avg_concentration": avg_concentration,
        "first_observed": first_observed,
        "last_observed": last_observed,
    }


def format_timestamp(ts: datetime | None) -> str:
    """Format a datetime for display or return N/A when missing."""
    return ts.strftime("%Y-%m-%d %H:%M") if ts is not None else "N/A"


def print_comparison_table(results: list[dict[str, Any]]) -> None:
    """Print a formatted comparison table grouped by station and pollutant."""
    header = (
        f"{'Station':<32} | {'Pollutant':<9} | {'Unique/Exp':<10} | "
        f"{'Coverage':>8} | {'Missing':>7} | {'Internal':>8} | {'Trailing':>8} | "
        f"{'Avg Conc':>8} | {'Dups':>4} | {'First Observation':<16} | {'Last Observation':<16}"
    )
    separator = "-" * len(header)

    print("\n========== MULTI-STATION HOURLY DATA-QUALITY COMPARISON ==========")
    print(f"Period: {START_DATE} to {END_DATE} (hourly aggregation)")
    print(separator)
    print(header)
    print(separator)

    current_station = None
    for item in results:
        station_label = f"{item['station_name']} ({item['station_id']})"
        if current_station is not None and current_station != item["station_id"]:
            print(separator)
        current_station = item["station_id"]

        unique_exp = f"{item['unique_records']}/{item['expected_records']}"
        coverage_str = f"{item['coverage_pct']:.2f}%"
        avg_str = (
            f"{item['avg_concentration']:.2f}"
            if item["avg_concentration"] is not None
            else "N/A"
        )

        print(
            f"{station_label:<32} | "
            f"{item['pollutant']:<9} | "
            f"{unique_exp:<10} | "
            f"{coverage_str:>8} | "
            f"{item['missing_records']:>7} | "
            f"{item['internal_missing']:>8} | "
            f"{item['trailing_missing']:>8} | "
            f"{avg_str:>8} | "
            f"{item['duplicate_records']:>4} | "
            f"{format_timestamp(item['first_observed']):<16} | "
            f"{format_timestamp(item['last_observed']):<16}"
        )

    print(separator)


def main() -> None:
    """Run the multi-station historical data-quality comparison."""
    try:
        api_key = load_api_key()
        expected_timestamps = build_expected_timestamps(START_DATE, END_DATE)
        rows, resp_headers = fetch_measurements(
            api_key=api_key,
            stations=STATIONS,
            pollutants=POLLUTANTS,
            start_date=START_DATE,
            end_date=END_DATE,
            agg=AGGREGATION,
        )

        print("Request successful.")
        print("Rows returned (X-Row-Count):", resp_headers.get("X-Row-Count", len(rows)))
        print("Truncated (X-Truncated):", resp_headers.get("X-Truncated", "unknown"))

        if str(resp_headers.get("X-Truncated", "")).lower() == "true":
            print("WARNING: API response was truncated; completeness metrics may be incomplete.")

        results = []
        for station_id, station_name in STATIONS:
            for pollutant in POLLUTANTS:
                metrics = analyze_station_pollutant(
                    rows=rows,
                    station_id=station_id,
                    station_name=station_name,
                    pollutant=pollutant,
                    expected_timestamps=expected_timestamps,
                )
                results.append(metrics)

        print_comparison_table(results)

    except requests.exceptions.HTTPError as error:
        status_code = error.response.status_code if error.response is not None else "unknown"
        body_preview = error.response.text[:500] if error.response is not None else ""
        print(f"HTTP error (status {status_code}): {error}")
        if body_preview:
            print("Response preview:", body_preview)

    except requests.exceptions.RequestException as error:
        print(f"Connection error: {error}")

    except (ValueError, KeyError, TypeError) as error:
        print(f"Configuration or data error: {error}")


if __name__ == "__main__":
    main()
