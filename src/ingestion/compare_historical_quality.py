from typing import Any

import requests

try:
    from src.ingestion.compare_station_quality import (
        AGGREGATION,
        API_URL,
        POLLUTANTS,
        STATIONS,
        analyze_station_pollutant,
        build_expected_timestamps,
        format_timestamp,
        load_api_key,
    )
except ImportError:
    from compare_station_quality import (
        AGGREGATION,
        API_URL,
        POLLUTANTS,
        STATIONS,
        analyze_station_pollutant,
        build_expected_timestamps,
        format_timestamp,
        load_api_key,
    )


WINDOWS = [
    ("Winter", "2024-01-15", "2024-01-21"),
    ("Summer / Pre-Monsoon", "2024-04-15", "2024-04-21"),
    ("Southwest Monsoon", "2024-08-24", "2024-08-30"),
    ("Post-Monsoon", "2024-11-11", "2024-11-17"),
]


def fetch_window_measurements(
    api_key: str,
    stations: list[tuple[str, str]],
    pollutants: list[str],
    start_date: str,
    end_date: str,
    agg: str,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, str]]:
    """Fetch hourly measurements and metadata for one historical window."""
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
        raise ValueError(
            "Unexpected API response structure: expected JSON object with 'data' list."
        )

    meta = payload.get("meta")
    if not isinstance(meta, dict):
        meta = {}

    return payload["data"], meta, dict(response.headers)


def is_response_truncated(meta: dict[str, Any], headers: dict[str, str]) -> bool:
    """Check both JSON metadata and response headers for truncation."""
    header_flag = str(headers.get("X-Truncated", "")).strip().lower() == "true"
    meta_flag = bool(meta.get("truncated", False))
    return header_flag or meta_flag


def verify_naive_ist_timestamps(metrics: dict[str, Any]) -> None:
    """Ensure observed timestamps are naive datetimes (no timezone offset attached)."""
    for key in ("first_observed", "last_observed"):
        ts = metrics.get(key)
        if ts is not None and ts.tzinfo is not None:
            raise ValueError(
                f"Expected naive IST timestamp for {metrics['station_id']} "
                f"{metrics['pollutant']}, got timezone-aware {ts.isoformat()}"
            )


def print_window_comparison_table(results: list[dict[str, Any]]) -> None:
    """Print a comparison table grouped by season, station, and pollutant."""
    header = (
        f"{'Season':<21} | {'Station':<32} | {'Pollutant':<9} | "
        f"{'Raw':>3} | {'Unique/Exp':<10} | {'Dups':>4} | {'Coverage':>8} | "
        f"{'Missing':>7} | {'Internal':>8} | {'Trailing':>8} | "
        f"{'Avg Conc':>8} | {'First Observation':<16} | {'Last Observation':<16}"
    )
    separator = "-" * len(header)

    print("\n========== HISTORICAL DATA-QUALITY STABILITY COMPARISON (2024) ==========")
    print(separator)
    print(header)
    print(separator)

    prev_season = None
    prev_station = None
    for item in results:
        if prev_season is not None and (
            prev_season != item["season"] or prev_station != item["station_id"]
        ):
            print(separator)
        prev_season = item["season"]
        prev_station = item["station_id"]

        station_label = f"{item['station_name']} ({item['station_id']})"
        unique_exp = f"{item['unique_records']}/{item['expected_records']}"
        coverage_str = f"{item['coverage_pct']:.2f}%"
        avg_str = (
            f"{item['avg_concentration']:.2f}"
            if item["avg_concentration"] is not None
            else "N/A"
        )

        print(
            f"{item['season']:<21} | "
            f"{station_label:<32} | "
            f"{item['pollutant']:<9} | "
            f"{item['raw_records']:>3} | "
            f"{unique_exp:<10} | "
            f"{item['duplicate_records']:>4} | "
            f"{coverage_str:>8} | "
            f"{item['missing_records']:>7} | "
            f"{item['internal_missing']:>8} | "
            f"{item['trailing_missing']:>8} | "
            f"{avg_str:>8} | "
            f"{format_timestamp(item['first_observed']):<16} | "
            f"{format_timestamp(item['last_observed']):<16}"
        )

    print(separator)


def print_station_summary(results: list[dict[str, Any]]) -> None:
    """Print a concise summary comparing each station's coverage across all windows."""
    season_names = [season for season, _, _ in WINDOWS]
    lookup = {
        (item["station_id"], item["pollutant"], item["season"]): item["coverage_pct"]
        for item in results
    }

    header = (
        f"{'Station':<32} | {'Pollutant':<9} | "
        + " | ".join(f"{name:<20}" for name in season_names)
        + f" | {'Mean Cov':>8}"
    )
    separator = "-" * len(header)

    print("\n========== STATION COVERAGE SUMMARY ACROSS 2024 WINDOWS ==========")
    print("Note: Coverage measures data availability only, not sensor accuracy.")
    print(separator)
    print(header)
    print(separator)

    for station_id, station_name in STATIONS:
        station_label = f"{station_name} ({station_id})"
        for pollutant in POLLUTANTS:
            cov_values = [
                lookup[(station_id, pollutant, season)] for season in season_names
            ]
            mean_cov = sum(cov_values) / len(cov_values) if cov_values else 0.0
            cov_cells = " | ".join(f"{val:>19.2f}%" for val in cov_values)
            print(
                f"{station_label:<32} | {pollutant:<9} | {cov_cells} | {mean_cov:>7.2f}%"
            )
        print(separator)


def main() -> None:
    """Run the four-window 2024 historical data-quality stability comparison."""
    try:
        api_key = load_api_key()
        all_results: list[dict[str, Any]] = []
        any_truncated = False

        print("Fetching 2024 seasonal windows from XKDR /v1/measurements...")

        for season, start_date, end_date in WINDOWS:
            expected_timestamps = build_expected_timestamps(start_date, end_date)
            rows, meta, resp_headers = fetch_window_measurements(
                api_key=api_key,
                stations=STATIONS,
                pollutants=POLLUTANTS,
                start_date=start_date,
                end_date=end_date,
                agg=AGGREGATION,
            )

            truncated = is_response_truncated(meta, resp_headers)
            if truncated:
                any_truncated = True

            row_count = resp_headers.get("X-Row-Count", len(rows))
            tz_info = meta.get("timezone", "not reported")
            print(
                f"  - {season} ({start_date} to {end_date}): "
                f"rows={row_count}, truncated={truncated}, timezone={tz_info!r}"
            )

            if truncated:
                print(
                    f"    WARNING: Response for {season} was truncated; "
                    "do not treat completeness metrics as complete."
                )

            if not rows:
                print(f"    WARNING: Empty response returned for {season}.")

            for station_id, station_name in STATIONS:
                for pollutant in POLLUTANTS:
                    metrics = analyze_station_pollutant(
                        rows=rows,
                        station_id=station_id,
                        station_name=station_name,
                        pollutant=pollutant,
                        expected_timestamps=expected_timestamps,
                    )
                    verify_naive_ist_timestamps(metrics)
                    metrics["season"] = season
                    metrics["start_date"] = start_date
                    metrics["end_date"] = end_date
                    metrics["truncated"] = truncated
                    all_results.append(metrics)

        print_window_comparison_table(all_results)
        print_station_summary(all_results)

        if any_truncated:
            print(
                "\nWARNING: One or more API responses were truncated. "
                "Completeness metrics above are incomplete."
            )

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
