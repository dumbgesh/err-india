
import os
import requests

from datetime import datetime, timedelta
from dotenv import load_dotenv


# --------------------------------------------------
# 1. CONFIGURATION
# --------------------------------------------------

load_dotenv()

api_key = os.getenv("XKDR_API_KEY")

if not api_key:
    raise ValueError("XKDR_API_KEY not found in .env")


url = "https://airquality.xkdr.org/v1/measurements"

headers = {
    "Authorization": f"Bearer {api_key}"
}

params = [
    ("station", "site_5106"),
    ("parameter", "PM2.5"),
    ("parameter", "PM10"),
    ("start", "2025-08-24"),
    ("end", "2025-08-30"),
    ("agg", "hourly")
]


# --------------------------------------------------
# 2. FETCH MEASUREMENTS
# --------------------------------------------------

try:
    response = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=100
    )

    response.raise_for_status()

    measurements = response.json()

    print("HTTP Status:", response.status_code)
    print("Rows returned:", response.headers.get("X-Row-Count"))
    print("Truncated:", response.headers.get("X-Truncated"))


    # --------------------------------------------------
    # 3. PREPARE TIMESTAMPS
    # --------------------------------------------------

    rows = measurements["data"]

    start_time = datetime(2025, 8, 24, 0, 0)
    end_time = datetime(2025, 8, 30, 23, 0)

    expected_timestamps = {
        start_time + timedelta(hours=i)
        for i in range(168)
    }


    # --------------------------------------------------
    # 4. HOURLY COMPLETENESS ANALYSIS
    # --------------------------------------------------

    print("\n========== HOURLY COMPLETENESS ANALYSIS ==========")

    for pollutant in ["PM2.5", "PM10"]:

        pollutant_rows = [
            row for row in rows
            if row["parameter_name"] == pollutant
        ]

        actual_timestamps = {
            datetime.fromisoformat(row["period_start"])
            for row in pollutant_rows
        }

        missing_timestamps = sorted(
            expected_timestamps - actual_timestamps
        )

        last_observed = max(actual_timestamps) if actual_timestamps else None

        internal_missing = [
            timestamp for timestamp in missing_timestamps
            if last_observed is not None and timestamp <= last_observed
        ]

        trailing_missing = [
            timestamp for timestamp in missing_timestamps
            if last_observed is not None and timestamp > last_observed
        ]

        coverage = (
            len(actual_timestamps) / len(expected_timestamps) * 100
        )

        print(f"\nPollutant: {pollutant}")
        print("Records received:", len(actual_timestamps))
        print("Expected records:", len(expected_timestamps))
        print("Coverage:", round(coverage, 2), "%")
        print("Internal missing hours:", len(internal_missing))
        print("Trailing missing hours:", len(trailing_missing))

        if actual_timestamps:
            print("First observation:", min(actual_timestamps))
            print("Last observation:", last_observed)

        print("\nFirst 10 missing timestamps:")

        for timestamp in missing_timestamps[:10]:
            print(timestamp)


except requests.exceptions.HTTPError as error:
    print("HTTP error:", error)
    print("Response:", response.text[:1000])

except requests.exceptions.RequestException as error:
    print("Connection error:", error)

except (ValueError, KeyError) as error:
    print("Data processing error:", error)