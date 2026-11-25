
# ERR — Technical Documentation

## 1. Project Overview

ERR is an air-quality intelligence project focused on monitoring air quality across India and forecasting pollutant concentrations for the next 72 hours.

Initial forecasting targets:
- PM2.5
- PM10

The project uses historical air-quality measurements, live observations and weather information to develop forecasting models.

---

## 2. Development Environment

- Language: Python 3.13
- IDE: Visual Studio Code
- Environment: Python virtual environment (`.venv`)
- API: XKDR India Air Quality Database
- HTTP library: Requests
- Environment variable management: python-dotenv

### Dependencies

- requests
- python-dotenv

---

## 3. Data Source: XKDR

Base URL:

`https://airquality.xkdr.org`

Authentication: Bearer token.

### 3.1 Metadata Inspection

Script: `src/ingestion/inspect_xkdr.py`

Endpoint:
`GET /v1/meta`

Purpose:
- Inspect dataset coverage.
- Retrieve export timestamp.
- Inspect measurement count.
- Identify available stations and parameters.
- Verify API access tier and query limits.

### 3.2 Station Inspection

Script: `src/ingestion/inspect_stations.py`

Endpoint:
`GET /v1/stations`

Parameters:
- `city=Mumbai`

Purpose:
- Retrieve Mumbai monitoring stations.
- Inspect station IDs and names.
- Retrieve geographic coordinates.
- Examine first and last recorded observations.
- Identify available pollutants.

### 3.3 Measurement Inspection

Script: `src/ingestion/inspect_measurements.py`

Endpoint:
`GET /v1/measurements`

Query configuration:
- Station: `site_5106`
- Pollutants: PM2.5, PM10
- Start: 2025-08-24
- End: 2025-08-30
- Aggregation: Hourly

Purpose:
- Retrieve historical pollutant measurements.
- Inspect response structure.
- Examine concentration values and units.
- Calculate hourly data completeness.
- Identify missing timestamps.

### 3.4 Data Completeness Method

Expected hourly timestamps are generated for the requested period.

Actual timestamps are extracted from the API response.

Missing timestamps are calculated using set difference:

`Missing = Expected timestamps - Actual timestamps`

Missing observations are classified as:
- Internal gaps: Missing timestamps before or at the last observed timestamp.
- Trailing gaps: Missing timestamps after the last observed timestamp.

Coverage:

`Coverage (%) = (Observed hours / Expected hours) × 100`

### 3.5 Multi-Station Data-Quality Comparison

Script: `src/ingestion/compare_station_quality.py`

Endpoint:
`GET /v1/measurements`

Query configuration:
- Stations (repeated `station` parameter):
  - `site_5106` (Vile Parle West)
  - `site_5399` (Chakala–Andheri East)
  - `site_5102` (Malad West)
- Pollutants (repeated `parameter` parameter): `PM2.5`, `PM10`
- Start: `2025-08-24`
- End: `2025-08-30`
- Aggregation: `hourly`

Metrics computed per station and pollutant:
- Unique observed hourly timestamps vs. expected hourly timestamps (168 hours)
- Duplicate timestamp count (raw records minus unique timestamps, reported without silent removal)
- Total missing timestamps, internal gaps (`<= last_observed`), and trailing gaps (`> last_observed`)
- Coverage percentage
- Average pollutant concentration (`mean` across non-null observations; missing readings are never treated as zero or imputed)
- First and last observed timestamps

Methodology:
- All expected hourly timestamps from `2025-08-24 00:00` through `2025-08-30 23:00` (168 hours) are generated.
- API rows are grouped by `(station_id, parameter_name)` without modifying or imputing missing values.
- Empty station/pollutant combinations are handled gracefully (`0/168`, `0.00%` coverage, `N/A` average concentration and timestamps).

### 3.6 Historical Data-Quality Stability Comparison

Script: `src/ingestion/compare_historical_quality.py`

Endpoint:
`GET /v1/measurements`

Query configuration:
- Stations (repeated `station` parameter):
  - `site_5106` (Vile Parle West)
  - `site_5399` (Chakala–Andheri East)
  - `site_5102` (Malad West)
- Pollutants (repeated `parameter` parameter): `PM2.5`, `PM10`
- Aggregation: `hourly`
- Four seven-day 2024 windows (`168` expected hours per window):
  - Winter: `2024-01-15` to `2024-01-21`
  - Summer / Pre-Monsoon: `2024-04-15` to `2024-04-21`
  - Southwest Monsoon: `2024-08-24` to `2024-08-30`
  - Post-Monsoon: `2024-11-11` to `2024-11-17`

Methodology and metrics:
- Reuses `compare_station_quality.py` configuration, timestamp generation, and per-station/pollutant completeness analysis.
- Evaluates each `(season, station, pollutant)` combination independently (`24` combinations total).
- Verifies that `meta.timezone` reports naive IST for `cpcb_caaqm` rows and checks that parsed timestamps carry no timezone offset (`tzinfo is None`).
- Checks both `X-Truncated` headers and `meta.truncated` to ensure truncated responses are never silently treated as complete.
- Computes expected hours (`168`), raw records, unique timestamps, duplicate timestamps, missing timestamps, internal gaps, trailing gaps, coverage percentage, average concentration from valid numeric `mean` values (without zero-filling or imputation), and first/last observed timestamps.
- Outputs a grouped comparison table and a four-window station coverage summary, keeping data availability distinct from sensor accuracy.

---

## 4. Current Development Status

Completed:
- Python environment configured.
- XKDR API connection established.
- Dataset metadata inspected.
- Mumbai station metadata retrieved.
- Historical PM2.5 and PM10 measurements retrieved.
- Initial hourly completeness analysis implemented.

In progress:
- Multi-station data-quality comparison.
- Historical data extraction.
- Data cleaning and preprocessing.

Not yet implemented:
- Live data ingestion.
- Weather data integration.
- Forecasting models.
- AQI calculation.
