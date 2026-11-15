
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
