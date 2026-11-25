
# ERR — Data Observations & Research Log

## 1. XKDR Dataset Metadata

**Date inspected:** 16 November 2026

| Metric | Observed value |
|---|---:|
| Measurement rows | 196,521,834 |
| Stations | 558 |
| Parameters | 15 |
| Months covered | 207 |
| First month | January 2009 |
| Last month | March 2026 |
| Export timestamp | 30 September 2026 |
| Access tier | Full |

### Observation 1.1 — Dataset freshness

The export timestamp is September 30, 2026, but the latest measurement month is March 2026.

The export timestamp must not be interpreted as the latest observation date.

**Implication:** XKDR needs to be evaluated as a historical data source, not assumed to provide live observations.

---

## 2. Mumbai Station Exploration

**Endpoint:** `/v1/stations?city=Mumbai`

**Records returned:** 31

### Observation 2.1 — Station coverage

The returned station records contain:
- Station identifiers.
- Station names.
- Geographic coordinates.
- First and last observation timestamps.
- Available pollutants.
- Measurement counts.

Most CPCB station records have their latest observations around August–September 2025.

The US Embassy Mumbai record ends on March 3, 2025.

### Observation 2.2 — Multiple station identifiers

Some stations have different identifiers despite sharing identical or nearly identical coordinates.

Example:
- Bandra Kurla Complex — IITM
- Bandra Kurla Complex — MPCB

**Open question:** Are these independent monitoring instruments, duplicate records or separate data streams?

Do not merge them without investigating their provenance.

---

## 3. Vile Parle West — Initial Measurement Analysis

**Station:** Vile Parle West  
**Station ID:** `site_5106`  
**Source:** CPCB CAAQM

Station metadata:
- First observation: June 3, 2019
- Last observation: August 30, 2025
- Total station records: 514,848

### Observation 3.1 — Seven-day hourly sample

Period: August 24–30, 2025

| Metric | PM2.5 | PM10 |
|---|---:|---:|
| Records received | 110 | 110 |
| Expected hours | 168 | 168 |
| Coverage | 65.48% | 65.48% |
| Missing hours | 58 | 58 |
| Average concentration | 10.41 | 41.61 |
| Unit | µg/m³ | µg/m³ |

### Observation 3.2 — Missing data

For both pollutants:
- Internal missing hours: 37
- Trailing missing hours: 21
- First observation: August 24, 00:00
- Last observation: August 30, 02:00

Both pollutants have identical missing timestamps in this sample.

**Possible explanation:** The pollutants may share a common reporting pattern or data outage.

This is a hypothesis, not a confirmed cause.

### Observation 3.3 — Underlying observation count

Every returned hourly aggregate had `n = 1`.

This indicates one underlying measurement per hourly aggregate.

It does not establish continuous monitoring throughout each hour.

---

## 4. Multi-Station Data-Quality Comparison

**Script:** `src/ingestion/compare_station_quality.py`  
**Period:** 2025-08-24 to 2025-08-30  
**Aggregation:** hourly  
**Expected hours per pollutant per station:** 168  
**Stations:** Vile Parle West (`site_5106`), Chakala–Andheri East (`site_5399`), Malad West (`site_5102`)  
**Pollutants:** PM2.5 and PM10  
**Total API records returned:** 833  
**Truncated:** false  

### Observation 4.1 — Seven-day hourly comparison

| Station | Pollutant | Received | Expected | Coverage | Missing | Internal gaps | Trailing gaps | Average concentration | Duplicates |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Vile Parle West | PM2.5 | 110 | 168 | 65.48% | 58 | 37 | 21 | 10.41 | 0 |
| Vile Parle West | PM10 | 110 | 168 | 65.48% | 58 | 37 | 21 | 41.61 | 0 |
| Chakala–Andheri East | PM2.5 | 153 | 168 | 91.07% | 15 | 15 | 0 | 12.40 | 0 |
| Chakala–Andheri East | PM10 | 167 | 168 | 99.40% | 1 | 1 | 0 | 45.30 | 0 |
| Malad West | PM2.5 | 140 | 168 | 83.33% | 28 | 25 | 3 | 8.97 | 0 |
| Malad West | PM10 | 153 | 168 | 91.07% | 15 | 12 | 3 | 27.35 | 0 |

### Observation 4.2 — Sample findings

*Note: These findings reflect a single seven-day historical sample (2025-08-24 to 2025-08-30) and should not be treated as permanent station reliability rankings.*

1. Chakala–Andheri East had the highest coverage in this sample.
2. Malad West had intermediate coverage.
3. Vile Parle West had the lowest coverage.
4. All six combinations reported zero duplicate timestamps.
5. Vile Parle West had identical missing timestamps for PM2.5 and PM10.

---

## 5. Historical Data-Quality Stability (2024 Four-Window Sample)

**Script:** `src/ingestion/compare_historical_quality.py`

**Aggregation:** hourly (`168` expected hours per pollutant per station per window)

**Stations:** Vile Parle West (`site_5106`), Chakala–Andheri East (`site_5399`), Malad West (`site_5102`)

**Pollutants:** PM2.5 and PM10

**Windows:**
- Winter: `2024-01-15` to `2024-01-21` (`937` rows returned, `truncated=False`)
- Summer / Pre-Monsoon: `2024-04-15` to `2024-04-21` (`869` rows returned, `truncated=False`)
- Southwest Monsoon: `2024-08-24` to `2024-08-30` (`657` rows returned, `truncated=False`)
- Post-Monsoon: `2024-11-11` to `2024-11-17` (`556` rows returned, `truncated=False`)

*Note: These results reflect a four-window 2024 historical sample and measure data availability only, not measurement accuracy. They do not establish a permanently most reliable station.*

### Observation 5.1 — Four-window hourly comparison

| Season | Station | Pollutant | Raw | Unique/Exp | Dups | Coverage | Missing | Internal | Trailing | Avg Conc | First Observation | Last Observation |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| Winter | Vile Parle West | PM2.5 | 167 | 167/168 | 0 | 99.40% | 1 | 1 | 0 | 33.91 | 2024-01-15 00:00 | 2024-01-21 23:00 |
| Winter | Vile Parle West | PM10 | 167 | 167/168 | 0 | 99.40% | 1 | 1 | 0 | 73.65 | 2024-01-15 00:00 | 2024-01-21 23:00 |
| Winter | Chakala–Andheri East | PM2.5 | 131 | 131/168 | 0 | 77.98% | 37 | 37 | 0 | 16.28 | 2024-01-16 13:00 | 2024-01-21 23:00 |
| Winter | Chakala–Andheri East | PM10 | 167 | 167/168 | 0 | 99.40% | 1 | 1 | 0 | 67.57 | 2024-01-15 00:00 | 2024-01-21 23:00 |
| Winter | Malad West | PM2.5 | 152 | 152/168 | 0 | 90.48% | 16 | 16 | 0 | 26.35 | 2024-01-15 00:00 | 2024-01-21 23:00 |
| Winter | Malad West | PM10 | 153 | 153/168 | 0 | 91.07% | 15 | 15 | 0 | 90.04 | 2024-01-15 00:00 | 2024-01-21 23:00 |
| Summer / Pre-Monsoon | Vile Parle West | PM2.5 | 162 | 162/168 | 0 | 96.43% | 6 | 6 | 0 | 37.54 | 2024-04-15 00:00 | 2024-04-21 23:00 |
| Summer / Pre-Monsoon | Vile Parle West | PM10 | 163 | 163/168 | 0 | 97.02% | 5 | 5 | 0 | 122.54 | 2024-04-15 00:00 | 2024-04-21 23:00 |
| Summer / Pre-Monsoon | Chakala–Andheri East | PM2.5 | 150 | 150/168 | 0 | 89.29% | 18 | 18 | 0 | 61.40 | 2024-04-15 00:00 | 2024-04-21 23:00 |
| Summer / Pre-Monsoon | Chakala–Andheri East | PM10 | 126 | 126/168 | 0 | 75.00% | 42 | 42 | 0 | 141.30 | 2024-04-15 00:00 | 2024-04-21 23:00 |
| Summer / Pre-Monsoon | Malad West | PM2.5 | 137 | 137/168 | 0 | 81.55% | 31 | 31 | 0 | 40.01 | 2024-04-15 00:00 | 2024-04-21 23:00 |
| Summer / Pre-Monsoon | Malad West | PM10 | 131 | 131/168 | 0 | 77.98% | 37 | 37 | 0 | 102.78 | 2024-04-15 00:00 | 2024-04-21 23:00 |
| Southwest Monsoon | Vile Parle West | PM2.5 | 168 | 168/168 | 0 | 100.00% | 0 | 0 | 0 | 10.55 | 2024-08-24 00:00 | 2024-08-30 23:00 |
| Southwest Monsoon | Vile Parle West | PM10 | 168 | 168/168 | 0 | 100.00% | 0 | 0 | 0 | 49.19 | 2024-08-24 00:00 | 2024-08-30 23:00 |
| Southwest Monsoon | Chakala–Andheri East | PM2.5 | 0 | 0/168 | 0 | 0.00% | 168 | 0 | 0 | N/A | N/A | N/A |
| Southwest Monsoon | Chakala–Andheri East | PM10 | 0 | 0/168 | 0 | 0.00% | 168 | 0 | 0 | N/A | N/A | N/A |
| Southwest Monsoon | Malad West | PM2.5 | 156 | 156/168 | 0 | 92.86% | 12 | 12 | 0 | 12.90 | 2024-08-24 00:00 | 2024-08-30 23:00 |
| Southwest Monsoon | Malad West | PM10 | 165 | 165/168 | 0 | 98.21% | 3 | 3 | 0 | 47.49 | 2024-08-24 00:00 | 2024-08-30 23:00 |
| Post-Monsoon | Vile Parle West | PM2.5 | 135 | 135/168 | 0 | 80.36% | 33 | 33 | 0 | 37.21 | 2024-11-11 00:00 | 2024-11-17 23:00 |
| Post-Monsoon | Vile Parle West | PM10 | 143 | 143/168 | 0 | 85.12% | 25 | 25 | 0 | 139.89 | 2024-11-11 00:00 | 2024-11-17 23:00 |
| Post-Monsoon | Chakala–Andheri East | PM2.5 | 0 | 0/168 | 0 | 0.00% | 168 | 0 | 0 | N/A | N/A | N/A |
| Post-Monsoon | Chakala–Andheri East | PM10 | 0 | 0/168 | 0 | 0.00% | 168 | 0 | 0 | N/A | N/A | N/A |
| Post-Monsoon | Malad West | PM2.5 | 148 | 148/168 | 0 | 88.10% | 20 | 20 | 0 | 55.82 | 2024-11-11 00:00 | 2024-11-17 23:00 |
| Post-Monsoon | Malad West | PM10 | 130 | 130/168 | 0 | 77.38% | 38 | 38 | 0 | 172.16 | 2024-11-11 00:00 | 2024-11-17 23:00 |

### Observation 5.2 — Cross-window coverage summary

| Station | Pollutant | Winter | Summer / Pre-Monsoon | Southwest Monsoon | Post-Monsoon | Four-Window Mean Coverage |
|---|---|---:|---:|---:|---:|---:|
| Vile Parle West | PM2.5 | 99.40% | 96.43% | 100.00% | 80.36% | 94.05% |
| Vile Parle West | PM10 | 99.40% | 97.02% | 100.00% | 85.12% | 95.39% |
| Chakala–Andheri East | PM2.5 | 77.98% | 89.29% | 0.00% | 0.00% | 41.82% |
| Chakala–Andheri East | PM10 | 99.40% | 75.00% | 0.00% | 0.00% | 43.60% |
| Malad West | PM2.5 | 90.48% | 81.55% | 92.86% | 88.10% | 88.24% |
| Malad West | PM10 | 91.07% | 77.98% | 98.21% | 77.38% | 86.16% |

*Note: `Four-Window Mean Coverage` is the arithmetic mean of coverage across all four selected seven-day calendar windows, including windows with zero observations.*

### Observation 5.3 — Evidence-based findings (2024 sample)

1. **Station completeness varies substantially across time:** Vile Parle West (`site_5106`) had the lowest coverage in the August 2025 sample (65.48%), yet achieved the highest mean coverage across the four 2024 windows (94.05% PM2.5, 95.39% PM10, including 100.00% in Southwest Monsoon 2024).
2. **Extended outage at Chakala–Andheri East (`site_5399`):** Although `site_5399` had the highest coverage in the August 2025 sample, it returned 0/168 records (0.00%) for both PM2.5 and PM10 in both the Southwest Monsoon and Post-Monsoon 2024 windows.
3. **Consistent intermediate coverage at Malad West (`site_5102`):** Malad West maintained 77.38%–98.21% coverage across all four 2024 windows (mean 88.24% PM2.5, 86.16% PM10).
4. **Zero duplicate timestamps:** All 24 station–pollutant–window combinations reported `0` duplicate timestamps, and all timestamps were naive IST as specified by XKDR metadata.

---

## 6. Open Research Questions

1. Do other Mumbai stations have better hourly coverage?
2. Are missing observations concentrated at particular hours or dates?
3. Are missing patterns shared between PM2.5 and PM10?
4. Are duplicate-looking stations actually separate instruments?
5. Can another source provide recent observations for live monitoring?
6. What minimum data-completeness threshold should ERR require for model training?

---

## 7. Important Data Principles

- Never replace missing pollutant measurements with zero automatically.
- Preserve original station IDs and source information.
- Distinguish observed readings from imputed values.
- Preserve timestamps in IST and handle timezone conversion explicitly.
- Evaluate station-level data quality before model training.
- Never claim forecast accuracy without chronological backtesting.
