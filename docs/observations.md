
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

## 5. Open Research Questions

1. Do other Mumbai stations have better hourly coverage?
2. Are missing observations concentrated at particular hours or dates?
3. Are missing patterns shared between PM2.5 and PM10?
4. Are duplicate-looking stations actually separate instruments?
5. Can another source provide recent observations for live monitoring?
6. What minimum data-completeness threshold should ERR require for model training?

---

## 6. Important Data Principles

- Never replace missing pollutant measurements with zero automatically.
- Preserve original station IDs and source information.
- Distinguish observed readings from imputed values.
- Preserve timestamps in IST and handle timezone conversion explicitly.
- Evaluate station-level data quality before model training.
- Never claim forecast accuracy without chronological backtesting.
