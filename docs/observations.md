
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

## 6. Single-Month DuckDB Validation (August 2024)

**Script:** `src/ingestion/validate_duckdb_month.py`

**Target Partition:** `v1/measurements/year=2024/month=08/data.parquet` (`6,096,671` total rows, `10,494,768` bytes / ~10.01 MiB)

**Partition Scope Correction:** June 2019 through November 2026 comprises **90 calendar months** (`713.88 MB` total across those 90 Parquet files), not 95 (which had included `2019-01` through `2019-05`).

### Observation 6.1 — Remote HTTP Range & DuckDB `httpfs` Verification

- `HEAD` on `v1/measurements/year=2024/month=08/data.parquet` returned HTTP `200`, `Accept-Ranges: bytes`, `Content-Length: 10494768`.
- `Range: bytes=0-63` returned HTTP `206 Partial Content` with Parquet magic bytes `PAR1`.
- DuckDB `EXPLAIN ANALYZE` (`httpfs`) transferred `3.2 MiB` (`Data Read: 3.3 MB`) across `1` `HEAD` and `7` `GET` Range requests in `2.57s`, returning `2,678` filtered rows out of `6,096,671` (~68% byte reduction vs. full-file download).

### Observation 6.2 — August 2024 Raw Cadence & Completeness (`744` Expected Hours)

| Station | Pollutant | Raw | Nulls | UniqTS | Dups | UniqHr | Obs/Hr (min-max) | Gap Min (min/med/max) | Lead/Int/Trail | MaxGapHr | First Observation | Last Observation |
|---|---|---:|---:|---:|---:|---:|---|---|---|---|---|---|
| Vile Parle West (`site_5106`) | PM2.5 | 678 | 0 | 678 | 0 | 678 | 1-1 | 60/60/1620 | 0/66/0 | 26 | 2024-08-01 00:00 | 2024-08-31 23:00 |
| Vile Parle West (`site_5106`) | PM10 | 672 | 0 | 672 | 0 | 672 | 1-1 | 60/60/1620 | 0/72/0 | 26 | 2024-08-01 00:00 | 2024-08-31 23:00 |
| Chakala–Andheri East (`site_5399`) | PM2.5 | 0 | 0 | 0 | 0 | 0 | N/A | N/A | EMPTY(744) | 744 | N/A | N/A |
| Chakala–Andheri East (`site_5399`) | PM10 | 0 | 0 | 0 | 0 | 0 | N/A | N/A | EMPTY(744) | 744 | N/A | N/A |
| Malad West (`site_5102`) | PM2.5 | 646 | 0 | 646 | 0 | 646 | 1-1 | 60/60/1020 | 0/98/0 | 16 | 2024-08-01 00:00 | 2024-08-31 23:00 |
| Malad West (`site_5102`) | PM10 | 682 | 0 | 682 | 0 | 682 | 1-1 | 60/60/1020 | 0/62/0 | 16 | 2024-08-01 00:00 | 2024-08-31 23:00 |

**Cadence finding:** All observed records for `site_5106` and `site_5102` in August 2024 have strictly `1` observation per hour (`min=1, max=1`), `0` nulls, `0` duplicate timestamps, and a minimum/median consecutive interval of `60` minutes. This confirms that hourly availability is an appropriate metric for these series in this month.

### Observation 6.3 — Baseline Reproduction (`2024-08-24` to `2024-08-30`, `168` Hours)

| Station | Pollutant | DuckDB Raw | DuckDB Hrs | API Hrs | DuckDB Mean | API Mean | Match? |
|---|---|---:|---:|---:|---:|---:|---|
| Vile Parle West (`site_5106`) | PM2.5 | 168 | 168/168 | 168/168 | 10.55 | 10.55 | YES |
| Vile Parle West (`site_5106`) | PM10 | 168 | 168/168 | 168/168 | 49.19 | 49.19 | YES |
| Chakala–Andheri East (`site_5399`) | PM2.5 | 0 | 0/168 | 0/168 | N/A | N/A | YES |
| Chakala–Andheri East (`site_5399`) | PM10 | 0 | 0/168 | 0/168 | N/A | N/A | YES |
| Malad West (`site_5102`) | PM2.5 | 156 | 156/168 | 156/168 | 12.90 | 12.90 | YES |
| Malad West (`site_5102`) | PM10 | 165 | 165/168 | 165/168 | 47.49 | 47.49 | YES |

### Observation 6.4 — Limitations & Unresolved Questions

1. **Cadence in other months:** While August 2024 is strictly hourly (`1` obs/hr) for `site_5106` and `site_5102`, the multi-month script should still check `Obs/Hr` per month in case sub-hourly intervals appear in earlier years.
2. **Data availability vs. accuracy:** Validating hourly availability does not verify sensor calibration or value quality.

---

## 7. 2024 Annual Historical Data-Quality Pilot

**Script:** `src/ingestion/analyze_duckdb_2024.py`

**Scope:** All 12 monthly 2024 XKDR Parquet partitions (`2024-01` through `2024-12`, `8,784` calendar hours in leap year 2024), queried remotely via DuckDB `httpfs`.

**Processing & Validation:** All 12 partitions (`status=ok`) were processed cleanly. August 2024 full-month valid-hour counts and the `2024-08-24`–`2024-08-30` API baseline matched `100%` with zero discrepancies.

### Observation 7.1 — Station-wise monthly availability (% of calendar hours with valid values)

| Station | Pollutant | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | Annual Valid Hours |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Vile Parle West (`site_5106`) | PM2.5 | 97.4% | 96.0% | 86.4% | 94.4% | 72.0% | 66.8% | 95.0% | 91.1% | 89.0% | 83.5% | 89.6% | 79.7% | 7,618 / 8,784 (86.73%) |
| Vile Parle West (`site_5106`) | PM10 | 87.1% | 96.0% | 88.8% | 94.9% | 71.2% | 67.8% | 90.5% | 90.3% | 90.7% | 86.4% | 90.7% | 84.3% | 7,599 / 8,784 (86.51%) |
| Chakala–Andheri East (`site_5399`) | PM2.5 | 73.0% | 97.3% | 94.0% | 95.7% | 86.8% | 93.1% | 23.8% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 4,101 / 8,784 (46.69%) |
| Chakala–Andheri East (`site_5399`) | PM10 | 94.4% | 90.4% | 99.6% | 88.8% | 73.0% | 99.2% | 22.6% | 0.0% | 0.0% | 0.0% | 0.0% | 0.0% | 4,136 / 8,784 (47.09%) |
| Malad West (`site_5102`) | PM2.5 | 89.4% | 82.9% | 88.6% | 82.9% | 85.5% | 82.1% | 74.2% | 86.8% | 82.4% | 87.4% | 92.5% | 87.9% | 7,486 / 8,784 (85.22%) |
| Malad West (`site_5102`) | PM10 | 89.4% | 82.8% | 62.5% | 85.0% | 86.2% | 88.8% | 78.2% | 91.7% | 83.8% | 83.7% | 92.6% | 89.0% | 7,417 / 8,784 (84.44%) |

### Observation 7.2 — Cadence, gaps, and zero-observation months

- **Strictly hourly cadence in 2024:** Across all active station–pollutant–month combinations, `nulls = 0`, `duplicates = 0`, `max_obs_per_hour = 1`, and consecutive timestamp intervals had `min = 60` min and `median = 60` min.
- **Zero-observation months (`status = no_records`):** Chakala–Andheri East (`site_5399`) had zero `PM2.5` and `PM10` records for **five consecutive months** (`2024-08` through `2024-12`), following a drop to `23.8%` / `22.6%` in `2024-07`. (This indicates missing particulate records, not necessarily that the entire station was offline.)
- **Longest single-month missing-hour gaps:**
  - `site_5106`: `108` hours (PM2.5 and PM10)
  - `site_5102`: `40` hours (PM2.5) and `46` hours (PM10)
  - `site_5399`: `744` hours (full-month gaps in `2024-08`, `2024-10`, `2024-12`, and `720` hours in `2024-09`, `2024-11`)
- **Availability vs. accuracy:** These metrics quantify historical data availability only; sensor calibration and value accuracy remain to be evaluated separately.

---

## 8. Full Historical Continuity Analysis (June 2019–November 2026)

**Script:** `src/ingestion/analyze_duckdb_full_history.py`

**Scope:** `90` calendar months (`2019-06` through `2026-11`, `65,760` calendar hours) for `site_5106`, `site_5399`, and `site_5102` (`PM2.5` and `PM10`).

**Partial-Month Export Note:** The XKDR archive was exported at `2026-11-24T06:54:03+00:00` (`2026-11-24 12:24:03 IST`), making `2026-11` a partial month. The `65,760`-hour calendar-window denominator is preserved below for consistency with monthly tables, but it includes `155` post-export hours (`2026-11-24 13:00`–`2026-11-30 23:00`). Up to the export cutoff, the window contains `65,605` hours. **Active-period availability and active-period internal gaps are unaffected by this cutoff.**

**Processing & Validation:** All `90` partitions returned `status=ok` (`failed=0`, `unavailable=0`), yielding `234,645` filtered records (`540` monthly station–pollutant rows). Cross-check against the 2024 annual pilot (`72` rows) passed with zero discrepancies.

### Observation 8.1 — Station–pollutant historical availability summary

| Station | Pollutant | Full-Window (`65,760`h, incl. 155 post-export hrs) | Up-to-Export (`65,605`h) | Confirmed Active Span | Active-Period Availability | Zero-Record Months | Nulls | Duplicates |
|---|---|---:|---:|---|---:|---:|---:|---:|
| Vile Parle West (`site_5106`) | PM2.5 | 64.31% (`42,292`) | 64.46% | `2019-06-03 15:00` – `2025-08-30 02:00` | 77.30% | 19 | 0 | 0 |
| Vile Parle West (`site_5106`) | PM10 | 65.02% (`42,760`) | 65.18% | `2019-06-03 15:00` – `2025-08-30 02:00` | 78.16% | 19 | 0 | 0 |
| Chakala–Andheri East (`site_5399`) | PM2.5 | 58.97% (`38,776`) | 59.11% | `2020-12-08 09:00` – `2026-11-08 23:00` | 74.74% | 27 | 0 | 0 |
| Chakala–Andheri East (`site_5399`) | PM10 | 58.95% (`38,766`) | 59.09% | `2020-12-08 09:00` – `2026-11-08 23:00` | 74.72% | 27 | 0 | 0 |
| Malad West (`site_5102`) | PM2.5 | 60.06% (`39,495`) | 60.20% | `2019-06-03 14:00` – `2025-09-01 00:00` | 72.13% | 18 | 0 | 0 |
| Malad West (`site_5102`) | PM10 | 49.51% (`32,556`) | 49.62% | `2019-06-03 14:00` – `2025-09-01 00:00` | 59.46% | 27 | 0 | 0 |

### Observation 8.2 — Cross-month longest missing-hour gaps

| Station | Pollutant | Full-Window Longest Gap | Active-Period Longest Internal Gap |
|---|---|---|---|
| Vile Parle West (`site_5106`) | PM2.5 | `10,989` hrs (`2025-08-30 03:00` – `2026-11-30 23:00`) | `2,160` hrs (`2025-01-01 00:00` – `2025-03-31 23:00`) |
| Vile Parle West (`site_5106`) | PM10 | `10,989` hrs (`2025-08-30 03:00` – `2026-11-30 23:00`) | `2,160` hrs (`2025-01-01 00:00` – `2025-03-31 23:00`) |
| Chakala–Andheri East (`site_5399`) | PM2.5 | `13,353` hrs (`2019-06-01 00:00` – `2020-12-08 08:00`) | `5,931` hrs (`2024-07-08 13:00` – `2025-03-12 15:00`) |
| Chakala–Andheri East (`site_5399`) | PM10 | `13,353` hrs (`2019-06-01 00:00` – `2020-12-08 08:00`) | `5,943` hrs (`2024-07-08 01:00` – `2025-03-12 15:00`) |
| Malad West (`site_5102`) | PM2.5 | `10,943` hrs (`2025-09-01 01:00` – `2026-11-30 23:00`) | `2,160` hrs (`2025-01-01 00:00` – `2025-03-31 23:00`) |
| Malad West (`site_5102`) | PM10 | `10,943` hrs (`2025-09-01 01:00` – `2026-11-30 23:00`) | `6,745` hrs (`2020-10-25 16:00` – `2021-08-02 16:00`) |

### Observation 8.3 — Cadence and continuity findings

1. **Hourly timestamp resolution, with substantial reporting gaps:** Every active station–pollutant–month had `max_obs_per_hour = 1`, `min_interval_min = 60`, `median_interval_min = 60`, `0` nulls, and `0` duplicate timestamps. This establishes hourly timestamp resolution, not uninterrupted reporting, as all three stations exhibit multi-month gaps.
2. **2025 Q1 zero-record gap (`2025-01`–`2025-03`):** All three stations had zero `PM2.5` and `PM10` records in `2025-01` and `2025-02` (extending through `2025-03` for `site_5106` and `site_5102`, creating a `2,160`-hour internal gap).
3. **Divergent active windows:** `site_5106` and `site_5102` span `2019-06` to `2025-08`/`2025-09`, whereas `site_5399` PM2.5/PM10 starts `2020-12-08` and continues to `2026-11-08` (with an `~8`-month internal gap in `2024-07`–`2025-03`).
4. **Pollutant-specific outage at `site_5102`:** Malad West PM10 experienced a `6,745`-hour (~9.3-month) gap in `2020-10`–`2021-08` while PM2.5 continued reporting.

---

## 9. Measurement Quality & Anomaly Screening (2019–2026)

**Script:** `src/preprocessing/screen_measurement_quality.py`

**Scope:** All `234,645` filtered hourly observations across `site_5106`, `site_5399`, and `site_5102` (`PM2.5` and `PM10`).

### Observation 9.1 — Measurement distributions & invalid/zero counts

| Station | Pollutant | Observations | Nulls | Negatives | Zeros | Min | Median | Mean | P95 | P99 | Max |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Vile Parle West (`site_5106`) | PM2.5 | 42,292 | 0 | 0 | 0 | 0.01 | 28.20 | 42.53 | 115.28 | 167.22 | 985.00 |
| Vile Parle West (`site_5106`) | PM10 | 42,760 | 0 | 0 | 0 | 0.09 | 88.93 | 116.04 | 262.40 | 667.47 | 985.00 |
| Chakala–Andheri East (`site_5399`) | PM2.5 | 38,776 | 0 | 0 | 3 | 0.00 | 37.67 | 50.80 | 135.20 | 193.02 | 947.44 |
| Chakala–Andheri East (`site_5399`) | PM10 | 38,766 | 0 | 0 | 0 | 0.04 | 88.25 | 108.46 | 261.56 | 399.96 | 976.83 |
| Malad West (`site_5102`) | PM2.5 | 39,495 | 0 | 0 | 0 | 0.02 | 26.38 | 35.69 | 91.26 | 120.76 | 301.96 |
| Malad West (`site_5102`) | PM10 | 32,556 | 0 | 0 | 0 | 0.59 | 87.72 | 108.54 | 262.18 | 382.55 | 1000.00 |

- **Confirmed invalid values:** `0` negative values and `0` nulls across all `234,645` observations; only `3` exact `0.0` readings exist (`site_5399` PM2.5).

### Observation 9.2 — Extreme values, 1-hour jumps, and flatlines

- **Statistical extremes & 1-hour jumps (requiring investigation, not automatically invalid):** Every series exhibits right-skewed pollution tails (`>3.0×IQR`: `88`–`719` rows; `robust Z > 3.5`: `511`–`1,757` rows) and abrupt 1-hour jumps (`1h_spikes`: `568`–`1,033`; `1h_drops`: `581`–`1,058`).
- **Sensor-ceiling & stuck-value flatlines (`>= 4h`):**
  - `site_5106` PM2.5: `13` runs `>= 4h`, max `183` hours at `81.0` (`2023-02-03 14:00`–`2023-02-11 04:00`).
  - `site_5106` PM10: `20` runs `>= 4h`, max `34` hours; notably includes repeated `985.0` saturation flatlines (`24h`, `17h`, `13h`, `12h`, `9h`, `5h`) in May–August 2022 (which also inflate `site_5106` PM10 `P99 = 667.47`).
  - `site_5399` PM10: `2` runs `>= 4h`, max `159` hours at `31.63` (`2026-06-07 04:00`–`2026-06-13 18:00`).
  - `site_5102` PM2.5 & PM10: simultaneous flatlines in March 2020 (`101h` at `22.0` PM2.5; `98h` at `142.0` PM10).

### Observation 9.3 — Seasonal and station patterns

- All three stations show a consistent seasonal cycle: **Winter** (`PM2.5` mean `60.88–85.25`, `PM10` `160.41–166.47`) > **Post-Monsoon** (`50.45–62.58`, `121.76–139.12`) > **Pre-Monsoon** (`31.52–49.49`, `115.97–123.37`) > **Monsoon** (`13.85–18.12`, `47.07–70.27`).
- Differences in mean levels across stations (e.g., `site_5399` PM2.5 mean `50.80` vs. `site_5102` `35.69`) reflect local site/meteorological exposure and differing active windows as much as any sensor effects, and cannot be treated as sensor error without co-located reference data.

---

## 10. Open Research Questions

1. Do other Mumbai stations have better hourly coverage?
2. Are missing observations concentrated at particular hours or dates?
3. Are missing patterns shared between PM2.5 and PM10?
4. Are duplicate-looking stations actually separate instruments?
5. Can another source provide recent observations for live monitoring?
6. What minimum data-completeness threshold should ERR require for model training?

---

## 11. Important Data Principles

- Never replace missing pollutant measurements with zero automatically.
- Preserve original station IDs and source information.
- Distinguish observed readings from imputed values.
- Preserve timestamps in IST and handle timezone conversion explicitly.
- Evaluate station-level data quality before model training.
- Never claim forecast accuracy without chronological backtesting.
