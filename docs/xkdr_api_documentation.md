## Endpoints

Every request carries your key as a bearer token. All endpoints accept `format=json|csv|parquet`.

Interactive documentation with a try-it-out console is available at [/v1/docs](https://airquality.xkdr.org/v1/docs).

| Endpoint | Description |
|---|---|
| `GET /v1/meta` | Returns published data information, including export time, months covered, row counts, schema, your tier, and row cap. |
| `GET /v1/stations` | Returns stations with coordinates, first and last readings, and reported pollutants. Supports filters: `source`, `state`, `city`, `q`. |
| `GET /v1/parameters` | Returns available pollutants, units, and station counts. |
| `GET /v1/measurements` | Returns air quality time-series measurements. |
| `GET /v1/files` | Lists bulk files with their sizes and row counts. |
| `GET /v1/files/<key>` | Downloads an individual bulk file. Supports `Range` requests, allowing query engines to read data in place. |

### Measurements Parameters

| Parameter | Description |
|---|---|
| `station` | A `station_id`, such as `site_103` or `DS1010001`. Can be repeated for multiple stations. |
| `parameter` | Pollutant name, such as `PM2.5`, `PM10`, `NO2`, `SO2`, `CO`, `Ozone`, `NH3`, `Benzene`, etc. Can be repeated for multiple pollutants. |
| `city` | Case-insensitive exact city match, e.g. `city=Delhi`. |
| `state` | Case-insensitive exact state match. |
| `source` | Data source: `cpcb_caaqm` or `us_embassy`. |
| `start` | Start date in `YYYY-MM-DD` format (inclusive). |
| `end` | End date in `YYYY-MM-DD` format (inclusive). |
| `agg` | Aggregation level: `raw` (default), `hourly`, `daily`, or `monthly`. Aggregated results include mean, minimum, maximum, and count per period. |
| `limit` | Optional row cap for the request. |

**Date handling:** Dates use Indian Standard Time (IST). If `start` and `end` are not specified, the API defaults to the last 30 days.

### Response Headers

API responses include the following headers:

| Header | Description |
|---|---|
| `X-Row-Count` | Number of rows returned. |
| `X-Truncated` | Indicates whether the response was truncated. Returns `true` only when a `limit` parameter was explicitly provided. |