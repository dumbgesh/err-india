import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "ingestion"))
import ingest_archive_forecasts as fc  # noqa: E402


def make_mock_prev_runs_payload(
    grid_lat: float,
    grid_lon: float,
    start_date: str,
    end_date: str,
    tz: str = "Asia/Kolkata",
    utc_offset: int = 19800,
    drop_last_hour: bool = False,
    duplicate_first_hour: bool = False,
    mismatch_var: str | None = None,
    null_day3_temp: bool = False,
    oob_day1_temp: bool = False,
) -> dict:
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23)
    times: list[str] = []
    cur = start_dt
    while cur <= end_dt:
        times.append(cur.strftime("%Y-%m-%dT%H:%M"))
        cur += timedelta(hours=1)

    if drop_last_hour and times:
        times.pop()
    if duplicate_first_hour and len(times) >= 2:
        times[1] = times[0]

    n = len(times)
    hourly: dict = {"time": times}
    defaults = {
        "temperature_2m": 25.0,
        "relative_humidity_2m": 60.0,
        "wind_speed_10m": 10.0,
        "wind_direction_10m": 180.0,
        "precipitation": 0.0,
        "surface_pressure": 1010.0,
        "cloud_cover": 20.0,
    }
    for var, base in defaults.items():
        for lead_hr, suffix in fc.LEAD_OFFSETS:
            col = f"{var}_{suffix}"
            arr = [base + (lead_hr / 100.0)] * n
            if col == mismatch_var and n > 0:
                arr.pop()
            if null_day3_temp and col == "temperature_2m_previous_day3" and n > 0:
                arr[0] = None
            if oob_day1_temp and col == "temperature_2m_previous_day1" and n > 0:
                arr[0] = 70.0
            hourly[col] = arr

    return {
        "latitude": grid_lat,
        "longitude": grid_lon,
        "elevation": 10.0,
        "timezone": tz,
        "utc_offset_seconds": utc_offset,
        "hourly_units": {c: "unit" for c in fc.ALL_21_HOURLY_VARS},
        "hourly": hourly,
    }


class TestArchivedForecastIngestion(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.raw_dir = self.root / "raw"
        self.manifest_path = self.root / "manifests" / "processing_manifest.json"
        self.units_path = self.root / "manifests" / "units.json"
        self.val_dir = self.root / "validation"
        self.one_cell = [
            {"grid_id": "grid_19.125_72.875", "grid_lat": 19.125, "grid_lon": 72.875, "station_ids": ["site_5106"]}
        ]
        self.two_months = [
            {"ym": "2024-01", "year": 2024, "month": 1, "start_date": "2024-01-01", "end_date": "2024-01-02", "expected_valid_hours": 48, "expected_parquet_rows": 144},
            {"ym": "2024-02", "year": 2024, "month": 2, "start_date": "2024-02-01", "end_date": "2024-02-02", "expected_valid_hours": 48, "expected_parquet_rows": 144},
        ]

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_january_probe_success_and_failure(self) -> None:
        r_ok = MagicMock(status_code=200)
        r_ok.json.side_effect = [
            make_mock_prev_runs_payload(19.125, 72.875, "2024-01-01", "2024-01-03"),
            make_mock_prev_runs_payload(19.25, 72.875, "2024-01-01", "2024-01-03"),
        ]
        with patch.object(fc.requests, "get", return_value=r_ok):
            rc = fc.run_january_probe(self.val_dir)
        self.assertEqual(rc, 0)
        self.assertTrue((self.val_dir / "january_2024_probe.json").exists())

        r_fail = MagicMock(status_code=500)
        r_fail.raise_for_status.side_effect = requests.exceptions.HTTPError("500", response=r_fail)
        with patch.object(fc.requests, "get", return_value=r_fail), patch.object(fc.time, "sleep"):
            rc_bad = fc.run_january_probe(self.val_dir)
        self.assertEqual(rc_bad, 1)

    def test_retry_recovery_and_permanent_failure(self) -> None:
        r503 = MagicMock(status_code=503)
        r200 = MagicMock(status_code=200)
        r200.json.return_value = make_mock_prev_runs_payload(19.125, 72.875, "2024-01-01", "2024-01-02")
        with patch.object(fc.requests, "get", side_effect=[r503, r200]), patch.object(fc.time, "sleep"):
            payload, meta = fc.fetch_previous_runs_chunk("grid_19.125_72.875", 19.125, 72.875, "2024-01-01", "2024-01-02")
        self.assertIsNotNone(payload)
        self.assertEqual(meta["attempts"], 2)

        r404 = MagicMock(status_code=404)
        r404.raise_for_status.side_effect = requests.exceptions.HTTPError("404", response=r404)
        with patch.object(fc.requests, "get", return_value=r404), patch.object(fc.time, "sleep"):
            p_fail, m_fail = fc.fetch_previous_runs_chunk("grid_19.125_72.875", 19.125, 72.875, "2024-01-01", "2024-01-02")
        self.assertIsNone(p_fail)
        self.assertEqual(m_fail["http_status"], 404)

    def test_timezone_and_array_length_validation(self) -> None:
        bad_tz = make_mock_prev_runs_payload(19.125, 72.875, "2024-01-01", "2024-01-01", tz="GMT", utc_offset=0)
        with self.assertRaises(ValueError):
            fc.validate_previous_runs_response(bad_tz)

        bad_len = make_mock_prev_runs_payload(19.125, 72.875, "2024-01-01", "2024-01-01", mismatch_var="cloud_cover_previous_day3")
        with self.assertRaises(ValueError):
            fc.validate_previous_runs_response(bad_len)

    def test_long_format_conversion_and_21_variables(self) -> None:
        self.assertEqual(len(fc.ALL_21_HOURLY_VARS), 21)
        payload = make_mock_prev_runs_payload(19.125, 72.875, "2024-01-01", "2024-01-02")
        exp_hours = fc.build_window_hours("2024-01-01", "2024-01-02")
        long_rows, report = fc.reshape_to_long_and_validate("grid_19.125_72.875", payload, exp_hours)
        self.assertEqual(report["long_rows"], 144)
        leads = {r["lead_hours_nominal"] for r in long_rows}
        self.assertEqual(leads, {24, 48, 72})
        # Verify lead-time separation preserved distinct values (25.24 vs 25.48 vs 25.72)
        temps_t0 = {r["lead_hours_nominal"]: r["temperature_2m"] for r in long_rows[:3]}
        self.assertAlmostEqual(temps_t0[24], 25.24)
        self.assertAlmostEqual(temps_t0[48], 25.48)
        self.assertAlmostEqual(temps_t0[72], 25.72)

    def test_null_preservation_and_oob_review_status(self) -> None:
        r_null = MagicMock(status_code=200)
        r_null.json.return_value = make_mock_prev_runs_payload(19.125, 72.875, "2024-01-01", "2024-01-02", null_day3_temp=True)
        with patch.object(fc, "ICON_GRID_CELLS", self.one_cell), \
             patch.object(fc, "build_monthly_chunks", return_value=[self.two_months[0]]), \
             patch.object(fc.requests, "get", return_value=r_null):
            rc = fc.run_ingestion(self.raw_dir, self.manifest_path, self.units_path, self.val_dir, sleep_sec=0)
        self.assertEqual(rc, 0)
        m = fc.load_manifest(self.manifest_path)
        self.assertEqual(m["chunks"]["grid_19.125_72.875|2024-01"]["total_null_cells"], 1)

        r_oob = MagicMock(status_code=200)
        r_oob.json.return_value = make_mock_prev_runs_payload(19.125, 72.875, "2024-01-01", "2024-01-02", oob_day1_temp=True)
        m2_path = self.root / "manifests" / "m2.json"
        with patch.object(fc, "ICON_GRID_CELLS", self.one_cell), \
             patch.object(fc, "build_monthly_chunks", return_value=[self.two_months[0]]), \
             patch.object(fc.requests, "get", return_value=r_oob):
            rc_oob = fc.run_ingestion(self.raw_dir / "oob", m2_path, self.units_path, self.val_dir, sleep_sec=0)
        self.assertEqual(rc_oob, 1)
        m2 = fc.load_manifest(m2_path)
        self.assertEqual(m2["chunks"]["grid_19.125_72.875|2024-01"]["validation_status"], "review_required")

    def test_duplicate_and_missing_timestamps(self) -> None:
        for kw in [{"drop_last_hour": True}, {"duplicate_first_hour": True}]:
            r = MagicMock(status_code=200)
            r.json.return_value = make_mock_prev_runs_payload(19.125, 72.875, "2024-01-01", "2024-01-02", **kw)
            with patch.object(fc, "ICON_GRID_CELLS", self.one_cell), \
                 patch.object(fc, "build_monthly_chunks", return_value=[self.two_months[0]]), \
                 patch.object(fc.requests, "get", return_value=r):
                rc = fc.run_ingestion(self.raw_dir, self.manifest_path, self.units_path, self.val_dir, sleep_sec=0)
            self.assertEqual(rc, 1)

    def test_interrupted_resume_checksum_and_corrupt_detection(self) -> None:
        r1 = MagicMock(status_code=200)
        r1.json.return_value = make_mock_prev_runs_payload(19.125, 72.875, "2024-01-01", "2024-01-02")
        r2 = MagicMock(status_code=200)
        r2.json.return_value = make_mock_prev_runs_payload(19.125, 72.875, "2024-02-01", "2024-02-02")

        with patch.object(fc, "ICON_GRID_CELLS", self.one_cell), \
             patch.object(fc, "build_monthly_chunks", return_value=self.two_months), \
             patch.object(fc.requests, "get", side_effect=[r1] + [requests.exceptions.Timeout("net")] * fc.MAX_RETRIES), \
             patch.object(fc.time, "sleep"):
            rc1 = fc.run_ingestion(self.raw_dir, self.manifest_path, self.units_path, self.val_dir, sleep_sec=0)
        self.assertEqual(rc1, 1)

        with patch.object(fc, "ICON_GRID_CELLS", self.one_cell), \
             patch.object(fc, "build_monthly_chunks", return_value=self.two_months), \
             patch.object(fc.requests, "get", return_value=r2) as mock_get:
            rc2 = fc.run_ingestion(self.raw_dir, self.manifest_path, self.units_path, self.val_dir, sleep_sec=0)
        self.assertEqual(rc2, 0)
        self.assertEqual(mock_get.call_count, 1)

        pq = self.raw_dir / "grid_19.125_72.875" / "2024-01.parquet"
        m = fc.load_manifest(self.manifest_path)
        m["chunks"]["grid_19.125_72.875|2024-01"]["sha256"] = "0" * 64
        ok_sha, _ = fc.verify_existing_chunk("grid_19.125_72.875|2024-01", m, pq, "grid_19.125_72.875", "2024-01-01", "2024-01-02", 144)
        self.assertFalse(ok_sha)

        pq.write_bytes(b"corrupt")
        m["chunks"]["grid_19.125_72.875|2024-01"]["sha256"] = fc.compute_sha256(pq)
        ok_corrupt, _ = fc.verify_existing_chunk("grid_19.125_72.875|2024-01", m, pq, "grid_19.125_72.875", "2024-01-01", "2024-01-02", 144)
        self.assertFalse(ok_corrupt)


if __name__ == "__main__":
    unittest.main(verbosity=2)
