import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "ingestion"))
import ingest_historical_era5 as era5  # noqa: E402


def make_mock_payload(
    grid_lat: float,
    grid_lon: float,
    start_date: str,
    end_date: str,
    tz: str = "Asia/Kolkata",
    utc_offset: int = 19800,
    drop_last_hour: bool = False,
    duplicate_first_hour: bool = False,
    null_first_temp: bool = False,
    oob_first_temp: bool = False,
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
    if duplicate_first_hour and times:
        times[1] = times[0]

    n = len(times)
    temps: list[float | None] = [25.0] * n
    if null_first_temp and n > 0:
        temps[0] = None
    if oob_first_temp and n > 0:
        temps[0] = 75.0  # > 50.0 upper bound

    return {
        "latitude": grid_lat,
        "longitude": grid_lon,
        "elevation": 10.0,
        "timezone": tz,
        "utc_offset_seconds": utc_offset,
        "hourly_units": {v: "unit" for v in era5.VARIABLES},
        "hourly": {
            "time": times,
            "temperature_2m": temps,
            "relative_humidity_2m": [60.0] * n,
            "wind_speed_10m": [10.0] * n,
            "wind_direction_10m": [180.0] * n,
            "precipitation": [0.0] * n,
            "surface_pressure": [1010.0] * n,
            "cloud_cover": [20.0] * n,
        },
    }


class TestHistoricalEra5Ingestion(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.raw_dir = self.root / "raw"
        self.manifest_path = self.root / "manifests" / "processing_manifest.json"
        self.units_path = self.root / "manifests" / "units.json"
        self.val_dir = self.root / "validation"
        self.two_chunks = [
            {"year": 2019, "start_date": "2019-06-01", "end_date": "2019-06-02", "expected_hours": 48},
            {"year": 2020, "start_date": "2020-01-01", "end_date": "2020-01-02", "expected_hours": 48},
        ]
        self.one_cell = [
            {"grid_id": "grid_19.0_73.0", "grid_lat": 19.0, "grid_lon": 73.0, "station_ids": ["site_5106"]}
        ]

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_1_retry_recovery(self) -> None:
        resp_503 = MagicMock(status_code=503)
        resp_200 = MagicMock(status_code=200)
        resp_200.json.return_value = make_mock_payload(19.0, 73.0, "2019-06-01", "2019-06-02")
        with patch.object(era5.requests, "get", side_effect=[resp_503, resp_200]), \
             patch.object(era5.time, "sleep"):
            payload, meta = era5.fetch_chunk_with_retries("grid_19.0_73.0", 19.0, 73.0, "2019-06-01", "2019-06-02")
        self.assertIsNotNone(payload)
        self.assertEqual(meta["attempts"], 2)
        self.assertEqual(meta["http_status"], 200)

    def test_2_permanent_api_failure(self) -> None:
        resp_404 = MagicMock(status_code=404)
        resp_404.raise_for_status.side_effect = requests.exceptions.HTTPError("404", response=resp_404)
        with patch.object(era5, "GRID_CELLS", self.one_cell), \
             patch.object(era5, "build_yearly_chunks", return_value=[self.two_chunks[0]]), \
             patch.object(era5.requests, "get", return_value=resp_404), \
             patch.object(era5.time, "sleep"):
            rc = era5.run_ingestion(self.raw_dir, self.manifest_path, self.units_path, self.val_dir, sleep_sec=0)
        self.assertEqual(rc, 1)
        manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        entry = manifest["chunks"]["grid_19.0_73.0|2019"]
        self.assertEqual(entry["validation_status"], "failed")
        self.assertEqual(entry["http_status"], 404)

    def test_3_interruption_during_download_and_resume(self) -> None:
        p1 = make_mock_payload(19.0, 73.0, "2019-06-01", "2019-06-02")
        p2 = make_mock_payload(19.0, 73.0, "2020-01-01", "2020-01-02")
        r1 = MagicMock(status_code=200)
        r1.json.return_value = p1
        r2 = MagicMock(status_code=200)
        r2.json.return_value = p2

        # First run: chunk 1 succeeds, chunk 2 fails on network error
        with patch.object(era5, "GRID_CELLS", self.one_cell), \
             patch.object(era5, "build_yearly_chunks", return_value=self.two_chunks), \
             patch.object(era5.requests, "get", side_effect=[r1] + [requests.exceptions.Timeout("boom")] * era5.MAX_RETRIES), \
             patch.object(era5.time, "sleep"):
            rc1 = era5.run_ingestion(self.raw_dir, self.manifest_path, self.units_path, self.val_dir, sleep_sec=0)
        self.assertEqual(rc1, 1)

        # Second run: chunk 1 is skipped (only 1 API call made for chunk 2)
        with patch.object(era5, "GRID_CELLS", self.one_cell), \
             patch.object(era5, "build_yearly_chunks", return_value=self.two_chunks), \
             patch.object(era5.requests, "get", return_value=r2) as mock_get, \
             patch.object(era5.time, "sleep"):
            rc2 = era5.run_ingestion(self.raw_dir, self.manifest_path, self.units_path, self.val_dir, sleep_sec=0)
        self.assertEqual(rc2, 0)
        self.assertEqual(mock_get.call_count, 1)

    def test_4_interrupted_file_writing_atomic(self) -> None:
        target = self.root / "test.json"
        target.write_text('{"original": true}', encoding="utf-8")
        with patch("os.replace", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                era5.atomic_write_json(target, {"new": True})
        self.assertEqual(json.loads(target.read_text(encoding="utf-8")), {"original": True})
        tmp_leftovers = list(self.root.glob("*.tmp"))
        self.assertEqual(tmp_leftovers, [])

    def test_5_manifest_file_inconsistency_triggers_refetch(self) -> None:
        p1 = make_mock_payload(19.0, 73.0, "2019-06-01", "2019-06-02")
        r1 = MagicMock(status_code=200)
        r1.json.return_value = p1
        with patch.object(era5, "GRID_CELLS", self.one_cell), \
             patch.object(era5, "build_yearly_chunks", return_value=[self.two_chunks[0]]), \
             patch.object(era5.requests, "get", return_value=r1):
            era5.run_ingestion(self.raw_dir, self.manifest_path, self.units_path, self.val_dir, sleep_sec=0)

        pq_file = self.raw_dir / "grid_19.0_73.0" / "2019.parquet"
        pq_file.unlink()  # Delete file while manifest still says ok
        ok, reason = era5.verify_existing_chunk(
            "grid_19.0_73.0|2019", era5.load_manifest(self.manifest_path), pq_file,
            "grid_19.0_73.0", "2019-06-01", "2019-06-02", 48
        )
        self.assertFalse(ok)
        self.assertIn("missing", reason)

    def test_6_checksum_mismatch_triggers_refetch(self) -> None:
        p1 = make_mock_payload(19.0, 73.0, "2019-06-01", "2019-06-02")
        r1 = MagicMock(status_code=200)
        r1.json.return_value = p1
        with patch.object(era5, "GRID_CELLS", self.one_cell), \
             patch.object(era5, "build_yearly_chunks", return_value=[self.two_chunks[0]]), \
             patch.object(era5.requests, "get", return_value=r1):
            era5.run_ingestion(self.raw_dir, self.manifest_path, self.units_path, self.val_dir, sleep_sec=0)

        manifest = era5.load_manifest(self.manifest_path)
        manifest["chunks"]["grid_19.0_73.0|2019"]["sha256"] = "0" * 64
        pq_file = self.raw_dir / "grid_19.0_73.0" / "2019.parquet"
        ok, reason = era5.verify_existing_chunk(
            "grid_19.0_73.0|2019", manifest, pq_file,
            "grid_19.0_73.0", "2019-06-01", "2019-06-02", 48
        )
        self.assertFalse(ok)
        self.assertIn("checksum", reason)

    def test_7_corrupt_parquet_file_detected(self) -> None:
        p1 = make_mock_payload(19.0, 73.0, "2019-06-01", "2019-06-02")
        r1 = MagicMock(status_code=200)
        r1.json.return_value = p1
        with patch.object(era5, "GRID_CELLS", self.one_cell), \
             patch.object(era5, "build_yearly_chunks", return_value=[self.two_chunks[0]]), \
             patch.object(era5.requests, "get", return_value=r1):
            era5.run_ingestion(self.raw_dir, self.manifest_path, self.units_path, self.val_dir, sleep_sec=0)

        pq_file = self.raw_dir / "grid_19.0_73.0" / "2019.parquet"
        pq_file.write_bytes(b"not a parquet file")
        manifest = era5.load_manifest(self.manifest_path)
        manifest["chunks"]["grid_19.0_73.0|2019"]["sha256"] = era5.compute_sha256(pq_file)
        ok, reason = era5.verify_existing_chunk(
            "grid_19.0_73.0|2019", manifest, pq_file,
            "grid_19.0_73.0", "2019-06-01", "2019-06-02", 48
        )
        self.assertFalse(ok)
        self.assertIn("corrupt", reason)

    def test_8_incorrect_timestamp_boundaries_and_duplicates(self) -> None:
        for kw in [{"drop_last_hour": True}, {"duplicate_first_hour": True}]:
            r = MagicMock(status_code=200)
            r.json.return_value = make_mock_payload(19.0, 73.0, "2019-06-01", "2019-06-02", **kw)
            with patch.object(era5, "GRID_CELLS", self.one_cell), \
                 patch.object(era5, "build_yearly_chunks", return_value=[self.two_chunks[0]]), \
                 patch.object(era5.requests, "get", return_value=r):
                rc = era5.run_ingestion(self.raw_dir, self.manifest_path, self.units_path, self.val_dir, sleep_sec=0)
            self.assertEqual(rc, 1)
            manifest = era5.load_manifest(self.manifest_path)
            self.assertEqual(manifest["chunks"]["grid_19.0_73.0|2019"]["validation_status"], "failed")

    def test_9_null_preserved_and_oob_requires_review(self) -> None:
        r = MagicMock(status_code=200)
        r.json.return_value = make_mock_payload(
            19.0, 73.0, "2019-06-01", "2019-06-02", null_first_temp=True
        )
        with patch.object(era5, "GRID_CELLS", self.one_cell), \
             patch.object(era5, "build_yearly_chunks", return_value=[self.two_chunks[0]]), \
             patch.object(era5.requests, "get", return_value=r):
            rc = era5.run_ingestion(self.raw_dir, self.manifest_path, self.units_path, self.val_dir, sleep_sec=0)
        self.assertEqual(rc, 0)
        manifest = era5.load_manifest(self.manifest_path)
        self.assertEqual(manifest["chunks"]["grid_19.0_73.0|2019"]["total_nulls"], 1)

        # Out-of-bounds value -> review_required and rc == 1
        r_oob = MagicMock(status_code=200)
        r_oob.json.return_value = make_mock_payload(
            19.0, 73.0, "2019-06-01", "2019-06-02", oob_first_temp=True
        )
        manifest_path_2 = self.root / "manifests" / "m2.json"
        with patch.object(era5, "GRID_CELLS", self.one_cell), \
             patch.object(era5, "build_yearly_chunks", return_value=[self.two_chunks[0]]), \
             patch.object(era5.requests, "get", return_value=r_oob):
            rc_oob = era5.run_ingestion(self.raw_dir / "oob", manifest_path_2, self.units_path, self.val_dir, sleep_sec=0)
        self.assertEqual(rc_oob, 1)
        m2 = era5.load_manifest(manifest_path_2)
        self.assertEqual(m2["chunks"]["grid_19.0_73.0|2019"]["validation_status"], "review_required")
        self.assertTrue((self.raw_dir / "oob" / "grid_19.0_73.0" / "2019.parquet").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
