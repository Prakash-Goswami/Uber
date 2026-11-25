"""
Automated Unit & Integration Tests for Uber Lakehouse Platform Components.

Validates configuration, geospatial H3 indexing, Haversine math, public data ingestion
(including live NYC TLC Taxi Zone & Open-Meteo API connectivity / fallback resilience),
real-time streaming & CDC event simulation, and Python AST syntax across all notebooks & DLT scripts.
"""

from __future__ import annotations

import ast
import glob
import json
import os
import tempfile
import unittest

import pandas as pd

from uber_platform.config import LakehouseConfig
from uber_platform.data_ingestion import (
    _generate_fallback_fhvhv_parquet,
    download_nyc_hourly_weather,
    download_taxi_zones_with_coords,
)
from uber_platform.geospatial import (
    get_zone_centroid,
    haversine_km,
    latlon_to_hex_python,
)
from uber_platform.realtime_simulator import (
    generate_cdc_batches,
    generate_streaming_telemetry_batches,
)


class TestUberLakehousePlatform(unittest.TestCase):
    def test_config_unity_catalog_vs_hive_fallback(self) -> None:
        uc_cfg = LakehouseConfig(catalog="uber_lakehouse", use_unity_catalog=True)
        self.assertEqual(uc_cfg.table_fqn("gold", "fact_trips"), "`uber_lakehouse`.`gold`.`fact_trips`")
        self.assertEqual(
            uc_cfg.volume_posix_path,
            "/Volumes/uber_lakehouse/raw_landing/landing_zone",
        )

        hive_cfg = LakehouseConfig(catalog="spark_catalog", use_unity_catalog=False)
        self.assertEqual(hive_cfg.table_fqn("silver", "trips_enriched"), "`uber_silver`.`trips_enriched`")
        self.assertTrue(hive_cfg.volume_spark_path.startswith(("file:", "dbfs:")))

    def test_geospatial_haversine_and_h3_indexing(self) -> None:
        # Midtown Center (161) to JFK Airport (132) ~ 21 km straight-line
        midtown_lat, midtown_lon = get_zone_centroid(161, "Manhattan")
        jfk_lat, jfk_lon = get_zone_centroid(132, "Queens")
        dist_km = haversine_km(midtown_lat, midtown_lon, jfk_lat, jfk_lon)
        self.assertGreater(dist_km, 18.0)
        self.assertLess(dist_km, 25.0)

        # Nearby points (~80m apart) in Times Square should map to same H3 Res-7 cell
        h7_a = latlon_to_hex_python(40.7589, -73.9851, resolution=7)
        h7_b = latlon_to_hex_python(40.7591, -73.9850, resolution=7)
        h7_jfk = latlon_to_hex_python(jfk_lat, jfk_lon, resolution=7)
        self.assertEqual(h7_a, h7_b)
        self.assertNotEqual(h7_a, h7_jfk)
        self.assertEqual(len(h7_a), 15)

        h8_a = latlon_to_hex_python(40.7589, -73.9851, resolution=8)
        self.assertTrue(h8_a.startswith("88"))

    def test_public_reference_ingestion_and_fhvhv_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            zones_csv = download_taxi_zones_with_coords(tmpdir, timeout_sec=15)
            self.assertTrue(os.path.exists(zones_csv))
            zones_df = pd.read_csv(zones_csv)
            self.assertGreaterEqual(len(zones_df), 260)
            self.assertTrue({"LocationID", "Borough", "Zone", "centroid_lat", "centroid_lon"}.issubset(zones_df.columns))

            weather_json = download_nyc_hourly_weather(
                tmpdir, start_date="2024-01-01", end_date="2024-01-03", timeout_sec=15
            )
            self.assertTrue(os.path.exists(weather_json))
            with open(weather_json, "r", encoding="utf-8") as fh:
                lines = [json.loads(line) for line in fh if line.strip()]
            self.assertEqual(len(lines), 72)  # 3 full days * 24 hours
            self.assertIn("precipitation_mm", lines[0])

            sample_pq = os.path.join(tmpdir, "fhvhv_test.parquet")
            _generate_fallback_fhvhv_parquet(sample_pq, num_rows=500)
            pq_df = pd.read_parquet(sample_pq)
            self.assertEqual(len(pq_df), 500)
            self.assertIn("HV0003", set(pq_df["hvfhs_license_num"]))
            self.assertIn("driver_pay", pq_df.columns)
            self.assertIn("base_passenger_fare", pq_df.columns)

    def test_realtime_simulator_and_cdc_generator(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            drv_dir = os.path.join(tmpdir, "cdc_drivers")
            rdr_dir = os.path.join(tmpdir, "cdc_riders")
            gps_dir = os.path.join(tmpdir, "gps")
            lc_dir = os.path.join(tmpdir, "lifecycle")

            cdc_res = generate_cdc_batches(drv_dir, rdr_dir, num_drivers=50, num_riders=100)
            self.assertGreater(cdc_res["driver_cdc_events"], 50)
            self.assertGreater(cdc_res["rider_cdc_events"], 100)

            stream_res = generate_streaming_telemetry_batches(
                gps_stream_dir=gps_dir,
                trip_stream_dir=lc_dir,
                num_batches=2,
                trips_per_batch=20,
                num_drivers=50,
                num_riders=100,
            )
            self.assertEqual(stream_res["batches_written"], 2)
            self.assertGreater(stream_res["gps_ping_events"], 100)
            self.assertGreater(stream_res["trip_lifecycle_events"], 80)

    def test_all_python_notebooks_and_pipelines_valid_ast(self) -> None:
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        patterns = [
            os.path.join(repo_root, "src", "uber_platform", "*.py"),
            os.path.join(repo_root, "notebooks", "*.py"),
            os.path.join(repo_root, "pipelines", "*.py"),
        ]
        checked = 0
        for pat in patterns:
            for py_file in glob.glob(pat):
                with open(py_file, "r", encoding="utf-8") as fh:
                    source = fh.read()
                ast.parse(source, filename=py_file)
                checked += 1
        self.assertGreaterEqual(checked, 15)


if __name__ == "__main__":
    unittest.main()
