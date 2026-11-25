"""
Public Dataset Downloader & Bronze Ingestion Utilities.

Fetches 100% free, real-world datasets with zero API keys required:
1. NYC TLC HVFHV Trip Records (`HV0003` = Actual Uber NYC trips) — Parquet via CloudFront
2. NYC TLC Taxi Zone Lookup (265 zones enriched with WGS84 coordinates) — CSV via CloudFront
3. Open-Meteo Historical Weather Archive (Hourly NYC observations) — REST JSON API

Includes memory-safe streaming downloads, configurable row-group sampling via PyArrow,
and automatic high-fidelity fallback generation if running in an air-gapped VPC without internet.
"""

from __future__ import annotations

import csv
import datetime as dt
import io
import json
import math
import os
import random
import tempfile
from typing import Any, Dict, List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from uber_platform.geospatial import KNOWN_NYC_ZONE_COORDS, get_zone_centroid

TLC_HVFHV_URL_TEMPLATE = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{year_month}.parquet"
)
TLC_TAXI_ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


def download_taxi_zones_with_coords(dest_dir: str, timeout_sec: int = 30) -> str:
    """
    Download official NYC TLC `taxi_zone_lookup.csv` and enrich every LocationID (1..265)
    with realistic `(centroid_lat, centroid_lon)` coordinates.
    """
    os.makedirs(dest_dir, exist_ok=True)
    out_csv_path = os.path.join(dest_dir, "taxi_zones_enriched.csv")

    rows: List[Dict[str, Any]] = []
    try:
        resp = requests.get(TLC_TAXI_ZONE_URL, timeout=timeout_sec)
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.text))
        for r in reader:
            loc_id = int(r["LocationID"])
            borough = (r.get("Borough") or "Manhattan").strip()
            zone = (r.get("Zone") or f"Zone {loc_id}").strip()
            service_zone = (r.get("service_zone") or "Boro Zone").strip()
            lat, lon = get_zone_centroid(loc_id, borough)
            rows.append(
                {
                    "LocationID": loc_id,
                    "Borough": borough,
                    "Zone": zone,
                    "service_zone": service_zone,
                    "centroid_lat": lat,
                    "centroid_lon": lon,
                }
            )
    except Exception as exc:
        print(f"[WARN] Could not reach {TLC_TAXI_ZONE_URL} ({exc}); generating fallback 265 NYC zones.")
        rows = _generate_fallback_taxi_zones()

    df = pd.DataFrame(rows)
    df.to_csv(out_csv_path, index=False)
    return out_csv_path


def download_nyc_hourly_weather(
    dest_dir: str,
    start_date: str = "2024-01-01",
    end_date: str = "2024-01-31",
    timeout_sec: int = 30,
) -> str:
    """
    Fetch real hourly NYC weather from Open-Meteo Archive API (free, no API key)
    and write newline-delimited JSON (`nyc_weather_hourly.json`).
    """
    os.makedirs(dest_dir, exist_ok=True)
    out_json_path = os.path.join(dest_dir, "nyc_weather_hourly.json")

    records: List[Dict[str, Any]] = []
    try:
        params = {
            "latitude": 40.7829,
            "longitude": -73.9654,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": "temperature_2m,precipitation,rain,snowfall,wind_speed_10m,weather_code",
            "timezone": "UTC",
        }
        resp = requests.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=timeout_sec)
        resp.raise_for_status()
        hourly = resp.json()["hourly"]
        times = hourly["time"]
        for i, ts_str in enumerate(times):
            records.append(
                {
                    "observation_hour_utc": ts_str,
                    "temp_celsius": float(hourly["temperature_2m"][i] or 0.0),
                    "precipitation_mm": float(hourly["precipitation"][i] or 0.0),
                    "rain_mm": float(hourly["rain"][i] or 0.0),
                    "snowfall_cm": float(hourly["snowfall"][i] or 0.0),
                    "wind_speed_kmh": float(hourly["wind_speed_10m"][i] or 0.0),
                    "weather_code": int(hourly["weather_code"][i] or 0),
                }
            )
    except Exception as exc:
        print(f"[WARN] Could not reach Open-Meteo ({exc}); generating realistic NYC Jan 2024 weather.")
        records = _generate_fallback_weather(start_date, end_date)

    with open(out_json_path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")

    return out_json_path


def download_fhvhv_uber_parquet(
    dest_dir: str,
    year_month: str = "2024-01",
    sample_rows: int = 150_000,
    timeout_sec: int = 180,
) -> str:
    """
    Download official NYC TLC High-Volume For-Hire Vehicle (HVFHV) Parquet dataset.

    - `HV0003` in `hvfhs_license_num` represents **actual Uber trips** in New York City.
    - If `sample_rows > 0`, streams the download to a temporary file, extracts `sample_rows`
      evenly or from initial row groups via PyArrow without blowing up driver RAM, writes
      a clean Parquet file into `dest_dir`, and cleans up temporary storage.
    - If `sample_rows <= 0`, preserves the full ~19.6M row monthly Parquet file.
    """
    os.makedirs(dest_dir, exist_ok=True)
    suffix = f"sample_{sample_rows}" if sample_rows and sample_rows > 0 else "full"
    target_parquet = os.path.join(dest_dir, f"fhvhv_tripdata_{year_month}_{suffix}.parquet")

    if os.path.exists(target_parquet) and os.path.getsize(target_parquet) > 10_000:
        print(f"[INFO] Reusing cached HVFHV Parquet at {target_parquet}")
        return target_parquet

    url = TLC_HVFHV_URL_TEMPLATE.format(year_month=year_month)
    tmp_path = None
    try:
        print(f"[INFO] Downloading NYC TLC HVFHV Parquet from {url} ...")
        with requests.get(url, stream=True, timeout=timeout_sec) as resp:
            resp.raise_for_status()
            if not sample_rows or sample_rows <= 0:
                with open(target_parquet, "wb") as out_f:
                    for chunk in resp.iter_content(chunk_size=4 * 1024 * 1024):
                        if chunk:
                            out_f.write(chunk)
                return target_parquet

            with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp_f:
                tmp_path = tmp_f.name
                for chunk in resp.iter_content(chunk_size=4 * 1024 * 1024):
                    if chunk:
                        tmp_f.write(chunk)

        pf = pq.ParquetFile(tmp_path)
        batches: List[pa.RecordBatch] = []
        collected = 0
        # Spread sampling across multiple row groups across the month so multiple days & weather conditions are covered!
        num_rgs = pf.num_row_groups
        step = max(1, num_rgs // 15)
        per_rg_cap = max(2_000, math.ceil(sample_rows / max(1, min(num_rgs, 15))))

        for rg_idx in range(0, num_rgs, step):
            table_rg = pf.read_row_group(rg_idx)
            slice_len = min(len(table_rg), per_rg_cap, sample_rows - collected)
            batches.extend(table_rg.slice(0, slice_len).to_batches())
            collected += slice_len
            if collected >= sample_rows:
                break

        # Top up from initial row groups if needed
        rg_idx = 0
        while collected < sample_rows and rg_idx < num_rgs:
            if rg_idx % step != 0:
                table_rg = pf.read_row_group(rg_idx)
                slice_len = min(len(table_rg), sample_rows - collected)
                batches.extend(table_rg.slice(0, slice_len).to_batches())
                collected += slice_len
            rg_idx += 1

        sampled_table = pa.Table.from_batches(batches)
        pq.write_table(sampled_table, target_parquet, compression="snappy")
        print(
            f"[INFO] Wrote {sampled_table.num_rows:,} real NYC HVFHV trips across {num_rgs} row-groups to {target_parquet}"
        )
        return target_parquet

    except Exception as exc:
        print(
            f"[WARN] Direct download from {url} failed ({exc}); generating realistic HVFHV fallback Parquet."
        )
        fallback_rows = sample_rows if (sample_rows and sample_rows > 0) else 50_000
        _generate_fallback_fhvhv_parquet(target_parquet, num_rows=fallback_rows)
        return target_parquet
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


def _generate_fallback_taxi_zones() -> List[Dict[str, Any]]:
    boroughs = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
    named_landmarks = {
        1: ("EWR", "Newark Airport", "EWR"),
        132: ("Queens", "JFK Airport", "Airports"),
        138: ("Queens", "LaGuardia Airport", "Airports"),
        161: ("Manhattan", "Midtown Center", "Yellow Zone"),
        230: ("Manhattan", "Times Sq/Theatre District", "Yellow Zone"),
        186: ("Manhattan", "Penn Station/Madison Sq West", "Yellow Zone"),
        79: ("Manhattan", "East Village", "Yellow Zone"),
        87: ("Manhattan", "Financial District North", "Yellow Zone"),
        255: ("Brooklyn", "Williamsburg (North Side)", "Boro Zone"),
        65: ("Brooklyn", "Downtown Brooklyn/MetroTech", "Boro Zone"),
        7: ("Queens", "Astoria", "Boro Zone"),
    }
    rows = []
    for loc_id in range(1, 266):
        if loc_id in named_landmarks:
            borough, zone_name, svc = named_landmarks[loc_id]
        else:
            borough = boroughs[loc_id % len(boroughs)]
            zone_name = f"{borough} Zone {loc_id}"
            svc = "Yellow Zone" if borough == "Manhattan" else "Boro Zone"
        lat, lon = get_zone_centroid(loc_id, borough)
        rows.append(
            {
                "LocationID": loc_id,
                "Borough": borough,
                "Zone": zone_name,
                "service_zone": svc,
                "centroid_lat": lat,
                "centroid_lon": lon,
            }
        )
    return rows


def _generate_fallback_weather(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    rng = random.Random(42)
    start_dt = dt.datetime.fromisoformat(f"{start_date}T00:00:00")
    end_dt = dt.datetime.fromisoformat(f"{end_date}T23:00:00")
    cur = start_dt
    out = []
    while cur <= end_dt:
        diurnal = math.sin((cur.hour - 6) / 24.0 * 2.0 * math.pi) * 3.5
        is_storm = (cur.day in (6, 9, 16, 25)) and (8 <= cur.hour <= 20)
        rain = round(rng.uniform(1.5, 7.5), 2) if (is_storm and cur.day != 16) else 0.0
        snow = round(rng.uniform(0.8, 3.2), 2) if (is_storm and cur.day == 16) else 0.0
        precip = round(rain + snow * 1.1, 2)
        temp = round((0.5 if snow > 0 else 4.5) + diurnal + rng.uniform(-1.5, 1.5), 1)
        wind = round((28.0 if is_storm else 12.0) + rng.uniform(-4.0, 8.0), 1)
        w_code = 73 if snow > 0 else (63 if rain > 3.0 else (61 if rain > 0 else 1))
        out.append(
            {
                "observation_hour_utc": cur.strftime("%Y-%m-%dT%H:00"),
                "temp_celsius": temp,
                "precipitation_mm": precip,
                "rain_mm": rain,
                "snowfall_cm": snow,
                "wind_speed_kmh": max(2.0, wind),
                "weather_code": w_code,
            }
        )
        cur += dt.timedelta(hours=1)
    return out


def _generate_fallback_fhvhv_parquet(target_parquet: str, num_rows: int = 25_000) -> None:
    """Deterministic fallback generator matching exact NYC TLC HVFHV schema."""
    rng = random.Random(202401)
    popular_zones = list(KNOWN_NYC_ZONE_COORDS.keys())
    base_start = dt.datetime(2024, 1, 1, 0, 0, 0)

    records = []
    for i in range(num_rows):
        license_num = "HV0003" if rng.random() < 0.78 else ("HV0005" if rng.random() < 0.85 else "HV0004")
        day_offset = rng.randint(0, 30)
        # Weight toward rush hours and evenings
        hour_choices = [7, 8, 8, 9, 12, 14, 17, 17, 18, 18, 19, 20, 22, rng.randint(0, 23)]
        hour = rng.choice(hour_choices)
        req_ts = base_start + dt.timedelta(
            days=day_offset, hours=hour, minutes=rng.randint(0, 59), seconds=rng.randint(0, 59)
        )
        arrival_sec = rng.randint(90, 480)
        curb_sec = rng.randint(20, 150)
        on_scene_ts = req_ts + dt.timedelta(seconds=arrival_sec)
        pickup_ts = on_scene_ts + dt.timedelta(seconds=curb_sec)

        pu_id = rng.choice(popular_zones)
        do_id = rng.choice(popular_zones)
        miles = round(max(0.4, rng.lognormvariate(1.1, 0.75)), 2)
        speed_mph = rng.uniform(11.0, 28.0) if pu_id not in (1, 132, 138) else rng.uniform(22.0, 42.0)
        trip_time = int(max(120, (miles / speed_mph) * 3600.0))
        dropoff_ts = pickup_ts + dt.timedelta(seconds=trip_time)

        surge = 1.35 if hour in (8, 17, 18, 19) else (1.55 if day_offset in (5, 8, 15, 24) else 1.0)
        base_fare = round((2.50 + 1.85 * miles + 0.60 * (trip_time / 60.0)) * surge, 2)
        is_airport = pu_id in (1, 132, 138) or do_id in (1, 132, 138)
        airport_fee = 2.50 if is_airport else 0.0
        congestion = 2.75 if pu_id >= 48 and pu_id <= 249 else 0.0
        tolls = 6.94 if is_airport and rng.random() < 0.5 else 0.0
        bcf = round(base_fare * 0.0275, 2)
        sales_tax = round(base_fare * 0.08875, 2)
        tips = round(base_fare * rng.choice([0.0, 0.0, 0.15, 0.20, 0.25]), 2)
        driver_pay = round(base_fare * rng.uniform(0.68, 0.84), 2)
        shared_req = "Y" if rng.random() < 0.06 else "N"
        shared_match = "Y" if (shared_req == "Y" and rng.random() < 0.55) else "N"
        wav_req = "Y" if rng.random() < 0.02 else "N"

        # Inject ~0.8% realistic DQ anomalies to exercise Silver Quarantine rules
        if i % 130 == 0:
            miles = -1.5
        elif i % 211 == 0:
            base_fare = -12.0

        records.append(
            {
                "hvfhs_license_num": license_num,
                "dispatching_base_num": "B03404" if license_num == "HV0003" else "B03406",
                "originating_base_num": "B03404" if license_num == "HV0003" else "B03406",
                "request_datetime": req_ts,
                "on_scene_datetime": on_scene_ts,
                "pickup_datetime": pickup_ts,
                "dropoff_datetime": dropoff_ts,
                "PULocationID": pu_id,
                "DOLocationID": do_id,
                "trip_miles": miles,
                "trip_time": trip_time,
                "base_passenger_fare": base_fare,
                "tolls": tolls,
                "bcf": bcf,
                "sales_tax": sales_tax,
                "congestion_surcharge": congestion,
                "airport_fee": airport_fee,
                "tips": tips,
                "driver_pay": driver_pay,
                "shared_request_flag": shared_req,
                "shared_match_flag": shared_match,
                "access_a_ride_flag": "N",
                "wav_request_flag": wav_req,
                "wav_match_flag": wav_req,
            }
        )

    df = pd.DataFrame(records)
    df.to_parquet(target_parquet, index=False)
