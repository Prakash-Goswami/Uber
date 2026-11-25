"""
Real-Time Event Simulator for Databricks Structured Streaming & Auto Loader.

Generates realistic micro-batch JSON files into landing directories:
1. `driver_gps_pings`: High-frequency (5s interval) driver GPS telemetry across NYC zones,
   including interpolated active trip routes, idle roaming drivers, ~2% mobile network retry
   duplicates, ~3% delayed out-of-order pings, and ~0.5% malformed schema-drift events.
2. `trip_lifecycle_events`: Event-driven ride state machine transitions:
   `REQUESTED -> DISPATCHED -> DRIVER_ON_SCENE -> IN_PROGRESS -> COMPLETED`
   plus realistic `CANCELLED_BY_RIDER` and `CANCELLED_BY_DRIVER` funnels.
3. `cdc_drivers` & `cdc_riders`: Debezium-style CDC (`op = 'c' | 'u'`) profile mutations
   (tier upgrades, EV transitions, Uber One memberships) to test SCD Type 2 `MERGE INTO`.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import random
import uuid
from typing import Any, Dict, List, Sequence

from uber_platform.geospatial import KNOWN_NYC_ZONE_COORDS, get_zone_centroid


def generate_cdc_batches(
    driver_cdc_dir: str,
    rider_cdc_dir: str,
    num_drivers: int = 600,
    num_riders: int = 2_500,
    seed: int = 101,
) -> Dict[str, int]:
    """
    Write initial `op='c'` bootstrap snapshot and subsequent `op='u'` mutation batches
    for both Drivers and Riders so SCD Type 2 history tables capture true multi-version timelines.
    """
    os.makedirs(driver_cdc_dir, exist_ok=True)
    os.makedirs(rider_cdc_dir, exist_ok=True)
    rng = random.Random(seed)

    boroughs = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
    borough_weights = [0.42, 0.25, 0.22, 0.08, 0.03]
    vehicles = ["Sedan", "SUV", "EV_Sedan", "EV_SUV", "WAV"]
    veh_weights = [0.46, 0.24, 0.16, 0.10, 0.04]
    tiers = ["Blue", "Gold", "Platinum", "Diamond"]

    # Batch 1: Initial driver onboarding prior to Jan 1, 2024
    drv_batch_1: List[Dict[str, Any]] = []
    driver_state: Dict[str, Dict[str, Any]] = {}
    t0 = dt.datetime(2023, 12, 15, 8, 0, 0)

    for idx in range(1, num_drivers + 1):
        drv_id = f"DRV-{idx:05d}"
        boro = rng.choices(boroughs, weights=borough_weights, k=1)[0]
        veh = rng.choices(vehicles, weights=veh_weights, k=1)[0]
        tier = rng.choices(tiers, weights=[0.45, 0.30, 0.17, 0.08], k=1)[0]
        rating = round(min(5.0, max(4.2, rng.gauss(4.86, 0.11))), 2)
        is_ev = veh.startswith("EV")
        rec = {
            "cdc_event_id": str(uuid.UUID(int=rng.getrandbits(128))),
            "op": "c",
            "event_ts": (t0 + dt.timedelta(minutes=idx % 720)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "driver_id": drv_id,
            "home_borough": boro,
            "vehicle_type": veh,
            "driver_rating": rating,
            "driver_tier": tier,
            "is_ev_vehicle": is_ev,
        }
        drv_batch_1.append(rec)
        driver_state[drv_id] = dict(rec)

    _write_jsonl(os.path.join(driver_cdc_dir, "driver_cdc_batch_01_init.json"), drv_batch_1)

    # Batch 2: Mid-January SCD Type 2 updates (~20% of drivers upgrade tier or switch to EV!)
    drv_batch_2: List[Dict[str, Any]] = []
    tier_next = {"Blue": "Gold", "Gold": "Platinum", "Platinum": "Diamond", "Diamond": "Diamond"}
    mutated_drivers = rng.sample(list(driver_state.keys()), k=max(1, int(num_drivers * 0.20)))
    for drv_id in mutated_drivers:
        prev = driver_state[drv_id]
        change_ts = dt.datetime(2024, 1, rng.randint(12, 20), rng.randint(6, 22), rng.randint(0, 59))
        new_veh = "EV_SUV" if (not prev["is_ev_vehicle"] and rng.random() < 0.45) else prev["vehicle_type"]
        new_tier = tier_next[prev["driver_tier"]]
        new_rating = round(min(5.0, prev["driver_rating"] + rng.uniform(0.02, 0.08)), 2)
        drv_batch_2.append(
            {
                "cdc_event_id": str(uuid.UUID(int=rng.getrandbits(128))),
                "op": "u",
                "event_ts": change_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "driver_id": drv_id,
                "home_borough": prev["home_borough"],
                "vehicle_type": new_veh,
                "driver_rating": new_rating,
                "driver_tier": new_tier,
                "is_ev_vehicle": new_veh.startswith("EV"),
            }
        )

    _write_jsonl(os.path.join(driver_cdc_dir, "driver_cdc_batch_02_updates.json"), drv_batch_2)

    # Riders Batch 1 + Mid-Month Uber One Subscription Upgrades
    rdr_batch_1: List[Dict[str, Any]] = []
    rider_state: Dict[str, Dict[str, Any]] = {}
    memberships = ["None", "Uber_One_Monthly", "Uber_One_Annual"]
    payments = ["Card", "ApplePay", "UberCash", "Corporate"]

    for idx in range(1, num_riders + 1):
        rdr_id = f"RDR-{idx:06d}"
        mem = rng.choices(memberships, weights=[0.65, 0.23, 0.12], k=1)[0]
        pay = rng.choices(payments, weights=[0.45, 0.35, 0.12, 0.08], k=1)[0]
        rating = round(min(5.0, max(4.1, rng.gauss(4.84, 0.14))), 2)
        rec = {
            "cdc_event_id": str(uuid.UUID(int=rng.getrandbits(128))),
            "op": "c",
            "event_ts": (t0 + dt.timedelta(minutes=idx % 1440)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "rider_id": rdr_id,
            "membership_status": mem,
            "rider_rating": rating,
            "preferred_payment": pay,
        }
        rdr_batch_1.append(rec)
        rider_state[rdr_id] = dict(rec)

    _write_jsonl(os.path.join(rider_cdc_dir, "rider_cdc_batch_01_init.json"), rdr_batch_1)

    rdr_batch_2: List[Dict[str, Any]] = []
    non_members = [k for k, v in rider_state.items() if v["membership_status"] == "None"]
    upgraded_riders = rng.sample(non_members, k=min(len(non_members), max(1, int(num_riders * 0.15))))
    for rdr_id in upgraded_riders:
        prev = rider_state[rdr_id]
        change_ts = dt.datetime(2024, 1, rng.randint(10, 22), rng.randint(8, 21), rng.randint(0, 59))
        rdr_batch_2.append(
            {
                "cdc_event_id": str(uuid.UUID(int=rng.getrandbits(128))),
                "op": "u",
                "event_ts": change_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "rider_id": rdr_id,
                "membership_status": rng.choice(["Uber_One_Monthly", "Uber_One_Annual"]),
                "rider_rating": prev["rider_rating"],
                "preferred_payment": prev["preferred_payment"],
            }
        )

    _write_jsonl(os.path.join(rider_cdc_dir, "rider_cdc_batch_02_updates.json"), rdr_batch_2)

    return {
        "driver_cdc_events": len(drv_batch_1) + len(drv_batch_2),
        "rider_cdc_events": len(rdr_batch_1) + len(rdr_batch_2),
    }


def generate_streaming_telemetry_batches(
    gps_stream_dir: str,
    trip_stream_dir: str,
    seed_trips: Sequence[Dict[str, Any]] | None = None,
    num_batches: int = 5,
    trips_per_batch: int = 160,
    num_drivers: int = 600,
    num_riders: int = 2_500,
    seed: int = 2024,
) -> Dict[str, int]:
    """
    Emit multiple micro-batch JSON files simulating real-time Kafka/EventHub partitions
    landing in cloud object storage for Databricks Auto Loader / Structured Streaming.

    Includes:
    - Active trip trajectories & lifecycle state transitions
    - Idle available driver pings across NYC hexagons (for real-time supply counting)
    - ~12% ride cancellations (`CANCELLED_BY_RIDER` / `CANCELLED_BY_DRIVER`)
    - ~2% duplicate retry `event_id`s (tests watermark deduplication)
    - ~3% delayed late-arriving timestamps (tests 10-minute event-time watermarking)
    - ~0.5% schema-drift / malformed telemetry records (tests Auto Loader `_rescued_data`)
    """
    os.makedirs(gps_stream_dir, exist_ok=True)
    os.makedirs(trip_stream_dir, exist_ok=True)
    rng = random.Random(seed)
    popular_zones = list(KNOWN_NYC_ZONE_COORDS.keys())

    total_gps = 0
    total_lifecycle = 0
    total_duplicates_injected = 0

    base_window_start = dt.datetime(2024, 1, 15, 17, 0, 0)  # Evening Rush Hour window

    for b_idx in range(num_batches):
        gps_events: List[Dict[str, Any]] = []
        lifecycle_events: List[Dict[str, Any]] = []
        batch_start = base_window_start + dt.timedelta(minutes=b_idx * 5)

        # 1. Idle / roaming available drivers emitting GPS pings in this 5-min window
        roaming_count = min(num_drivers, 220)
        for d_i in range(roaming_count):
            drv_id = f"DRV-{(d_i + 1 + b_idx * 17) % num_drivers + 1:05d}"
            z_id = rng.choice(popular_zones)
            z_lat, z_lon = get_zone_centroid(z_id)
            for ping_step in range(3):
                ev_ts = batch_start + dt.timedelta(seconds=ping_step * 90 + rng.randint(0, 25))
                ping = {
                    "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
                    "event_ts": ev_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "ingest_emitted_ts": (ev_ts + dt.timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "driver_id": drv_id,
                    "trip_id": None,
                    "status": "AVAILABLE",
                    "lat": round(z_lat + rng.uniform(-0.0045, 0.0045), 6),
                    "lon": round(z_lon + rng.uniform(-0.0045, 0.0045), 6),
                    "speed_mph": round(rng.uniform(0.0, 14.5), 1),
                    "heading_deg": rng.randint(0, 359),
                    "battery_pct": rng.randint(35, 99),
                    "zone_id": z_id,
                }
                gps_events.append(ping)

        # 2. Active & cancelled trips in this micro-batch
        for t_i in range(trips_per_batch):
            seed_row = (
                seed_trips[(b_idx * trips_per_batch + t_i) % len(seed_trips)]
                if seed_trips
                else None
            )
            pu_id = int(seed_row["PULocationID"]) if seed_row and seed_row.get("PULocationID") else rng.choice(popular_zones)
            do_id = int(seed_row["DOLocationID"]) if seed_row and seed_row.get("DOLocationID") else rng.choice(popular_zones)
            pu_lat, pu_lon = get_zone_centroid(pu_id)
            do_lat, do_lon = get_zone_centroid(do_id)
            pu_lat = round(pu_lat + rng.uniform(-0.003, 0.003), 6)
            pu_lon = round(pu_lon + rng.uniform(-0.003, 0.003), 6)
            do_lat = round(do_lat + rng.uniform(-0.003, 0.003), 6)
            do_lon = round(do_lon + rng.uniform(-0.003, 0.003), 6)

            trip_id = f"TRIP-RT-{b_idx:02d}-{t_i:05d}"
            drv_id = f"DRV-{rng.randint(1, num_drivers):05d}"
            rdr_id = f"RDR-{rng.randint(1, num_riders):06d}"
            product = rng.choices(
                ["UberX", "Uber Comfort", "UberX Share", "Uber Black/XL", "UberWAV"],
                weights=[0.64, 0.16, 0.10, 0.07, 0.03],
                k=1,
            )[0]
            surge = round(rng.choice([1.0, 1.0, 1.15, 1.25, 1.4, 1.65, 2.1]), 2)
            est_fare = round(
                float(seed_row["base_passenger_fare"])
                if seed_row and seed_row.get("base_passenger_fare") and float(seed_row["base_passenger_fare"]) > 0
                else rng.uniform(12.0, 58.0) * surge,
                2,
            )

            req_ts = batch_start + dt.timedelta(seconds=rng.randint(0, 260))
            # 3% late / out-of-order telemetry arrival test
            if rng.random() < 0.03:
                req_ts = req_ts - dt.timedelta(minutes=rng.randint(2, 7))

            is_cancelled = rng.random() < 0.12

            def _make_lc_event(ev_type: str, ts: dt.datetime, cancel_reason: str | None = None) -> Dict[str, Any]:
                return {
                    "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
                    "event_ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "trip_id": trip_id,
                    "event_type": ev_type,
                    "rider_id": rdr_id,
                    "driver_id": drv_id if ev_type != "REQUESTED" else None,
                    "product_type": product,
                    "pu_location_id": pu_id,
                    "do_location_id": do_id,
                    "pickup_lat": pu_lat,
                    "pickup_lon": pu_lon,
                    "dropoff_lat": do_lat,
                    "dropoff_lon": do_lon,
                    "quoted_surge_multiplier": surge,
                    "estimated_fare_usd": est_fare,
                    "cancel_reason": cancel_reason,
                }

            lifecycle_events.append(_make_lc_event("REQUESTED", req_ts))
            disp_ts = req_ts + dt.timedelta(seconds=rng.randint(4, 22))
            lifecycle_events.append(_make_lc_event("DISPATCHED", disp_ts))

            if is_cancelled:
                cancel_ts = disp_ts + dt.timedelta(seconds=rng.randint(30, 210))
                c_type = "CANCELLED_BY_RIDER" if rng.random() < 0.72 else "CANCELLED_BY_DRIVER"
                c_reason = (
                    rng.choice(["DRIVER_TOO_FAR", "CHANGED_PLANS", "SURGE_TOO_HIGH"])
                    if c_type == "CANCELLED_BY_RIDER"
                    else rng.choice(["TRAFFIC_DELAY", "UNSAFE_PICKUP"])
                )
                lifecycle_events.append(_make_lc_event(c_type, cancel_ts, cancel_reason=c_reason))
            else:
                scene_ts = disp_ts + dt.timedelta(seconds=rng.randint(90, 300))
                start_ts = scene_ts + dt.timedelta(seconds=rng.randint(20, 95))
                dur_sec = rng.randint(300, 1500)
                end_ts = start_ts + dt.timedelta(seconds=dur_sec)

                lifecycle_events.append(_make_lc_event("DRIVER_ON_SCENE", scene_ts))
                lifecycle_events.append(_make_lc_event("IN_PROGRESS", start_ts))
                lifecycle_events.append(_make_lc_event("COMPLETED", end_ts))

                # Interpolate 4 GPS trajectory pings along the active trip
                for step in range(4):
                    frac = (step + 1) / 4.0
                    interp_lat = round(pu_lat + (do_lat - pu_lat) * frac, 6)
                    interp_lon = round(pu_lon + (do_lon - pu_lon) * frac, 6)
                    p_ts = start_ts + dt.timedelta(seconds=int(dur_sec * frac * 0.8))
                    ping = {
                        "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
                        "event_ts": p_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "ingest_emitted_ts": (p_ts + dt.timedelta(seconds=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "driver_id": drv_id,
                        "trip_id": trip_id,
                        "status": "ON_TRIP",
                        "lat": interp_lat,
                        "lon": interp_lon,
                        "speed_mph": round(rng.uniform(14.0, 38.0), 1),
                        "heading_deg": rng.randint(0, 359),
                        "battery_pct": rng.randint(25, 98),
                        "zone_id": pu_id if frac < 0.5 else do_id,
                    }
                    gps_events.append(ping)

                    # Inject ~2% duplicate network retry events (identical event_id)
                    if rng.random() < 0.02:
                        gps_events.append(dict(ping))
                        total_duplicates_injected += 1

        # Inject 1 schema-drift record per batch to demonstrate Auto Loader `_rescued_data`
        gps_events.append(
            {
                "event_id": f"DRIFT-{b_idx:03d}",
                "event_ts": batch_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "driver_id": "DRV-00001",
                "status": "AVAILABLE",
                "lat": 40.7580,
                "lon": -73.9855,
                "speed_mph": 12.5,
                "unexpected_firmware_tag": "ios-driver-v9.42.1-beta",
            }
        )

        gps_file = os.path.join(gps_stream_dir, f"gps_pings_batch_{b_idx + 1:03d}.json")
        lc_file = os.path.join(trip_stream_dir, f"trip_events_batch_{b_idx + 1:03d}.json")
        _write_jsonl(gps_file, gps_events)
        _write_jsonl(lc_file, lifecycle_events)

        total_gps += len(gps_events)
        total_lifecycle += len(lifecycle_events)

    return {
        "batches_written": num_batches,
        "gps_ping_events": total_gps,
        "trip_lifecycle_events": total_lifecycle,
        "duplicate_retries_injected": total_duplicates_injected,
    }


def _write_jsonl(filepath: str, records: Sequence[Dict[str, Any]]) -> None:
    with open(filepath, "w", encoding="utf-8") as fh:
        for row in records:
            fh.write(json.dumps(row) + "\n")
