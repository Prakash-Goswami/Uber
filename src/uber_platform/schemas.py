"""
Explicit PySpark StructType definitions for all batch and streaming sources.

Defining explicit schemas prevents expensive full-file schema scans on large cloud storage
paths and enforces contract stability across Bronze and Silver pipelines.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_taxi_zone_schema() -> StructType:
    return StructType(
        [
            StructField("LocationID", IntegerType(), False),
            StructField("Borough", StringType(), True),
            StructField("Zone", StringType(), True),
            StructField("service_zone", StringType(), True),
            StructField("centroid_lat", DoubleType(), True),
            StructField("centroid_lon", DoubleType(), True),
        ]
    )


def get_weather_hourly_schema() -> StructType:
    return StructType(
        [
            StructField("observation_hour_utc", StringType(), False),
            StructField("temp_celsius", DoubleType(), True),
            StructField("precipitation_mm", DoubleType(), True),
            StructField("rain_mm", DoubleType(), True),
            StructField("snowfall_cm", DoubleType(), True),
            StructField("wind_speed_kmh", DoubleType(), True),
            StructField("weather_code", IntegerType(), True),
        ]
    )


def get_driver_gps_ping_schema() -> StructType:
    return StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("event_ts", TimestampType(), False),
            StructField("ingest_emitted_ts", TimestampType(), True),
            StructField("driver_id", StringType(), False),
            StructField("trip_id", StringType(), True),
            StructField("status", StringType(), True),  # AVAILABLE | EN_ROUTE | ON_TRIP
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("speed_mph", DoubleType(), True),
            StructField("heading_deg", IntegerType(), True),
            StructField("battery_pct", IntegerType(), True),
            StructField("zone_id", IntegerType(), True),
        ]
    )


def get_trip_lifecycle_event_schema() -> StructType:
    return StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("event_ts", TimestampType(), False),
            StructField("trip_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("rider_id", StringType(), True),
            StructField("driver_id", StringType(), True),
            StructField("product_type", StringType(), True),
            StructField("pu_location_id", IntegerType(), True),
            StructField("do_location_id", IntegerType(), True),
            StructField("pickup_lat", DoubleType(), True),
            StructField("pickup_lon", DoubleType(), True),
            StructField("dropoff_lat", DoubleType(), True),
            StructField("dropoff_lon", DoubleType(), True),
            StructField("quoted_surge_multiplier", DoubleType(), True),
            StructField("estimated_fare_usd", DoubleType(), True),
            StructField("cancel_reason", StringType(), True),
        ]
    )


def get_driver_cdc_schema() -> StructType:
    return StructType(
        [
            StructField("cdc_event_id", StringType(), False),
            StructField("op", StringType(), False),  # c (create) | u (update) | d (delete)
            StructField("event_ts", TimestampType(), False),
            StructField("driver_id", StringType(), False),
            StructField("home_borough", StringType(), True),
            StructField("vehicle_type", StringType(), True),
            StructField("driver_rating", DoubleType(), True),
            StructField("driver_tier", StringType(), True),
            StructField("is_ev_vehicle", BooleanType(), True),
        ]
    )


def get_rider_cdc_schema() -> StructType:
    return StructType(
        [
            StructField("cdc_event_id", StringType(), False),
            StructField("op", StringType(), False),
            StructField("event_ts", TimestampType(), False),
            StructField("rider_id", StringType(), False),
            StructField("membership_status", StringType(), True),
            StructField("rider_rating", DoubleType(), True),
            StructField("preferred_payment", StringType(), True),
        ]
    )


def get_fhvhv_expected_columns() -> list[tuple[str, str]]:
    """Canonical NYC TLC HVFHV column names and Spark SQL types."""
    return [
        ("hvfhs_license_num", "STRING"),
        ("dispatching_base_num", "STRING"),
        ("originating_base_num", "STRING"),
        ("request_datetime", "TIMESTAMP"),
        ("on_scene_datetime", "TIMESTAMP"),
        ("pickup_datetime", "TIMESTAMP"),
        ("dropoff_datetime", "TIMESTAMP"),
        ("PULocationID", "LONG"),
        ("DOLocationID", "LONG"),
        ("trip_miles", "DOUBLE"),
        ("trip_time", "LONG"),
        ("base_passenger_fare", "DOUBLE"),
        ("tolls", "DOUBLE"),
        ("bcf", "DOUBLE"),
        ("sales_tax", "DOUBLE"),
        ("congestion_surcharge", "DOUBLE"),
        ("airport_fee", "DOUBLE"),
        ("tips", "DOUBLE"),
        ("driver_pay", "DOUBLE"),
        ("shared_request_flag", "STRING"),
        ("shared_match_flag", "STRING"),
        ("access_a_ride_flag", "STRING"),
        ("wav_request_flag", "STRING"),
        ("wav_match_flag", "STRING"),
    ]
