# Databricks notebook source
# MAGIC %md
# MAGIC # Declarative Lakeflow / Delta Live Tables (DLT) Pipeline — Uber Medallion Architecture
# MAGIC
# MAGIC Attach this script to a **Databricks Lakeflow / Delta Live Tables Pipeline** (`Jobs & Pipelines -> Create Pipeline`) to get:
# MAGIC - Visual DAG lineage across Bronze $\rightarrow$ Silver $\rightarrow$ Gold Streaming Tables & Materialized Views
# MAGIC - Declarative **Data Quality Expectations** (`@dlt.expect_all_or_drop`, `@dlt.expect`) with built-in pass/fail metrics in the DLT UI
# MAGIC - Automatic incremental Auto Loader (`cloudFiles`) checkpointing and **SCD Type 2** (`dlt.apply_changes`)!
# MAGIC
# MAGIC **Pipeline Configuration Parameter**:
# MAGIC - `uber.landing_path`: `/Volumes/uber_lakehouse/raw_landing/landing_zone` (default)

# COMMAND ----------

try:
    import dlt  # type: ignore[import-not-found]
except ImportError:
    # Lightweight stub so `py_compile` / local linters can inspect this file outside DLT runtime
    class _DLTStub:
        def table(self, *args, **kwargs):
            return lambda fn: fn

        def view(self, *args, **kwargs):
            return lambda fn: fn

        def expect(self, *args, **kwargs):
            return lambda fn: fn

        def expect_or_drop(self, *args, **kwargs):
            return lambda fn: fn

        def expect_all_or_drop(self, *args, **kwargs):
            return lambda fn: fn

        def create_streaming_table(self, *args, **kwargs):
            return None

        def apply_changes(self, *args, **kwargs):
            return None

        def read(self, name):
            return spark.table(name)  # type: ignore[name-defined]

        def read_stream(self, name):
            return spark.readStream.table(name)  # type: ignore[name-defined]

    dlt = _DLTStub()

from pyspark.sql import functions as F

LANDING_PATH = (
    spark.conf.get("uber.landing_path", "/Volumes/uber_lakehouse/raw_landing/landing_zone")  # type: ignore[name-defined]
    if "spark" in globals()
    else "/Volumes/uber_lakehouse/raw_landing/landing_zone"
)

TRIP_DQ_EXPECTATIONS = {
    "valid_uber_license": "hvfhs_license_num = 'HV0003'",
    "chronological_timestamps": "dropoff_datetime > pickup_datetime",
    "realistic_trip_miles": "trip_miles > 0.05 AND trip_miles <= 250.0",
    "realistic_trip_duration": "trip_time >= 30 AND trip_time <= 21600",
    "positive_passenger_fare": "base_passenger_fare > 0.0 AND driver_pay >= 0.0",
    "valid_nyc_zones": "PULocationID BETWEEN 1 AND 265 AND DOLocationID BETWEEN 1 AND 265",
}


# ==============================================================================
# 1. BRONZE LAYER — Auto Loader Streaming Ingestion
# ==============================================================================
@dlt.table(
    name="dlt_bronze_fhvhv_trips",
    comment="Raw NYC TLC HVFHV Parquet records ingested incrementally via Auto Loader.",
)
def dlt_bronze_fhvhv_trips():
    return (
        spark.readStream.format("cloudFiles")  # type: ignore[name-defined]
        .option("cloudFiles.format", "parquet")
        .load(f"{LANDING_PATH}/batch/fhvhv")
        .withColumn("_ingest_ts", F.current_timestamp())
    )


@dlt.table(
    name="dlt_bronze_driver_pings",
    comment="Real-time driver GPS telemetry stream ingested via Auto Loader with _rescued_data.",
)
def dlt_bronze_driver_pings():
    return (
        spark.readStream.format("cloudFiles")  # type: ignore[name-defined]
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{LANDING_PATH}/streams/driver_gps_pings")
        .withColumn("_ingest_ts", F.current_timestamp())
    )


@dlt.table(
    name="dlt_bronze_driver_cdc",
    comment="Raw Debezium-style driver profile CDC events (c/u/d).",
)
def dlt_bronze_driver_cdc():
    return (
        spark.readStream.format("cloudFiles")  # type: ignore[name-defined]
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{LANDING_PATH}/streams/cdc_drivers")
        .withColumn("event_ts_parsed", F.to_timestamp("event_ts"))
    )


# ==============================================================================
# 2. SILVER LAYER — DLT Expectations, SCD Type 2 (`apply_changes`) & Watermarks
# ==============================================================================
@dlt.table(
    name="dlt_silver_uber_trips",
    comment="Validated Uber (HV0003) trips passing all 6 DLT Data Quality Expectations.",
)
@dlt.expect_all_or_drop(TRIP_DQ_EXPECTATIONS)
def dlt_silver_uber_trips():
    return (
        dlt.read_stream("dlt_bronze_fhvhv_trips")
        .withColumn(
            "trip_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("hvfhs_license_num"),
                    F.col("pickup_datetime").cast("string"),
                    F.col("PULocationID").cast("string"),
                    F.col("DOLocationID").cast("string"),
                    F.col("base_passenger_fare").cast("string"),
                ),
                256,
            ),
        )
        .withColumn(
            "total_rider_amount",
            F.round(
                F.col("base_passenger_fare")
                + F.coalesce(F.col("tolls"), F.lit(0.0))
                + F.coalesce(F.col("bcf"), F.lit(0.0))
                + F.coalesce(F.col("sales_tax"), F.lit(0.0))
                + F.coalesce(F.col("congestion_surcharge"), F.lit(0.0))
                + F.coalesce(F.col("airport_fee"), F.lit(0.0))
                + F.coalesce(F.col("tips"), F.lit(0.0)),
                2,
            ),
        )
        .withColumn("uber_gross_margin", F.round(F.col("base_passenger_fare") - F.col("driver_pay"), 2))
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
    )


# Declarative SCD Type 2 via DLT `apply_changes`!
dlt.create_streaming_table(
    name="dlt_silver_dim_driver_scd2",
    comment="Declarative SCD Type 2 Driver history managed automatically by Lakeflow DLT.",
)

dlt.apply_changes(
    target="dlt_silver_dim_driver_scd2",
    source="dlt_bronze_driver_cdc",
    keys=["driver_id"],
    sequence_by=F.col("event_ts_parsed"),
    apply_as_deletes=F.expr("op = 'd'"),
    except_column_list=["op", "cdc_event_id", "event_ts", "event_ts_parsed"],
    stored_as_scd_type="2",
)


# ==============================================================================
# 3. GOLD LAYER — Materialized Marketplace KPI Aggregations
# ==============================================================================
@dlt.table(
    name="dlt_gold_daily_zone_economics",
    comment="Gold Materialized View of daily Uber trip volume, revenue, and take-rate per pickup zone.",
)
def dlt_gold_daily_zone_economics():
    return (
        dlt.read("dlt_silver_uber_trips")
        .groupBy("pickup_date", "PULocationID")
        .agg(
            F.count("*").alias("completed_trips"),
            F.round(F.sum("total_rider_amount"), 2).alias("gross_bookings_usd"),
            F.round(F.sum("driver_pay"), 2).alias("total_driver_pay_usd"),
            F.round(F.sum("uber_gross_margin"), 2).alias("total_uber_margin_usd"),
            F.round(F.avg("trip_miles"), 2).alias("avg_trip_miles"),
        )
    )
