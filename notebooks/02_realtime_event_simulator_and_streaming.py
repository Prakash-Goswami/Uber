# Databricks notebook source
# MAGIC %md
# MAGIC # 02. Real-Time Event Simulator, Auto Loader (`cloudFiles`) & Stateful Stream Processing
# MAGIC
# MAGIC Simulates Uber's **Kafka + Flink / Structured Streaming** real-time pipeline:
# MAGIC 1. **Real-Time Telemetry & CDC Producer**: Replays actual NYC Uber pickup/dropoff corridors from `bronze.fhvhv_trips_raw` to emit live JSON micro-batches:
# MAGIC    - `driver_gps_pings`: 5-second active & idle driver GPS pings (+ 2% duplicate network retries, 3% delayed out-of-order pings, and schema-drift fields).
# MAGIC    - `trip_lifecycle_events`: `REQUESTED -> DISPATCHED -> DRIVER_ON_SCENE -> IN_PROGRESS -> COMPLETED` (plus rider/driver cancellations).
# MAGIC    - `cdc_drivers` & `cdc_riders`: Initial snapshots (`op='c'`) and mid-month tier/EV/Uber-One upgrades (`op='u'`).
# MAGIC 2. **Databricks Auto Loader (`cloudFiles`) $\rightarrow$ Bronze Streams**: Incremental ingestion with `_rescued_data` capture.
# MAGIC 3. **Stateful Watermarking + Deduplication + 5-Minute H3 Hexagon Aggregation**:
# MAGIC    - `withWatermark("event_ts", "10 minutes")` + `dropDuplicates(["event_id", "event_ts"])`
# MAGIC    - `foreachBatch` idempotent `MERGE INTO` `silver.hex_telemetry_5m`.

# COMMAND ----------

import os
import sys

for candidate in [
    os.path.abspath(os.path.join(os.getcwd(), "..", "src")),
    os.path.abspath(os.path.join(os.getcwd(), "src")),
]:
    if os.path.isdir(candidate) and candidate not in sys.path:
        sys.path.insert(0, candidate)

from pyspark.sql import functions as F

from uber_platform.config import bootstrap_lakehouse
from uber_platform.geospatial import add_h3_columns_spark, spark_supports_native_h3
from uber_platform.realtime_simulator import (
    generate_cdc_batches,
    generate_streaming_telemetry_batches,
)
from uber_platform.schemas import (
    get_driver_cdc_schema,
    get_driver_gps_ping_schema,
    get_rider_cdc_schema,
    get_trip_lifecycle_event_schema,
)

# COMMAND ----------

try:
    dbutils.widgets.text("catalog_name", "uber_lakehouse", "1. Target Catalog")  # type: ignore[name-defined]
    CATALOG_NAME = dbutils.widgets.get("catalog_name").strip() or "uber_lakehouse"  # type: ignore[name-defined]
except Exception:
    CATALOG_NAME = "uber_lakehouse"

cfg = bootstrap_lakehouse(spark, preferred_catalog=CATALOG_NAME)  # type: ignore[name-defined]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Emit Real-Time Streaming Micro-Batches & CDC Logs Seeded from Real Uber Trips

# COMMAND ----------

bronze_trips_tbl = cfg.table_fqn("bronze", "fhvhv_trips_raw")
seed_trips = []
if spark.catalog.tableExists(bronze_trips_tbl):  # type: ignore[name-defined]
    seed_rows = (
        spark.table(bronze_trips_tbl)  # type: ignore[name-defined]
        .filter("hvfhs_license_num = 'HV0003' AND PULocationID IS NOT NULL AND DOLocationID IS NOT NULL")
        .select("PULocationID", "DOLocationID", "base_passenger_fare")
        .limit(1000)
        .collect()
    )
    seed_trips = [r.asDict() for r in seed_rows]

cdc_stats = generate_cdc_batches(
    driver_cdc_dir=cfg.driver_cdc_posix,
    rider_cdc_dir=cfg.rider_cdc_posix,
    num_drivers=600,
    num_riders=2_500,
)
stream_stats = generate_streaming_telemetry_batches(
    gps_stream_dir=cfg.gps_stream_posix,
    trip_stream_dir=cfg.trip_stream_posix,
    seed_trips=seed_trips,
    num_batches=6,
    trips_per_batch=150,
    num_drivers=600,
    num_riders=2_500,
)

print("CDC Generation Summary      :", cdc_stats)
print("Streaming Telemetry Summary :", stream_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Stream Ingestion via Databricks Auto Loader (`cloudFiles`) into Bronze Delta Tables
# MAGIC Uses `trigger(availableNow=True)` so all landed JSON micro-batches are incrementally processed with full checkpointing, without leaving an expensive idle stream running.

# COMMAND ----------

def read_json_stream_with_autoloader_fallback(source_path: str, schema, checkpoint_subdir: str):
    """Use Databricks `cloudFiles` Auto Loader when available, with OSS Structured Streaming JSON fallback."""
    try:
        stream_df = (
            spark.readStream.format("cloudFiles")  # type: ignore[name-defined]
            .option("cloudFiles.format", "json")
            .option("rescuedDataColumn", "_rescued_data")
            .schema(schema)
            .load(source_path)
        )
        # Validate plan initialization
        _ = stream_df.schema
        return stream_df
    except Exception:
        return (
            spark.readStream.format("json")  # type: ignore[name-defined]
            .option("columnNameOfCorruptRecord", "_rescued_data")
            .schema(schema)
            .load(source_path)
        )


bronze_gps_tbl = cfg.table_fqn("bronze", "driver_pings_stream")
bronze_lc_tbl = cfg.table_fqn("bronze", "trip_lifecycle_stream")
bronze_drv_cdc_tbl = cfg.table_fqn("bronze", "driver_cdc_raw")
bronze_rdr_cdc_tbl = cfg.table_fqn("bronze", "rider_cdc_raw")

# 2A. Driver GPS Pings Stream -> Bronze
gps_raw_stream = (
    read_json_stream_with_autoloader_fallback(
        cfg.gps_stream_spark, get_driver_gps_ping_schema(), "gps_bronze"
    )
    .withColumn("_ingest_ts", F.current_timestamp())
)

q_gps = (
    gps_raw_stream.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{cfg.checkpoints_spark}/bronze_driver_pings")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(bronze_gps_tbl)
)
q_gps.awaitTermination()

# 2B. Trip Lifecycle Events Stream -> Bronze
lc_raw_stream = (
    read_json_stream_with_autoloader_fallback(
        cfg.trip_stream_spark, get_trip_lifecycle_event_schema(), "lc_bronze"
    )
    .withColumn("_ingest_ts", F.current_timestamp())
)

q_lc = (
    lc_raw_stream.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{cfg.checkpoints_spark}/bronze_trip_lifecycle")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(bronze_lc_tbl)
)
q_lc.awaitTermination()

# 2C. Driver & Rider CDC Streams -> Bronze
for src_path, schema, target_tbl, ckpt_name in [
    (cfg.driver_cdc_spark, get_driver_cdc_schema(), bronze_drv_cdc_tbl, "bronze_driver_cdc"),
    (cfg.rider_cdc_spark, get_rider_cdc_schema(), bronze_rdr_cdc_tbl, "bronze_rider_cdc"),
]:
    q_cdc = (
        read_json_stream_with_autoloader_fallback(src_path, schema, ckpt_name)
        .withColumn("_ingest_ts", F.current_timestamp())
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{cfg.checkpoints_spark}/{ckpt_name}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_tbl)
    )
    q_cdc.awaitTermination()

print(f"Bronze GPS Pings ingested       : {spark.table(bronze_gps_tbl).count():,}")  # type: ignore[name-defined]
print(f"Bronze Trip Lifecycle ingested  : {spark.table(bronze_lc_tbl).count():,}")  # type: ignore[name-defined]
print(f"Bronze Driver CDC ingested      : {spark.table(bronze_drv_cdc_tbl).count():,}")  # type: ignore[name-defined]
print(f"Bronze Rider CDC ingested       : {spark.table(bronze_rdr_cdc_tbl).count():,}")  # type: ignore[name-defined]

# Inspect rescued schema-drift payloads captured automatically by Auto Loader!
if "_rescued_data" in spark.table(bronze_gps_tbl).columns:  # type: ignore[name-defined]
    rescued_cnt = spark.table(bronze_gps_tbl).filter("_rescued_data IS NOT NULL").count()  # type: ignore[name-defined]
    print(f"Schema-drift records rescued in _rescued_data : {rescued_cnt}")
    spark.table(bronze_gps_tbl).filter("_rescued_data IS NOT NULL").select(  # type: ignore[name-defined]
        "event_id", "driver_id", "_rescued_data"
    ).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Stateful Stream Processing — Watermarking, Retry Deduplication & 5-Min H3 Supply Windows
# MAGIC Deduplicates mobile network retries (`dropDuplicates(["event_id", "event_ts"])` bounded by a `10 minutes` watermark), maps GPS `(lat, lon)` to **Uber H3 Res-7**, and upserts 5-minute hexagon telemetry into `silver.hex_telemetry_5m`.

# COMMAND ----------

silver_hex_5m_tbl = cfg.table_fqn("silver", "hex_telemetry_5m")
silver_lc_tbl = cfg.table_fqn("silver", "trip_lifecycle_clean")
native_h3 = spark_supports_native_h3(spark)  # type: ignore[name-defined]


def upsert_hex_telemetry_batch(micro_batch_df, batch_epoch_id: int) -> None:
    if micro_batch_df.limit(1).count() == 0:
        return

    indexed = add_h3_columns_spark(
        micro_batch_df.filter("lat IS NOT NULL AND lon IS NOT NULL"),
        "lat",
        "lon",
        prefix="ping",
        resolutions=(7,),
        native_h3=native_h3,
    )
    agg_batch = (
        indexed.withColumn("win", F.window("event_ts", "5 minutes"))
        .groupBy(
            F.col("win.start").alias("window_start_ts"),
            F.col("win.end").alias("window_end_ts"),
            F.col("ping_h3_r7").alias("h3_r7"),
        )
        .agg(
            F.count("*").alias("total_gps_pings"),
            F.countDistinct("driver_id").alias("distinct_reporting_drivers"),
            F.countDistinct(
                F.when(F.col("status") == "AVAILABLE", F.col("driver_id"))
            ).alias("distinct_available_drivers"),
            F.round(F.avg("speed_mph"), 2).alias("avg_traffic_speed_mph"),
        )
        .withColumn("updated_at_ts", F.current_timestamp())
    )

    if not micro_batch_df.sparkSession.catalog.tableExists(silver_hex_5m_tbl):
        agg_batch.write.format("delta").mode("overwrite").saveAsTable(silver_hex_5m_tbl)
        return

    agg_batch.createOrReplaceTempView("_tmp_hex_5m_microbatch")
    micro_batch_df.sparkSession.sql(
        f"""
        MERGE INTO {silver_hex_5m_tbl} AS t
        USING _tmp_hex_5m_microbatch AS s
        ON t.window_start_ts = s.window_start_ts AND t.h3_r7 = s.h3_r7
        WHEN MATCHED THEN UPDATE SET
            t.total_gps_pings = GREATEST(t.total_gps_pings, s.total_gps_pings),
            t.distinct_reporting_drivers = GREATEST(t.distinct_reporting_drivers, s.distinct_reporting_drivers),
            t.distinct_available_drivers = GREATEST(t.distinct_available_drivers, s.distinct_available_drivers),
            t.avg_traffic_speed_mph = s.avg_traffic_speed_mph,
            t.updated_at_ts = current_timestamp()
        WHEN NOT MATCHED THEN INSERT *
        """
    )


gps_bronze_stream = (
    spark.readStream.table(bronze_gps_tbl)  # type: ignore[name-defined]
    .withWatermark("event_ts", "10 minutes")
    .dropDuplicates(["event_id", "event_ts"])
)

q_silver_hex = (
    gps_bronze_stream.writeStream.foreachBatch(upsert_hex_telemetry_batch)
    .option("checkpointLocation", f"{cfg.checkpoints_spark}/silver_hex_telemetry_5m")
    .trigger(availableNow=True)
    .start()
)
q_silver_hex.awaitTermination()

# Deduplicate & H3-index streaming Trip Lifecycle events into Silver
lc_clean_df = (
    spark.table(bronze_lc_tbl)  # type: ignore[name-defined]
    .dropDuplicates(["event_id"])
    .filter("pickup_lat IS NOT NULL AND pickup_lon IS NOT NULL")
)
lc_clean_df = add_h3_columns_spark(
    lc_clean_df, "pickup_lat", "pickup_lon", prefix="pickup", resolutions=(7, 8), native_h3=native_h3
)
lc_clean_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    silver_lc_tbl
)

print(f"Silver 5-min H3 Windows created : {spark.table(silver_hex_5m_tbl).count():,}")  # type: ignore[name-defined]
print(f"Silver Trip Lifecycle Events    : {spark.table(silver_lc_tbl).count():,}")  # type: ignore[name-defined]
spark.table(silver_hex_5m_tbl).orderBy(F.col("total_gps_pings").desc()).show(10, truncate=False)  # type: ignore[name-defined]
