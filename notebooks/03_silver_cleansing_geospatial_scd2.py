# Databricks notebook source
# MAGIC %md
# MAGIC # 03. Silver Layer — Cleansing, Quarantine Gate, Uber H3 Spatial Indexing & SCD Type 2
# MAGIC
# MAGIC Transforms raw Bronze datasets into enterprise-grade **Silver Delta Tables**:
# MAGIC 1. `silver.weather_hourly`: Cleaned hourly NYC weather with `weather_condition` and `is_adverse_weather` flags.
# MAGIC 2. `silver.trips_enriched` & `silver.trips_quarantine`:
# MAGIC    - Filters to **Uber (`HV0003`)**, generates deterministic `trip_id` SHA-256 keys, evaluates **7 Data Quality rules**, and routes corrupt records to `silver.trips_quarantine` with `dq_failed_rules`.
# MAGIC    - Enriches valid Uber trips with **Uber H3 Resolution-7 & Resolution-8** spatial indices, Haversine distance, SLA wait times, product classification (`UberX`, `Uber Comfort`, `UberX Share`, `UberWAV`, `Uber Black/XL`), dynamic surge multiplier, and two-sided marketplace take-rate!
# MAGIC 3. `silver.dim_driver_scd2` & `silver.dim_rider_scd2`:
# MAGIC    - Applies **Slowly Changing Dimension Type 2 (`MERGE INTO`)** over CDC streams (`op='c'` and `op='u'`), preserving `effective_start_ts`, `effective_end_ts`, and `is_current` history!

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
from uber_platform.transformations import (
    apply_scd2_from_cdc,
    build_dim_zone,
    build_silver_weather,
    clean_and_split_fhvhv_trips,
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
# MAGIC ### Step 1: Build Silver Weather & Enriched Zone Lookup (with H3 Res-7 & Res-8)

# COMMAND ----------

bronze_weather_df = spark.table(cfg.table_fqn("bronze", "hourly_weather_raw"))  # type: ignore[name-defined]
silver_weather_tbl = cfg.table_fqn("silver", "weather_hourly")

silver_weather_df = build_silver_weather(bronze_weather_df)
silver_weather_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    silver_weather_tbl
)

bronze_zones_df = spark.table(cfg.table_fqn("bronze", "taxi_zones_raw"))  # type: ignore[name-defined]
dim_zone_df = build_dim_zone(bronze_zones_df)

print(f"Silver Weather rows : {spark.table(silver_weather_tbl).count():,}")  # type: ignore[name-defined]
spark.table(silver_weather_tbl).groupBy("weather_condition", "is_adverse_weather").count().show(truncate=False)  # type: ignore[name-defined]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Process HVFHV Uber Trips $\rightarrow$ `silver.trips_enriched` & `silver.trips_quarantine`

# COMMAND ----------

bronze_trips_df = spark.table(cfg.table_fqn("bronze", "fhvhv_trips_raw"))  # type: ignore[name-defined]
silver_trips_tbl = cfg.table_fqn("silver", "trips_enriched")
quarantine_tbl = cfg.table_fqn("silver", "trips_quarantine")

clean_trips_df, quarantine_trips_df = clean_and_split_fhvhv_trips(
    bronze_trips_df=bronze_trips_df,
    dim_zone_df=dim_zone_df,
    num_drivers=600,
    num_riders=2_500,
    uber_only=True,
)

(
    clean_trips_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_trips_tbl)
)

(
    quarantine_trips_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(quarantine_tbl)
)

# Enable Delta Change Data Feed on Silver Trips for downstream incremental pipelines
try:
    spark.sql(f"ALTER TABLE {silver_trips_tbl} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")  # type: ignore[name-defined]
except Exception:
    pass

clean_cnt = spark.table(silver_trips_tbl).count()  # type: ignore[name-defined]
quar_cnt = spark.table(quarantine_tbl).count()  # type: ignore[name-defined]
print(f"Clean Uber Trips (`silver.trips_enriched`)     : {clean_cnt:,}")
print(f"Quarantined Trips (`silver.trips_quarantine`)  : {quar_cnt:,}")

# Inspect Data Quality Quarantine Breakdown
spark.table(quarantine_tbl).select(  # type: ignore[name-defined]
    F.explode("dq_failed_rules").alias("dq_rule")
).groupBy("dq_rule").count().orderBy(F.col("count").desc()).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Build SCD Type 2 Dimensions (`silver.dim_driver_scd2` & `silver.dim_rider_scd2`)
# MAGIC Processes Batch 1 (`op='c'` initial onboarding) followed by incremental `MERGE INTO` of Batch 2 (`op='u'` mid-January EV / Driver Tier / Uber One upgrades) to verify multi-version SCD Type 2 history!

# COMMAND ----------

drv_cdc_all = spark.table(cfg.table_fqn("bronze", "driver_cdc_raw"))  # type: ignore[name-defined]
rdr_cdc_all = spark.table(cfg.table_fqn("bronze", "rider_cdc_raw"))  # type: ignore[name-defined]

silver_drv_tbl = cfg.table_fqn("silver", "dim_driver_scd2")
silver_rdr_tbl = cfg.table_fqn("silver", "dim_rider_scd2")

# Drop prior state so we cleanly demonstrate initial load + incremental SCD2 MERGE
spark.sql(f"DROP TABLE IF EXISTS {silver_drv_tbl}")  # type: ignore[name-defined]
spark.sql(f"DROP TABLE IF EXISTS {silver_rdr_tbl}")  # type: ignore[name-defined]

# Pass 1: Initial Create (`op = 'c'`)
apply_scd2_from_cdc(
    spark=spark,  # type: ignore[name-defined]
    cdc_df=drv_cdc_all.filter("op = 'c'"),
    target_table_fqn=silver_drv_tbl,
    natural_key="driver_id",
    surrogate_key="driver_sk",
    tracked_columns=["home_borough", "vehicle_type", "driver_rating", "driver_tier", "is_ev_vehicle"],
)

# Pass 2: Incremental Updates (`op = 'u'`) via atomic Delta MERGE INTO
apply_scd2_from_cdc(
    spark=spark,  # type: ignore[name-defined]
    cdc_df=drv_cdc_all.filter("op = 'u'"),
    target_table_fqn=silver_drv_tbl,
    natural_key="driver_id",
    surrogate_key="driver_sk",
    tracked_columns=["home_borough", "vehicle_type", "driver_rating", "driver_tier", "is_ev_vehicle"],
)

# Same 2-pass SCD2 lifecycle for Rider Dimension (Uber One subscription upgrades)
apply_scd2_from_cdc(
    spark=spark,  # type: ignore[name-defined]
    cdc_df=rdr_cdc_all.filter("op = 'c'"),
    target_table_fqn=silver_rdr_tbl,
    natural_key="rider_id",
    surrogate_key="rider_sk",
    tracked_columns=["membership_status", "rider_rating", "preferred_payment"],
)
apply_scd2_from_cdc(
    spark=spark,  # type: ignore[name-defined]
    cdc_df=rdr_cdc_all.filter("op = 'u'"),
    target_table_fqn=silver_rdr_tbl,
    natural_key="rider_id",
    surrogate_key="rider_sk",
    tracked_columns=["membership_status", "rider_rating", "preferred_payment"],
)

print("Driver SCD2 Summary (is_current=true vs closed historical versions):")
spark.table(silver_drv_tbl).groupBy("is_current").count().show()  # type: ignore[name-defined]

print("Sample Driver with Multi-Version SCD Type 2 History:")
sample_upgraded_drv = (
    spark.table(silver_drv_tbl)  # type: ignore[name-defined]
    .filter("is_current = false")
    .select("driver_id")
    .limit(1)
)
spark.table(silver_drv_tbl).join(sample_upgraded_drv, on="driver_id").orderBy("effective_start_ts").show(  # type: ignore[name-defined]
    truncate=False
)
