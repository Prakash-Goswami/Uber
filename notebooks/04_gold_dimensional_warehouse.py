# Databricks notebook source
# MAGIC %md
# MAGIC # 04. Gold Layer — Kimball Dimensional Data Warehouse & Marketplace Marts
# MAGIC
# MAGIC Materializes the enterprise **Gold Kimball Star Schema** in `uber_lakehouse.gold`:
# MAGIC - **Conformed Dimensions**:
# MAGIC   - `gold.dim_zone` (265 NYC Zones + WGS84 Centroids + Uber H3 Res 7 & 8)
# MAGIC   - `gold.dim_date` (Calendar attributes, weekend & federal holiday flags)
# MAGIC   - `gold.dim_time` (1,440 minute-of-day grain with AM/PM Rush Hour buckets)
# MAGIC   - `gold.dim_weather` (Hourly weather conditions & adverse storm flags)
# MAGIC   - `gold.dim_driver` & `gold.dim_rider` (Views/tables exposing full SCD Type 2 history)
# MAGIC - **Fact Tables (Liquid Clustered / Z-Ordered)**:
# MAGIC   - `gold.fact_trips`: Transaction + milestone fact with **point-in-time SCD2 surrogate keys** (`driver_sk`, `rider_sk`), two-sided marketplace take-rate, and H3 indices.
# MAGIC   - `gold.fact_hex_supply_demand_5m`: 5-minute H3 Resolution-7 marketplace supply, demand, unfulfilled cancellations, and dynamic surge pricing.
# MAGIC   - `gold.fact_driver_daily_summary`: Driver daily earnings, tips, engaged hours, and hourly utilization.

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
    build_dim_date,
    build_dim_time,
    build_dim_zone,
    build_fact_driver_daily_summary,
    build_fact_hex_supply_demand_5m,
    build_fact_trips,
)

# COMMAND ----------

try:
    dbutils.widgets.text("catalog_name", "uber_lakehouse", "1. Target Catalog")  # type: ignore[name-defined]
    CATALOG_NAME = dbutils.widgets.get("catalog_name").strip() or "uber_lakehouse"  # type: ignore[name-defined]
except Exception:
    CATALOG_NAME = "uber_lakehouse"

cfg = bootstrap_lakehouse(spark, preferred_catalog=CATALOG_NAME)  # type: ignore[name-defined]


def write_gold_table(df, table_fqn: str, cluster_cols: list[str] | None = None, comment: str = "") -> None:
    """Write Gold Delta table and apply Liquid Clustering (`CLUSTER BY`) or `OPTIMIZE ZORDER BY`."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_fqn)
    )
    if comment:
        safe_comment = comment.replace("'", "\\'")
        try:
            spark.sql(f"COMMENT ON TABLE {table_fqn} IS '{safe_comment}'")  # type: ignore[name-defined]
        except Exception:
            pass

    if cluster_cols:
        cols_csv = ", ".join(cluster_cols)
        try:
            spark.sql(f"ALTER TABLE {table_fqn} CLUSTER BY ({cols_csv})")  # type: ignore[name-defined]
            spark.sql(f"OPTIMIZE {table_fqn}")  # type: ignore[name-defined]
        except Exception:
            try:
                spark.sql(f"OPTIMIZE {table_fqn} ZORDER BY ({cols_csv})")  # type: ignore[name-defined]
            except Exception:
                pass


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Materialize Conformed Gold Dimensions (`dim_zone`, `dim_date`, `dim_time`, `dim_weather`, `dim_driver`, `dim_rider`)

# COMMAND ----------

dim_zone_tbl = cfg.table_fqn("gold", "dim_zone")
dim_date_tbl = cfg.table_fqn("gold", "dim_date")
dim_time_tbl = cfg.table_fqn("gold", "dim_time")
dim_weather_tbl = cfg.table_fqn("gold", "dim_weather")
dim_driver_tbl = cfg.table_fqn("gold", "dim_driver_scd2")
dim_rider_tbl = cfg.table_fqn("gold", "dim_rider_scd2")

dim_zone_df = build_dim_zone(spark.table(cfg.table_fqn("bronze", "taxi_zones_raw")))  # type: ignore[name-defined]
dim_date_df = build_dim_date(spark, start_date="2023-12-01", end_date="2024-03-31")  # type: ignore[name-defined]
dim_time_df = build_dim_time(spark)  # type: ignore[name-defined]
dim_weather_df = spark.table(cfg.table_fqn("silver", "weather_hourly")).select(  # type: ignore[name-defined]
    "weather_hour_key",
    "observation_hour_ts",
    "temp_celsius",
    "precipitation_mm",
    "rain_mm",
    "snowfall_cm",
    "wind_speed_kmh",
    "weather_code",
    "weather_condition",
    "is_adverse_weather",
)
dim_driver_df = spark.table(cfg.table_fqn("silver", "dim_driver_scd2"))  # type: ignore[name-defined]
dim_rider_df = spark.table(cfg.table_fqn("silver", "dim_rider_scd2"))  # type: ignore[name-defined]

write_gold_table(
    dim_zone_df,
    dim_zone_tbl,
    cluster_cols=["location_id"],
    comment="NYC 265 Taxi Zones enriched with WGS84 centroids and Uber H3 Res-7 & Res-8 indices.",
)
write_gold_table(
    dim_date_df,
    dim_date_tbl,
    cluster_cols=["date_key"],
    comment="Conformed calendar date dimension with weekend and US federal holiday flags.",
)
write_gold_table(
    dim_time_df,
    dim_time_tbl,
    cluster_cols=["time_key"],
    comment="Minute-of-day time dimension with NYC AM/PM Rush Hour marketplace buckets.",
)
write_gold_table(
    dim_weather_df,
    dim_weather_tbl,
    cluster_cols=["weather_hour_key"],
    comment="Hourly NYC weather observations from Open-Meteo with storm & precipitation flags.",
)
write_gold_table(
    dim_driver_df,
    dim_driver_tbl,
    cluster_cols=["driver_id"],
    comment="SCD Type 2 Driver dimension tracking vehicle EV upgrades, ratings, and Uber Pro tiers.",
)
write_gold_table(
    dim_rider_df,
    dim_rider_tbl,
    cluster_cols=["rider_id"],
    comment="SCD Type 2 Rider dimension tracking Uber One membership history and payment methods.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Materialize Gold Fact Tables (`fact_trips`, `fact_hex_supply_demand_5m`, `fact_driver_daily_summary`)

# COMMAND ----------

silver_trips_df = spark.table(cfg.table_fqn("silver", "trips_enriched"))  # type: ignore[name-defined]
silver_hex_5m_tbl = cfg.table_fqn("silver", "hex_telemetry_5m")
silver_lc_tbl = cfg.table_fqn("silver", "trip_lifecycle_clean")

silver_hex_5m_df = (
    spark.table(silver_hex_5m_tbl) if spark.catalog.tableExists(silver_hex_5m_tbl) else None  # type: ignore[name-defined]
)
silver_lc_df = (
    spark.table(silver_lc_tbl) if spark.catalog.tableExists(silver_lc_tbl) else None  # type: ignore[name-defined]
)

fact_trips_tbl = cfg.table_fqn("gold", "fact_trips")
fact_hex_tbl = cfg.table_fqn("gold", "fact_hex_supply_demand_5m")
fact_drv_daily_tbl = cfg.table_fqn("gold", "fact_driver_daily_summary")

fact_trips_df = build_fact_trips(silver_trips_df, dim_driver_df, dim_rider_df)
write_gold_table(
    fact_trips_df,
    fact_trips_tbl,
    cluster_cols=["pickup_date_key", "pickup_h3_r7"],
    comment="Core Uber trip fact table with point-in-time SCD2 keys, SLA wait times, H3 indices, and platform take-rate.",
)

fact_hex_df = build_fact_hex_supply_demand_5m(
    silver_trips_df=silver_trips_df,
    silver_hex_pings_5m_df=silver_hex_5m_df,
    silver_lifecycle_df=silver_lc_df,
)
write_gold_table(
    fact_hex_df,
    fact_hex_tbl,
    cluster_cols=["window_start_ts", "h3_r7"],
    comment="5-minute Uber H3 Resolution-7 periodic snapshot of driver supply, ride demand, and dynamic surge pricing.",
)

fact_drv_daily_df = build_fact_driver_daily_summary(
    fact_trips_df=spark.table(fact_trips_tbl),  # type: ignore[name-defined]
    dim_driver_scd2_df=dim_driver_df,
)
write_gold_table(
    fact_drv_daily_df,
    fact_drv_daily_tbl,
    cluster_cols=["pickup_date_key", "home_borough"],
    comment="Daily driver earnings, tips, engaged hours, and hourly earnings utilization.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Gold Warehouse Executive Verification Summary

# COMMAND ----------

for tbl in [
    dim_zone_tbl,
    dim_date_tbl,
    dim_time_tbl,
    dim_weather_tbl,
    dim_driver_tbl,
    dim_rider_tbl,
    fact_trips_tbl,
    fact_hex_tbl,
    fact_drv_daily_tbl,
]:
    print(f"{tbl:<55} : {spark.table(tbl).count():>10,} rows")  # type: ignore[name-defined]

print("\nUber Marketplace Product Mix & Platform Take-Rate:")
(
    spark.table(fact_trips_tbl)  # type: ignore[name-defined]
    .groupBy("product_type")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.sum("total_rider_amount"), 2).alias("gross_bookings_usd"),
        F.round(F.avg("base_passenger_fare"), 2).alias("avg_base_fare"),
        F.round(F.avg("driver_pay_amount"), 2).alias("avg_driver_pay"),
        F.round(F.avg("take_rate_pct"), 2).alias("avg_take_rate_pct"),
        F.round(F.avg("wait_time_sec") / 60.0, 2).alias("avg_wait_min"),
    )
    .orderBy(F.col("trips").desc())
    .show(truncate=False)
)
