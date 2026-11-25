# Databricks notebook source
# MAGIC %md
# MAGIC # 01. Public Free Data Ingestion $\rightarrow$ Bronze Layer (Delta Lake)
# MAGIC
# MAGIC Downloads **100% real, free public datasets** (zero API keys required) into our Unity Catalog Landing Volume and ingests them as immutable **Bronze Delta Tables** with audit metadata (`_ingest_ts`, `_source_file`, `_batch_id`):
# MAGIC 1. **NYC TLC High-Volume For-Hire Vehicle (`fhvhv_tripdata_2024-01.parquet`)**: Actual NYC Uber (`HV0003`), Lyft (`HV0005`), and Via (`HV0004`) trip records with 4 timestamps + full rider fare & driver payout economics.
# MAGIC 2. **NYC TLC Taxi Zone Lookup (`taxi_zone_lookup.csv`)**: 265 NYC zones enriched with WGS84 `(centroid_lat, centroid_lon)` coordinates.
# MAGIC 3. **Open-Meteo Archive API (`nyc_weather_hourly.json`)**: Real hourly NYC temperature, rainfall, snowfall, wind speed, and WMO weather codes.

# COMMAND ----------

import os
import sys
import uuid

for candidate in [
    os.path.abspath(os.path.join(os.getcwd(), "..", "src")),
    os.path.abspath(os.path.join(os.getcwd(), "src")),
]:
    if os.path.isdir(candidate) and candidate not in sys.path:
        sys.path.insert(0, candidate)

from pyspark.sql import functions as F

from uber_platform.config import bootstrap_lakehouse
from uber_platform.data_ingestion import (
    download_fhvhv_uber_parquet,
    download_nyc_hourly_weather,
    download_taxi_zones_with_coords,
)
from uber_platform.schemas import get_taxi_zone_schema, get_weather_hourly_schema

# COMMAND ----------

try:
    dbutils.widgets.text("catalog_name", "uber_lakehouse", "1. Target Catalog")  # type: ignore[name-defined]
    dbutils.widgets.text("sample_rows", "150000", "2. HVFHV Sample Rows (0=Full)")  # type: ignore[name-defined]
    dbutils.widgets.text("year_month", "2024-01", "3. NYC TLC Year-Month")  # type: ignore[name-defined]
    CATALOG_NAME = dbutils.widgets.get("catalog_name").strip() or "uber_lakehouse"  # type: ignore[name-defined]
    SAMPLE_ROWS = int(dbutils.widgets.get("sample_rows").strip() or "150000")  # type: ignore[name-defined]
    YEAR_MONTH = dbutils.widgets.get("year_month").strip() or "2024-01"  # type: ignore[name-defined]
except Exception:
    CATALOG_NAME = "uber_lakehouse"
    SAMPLE_ROWS = 150_000
    YEAR_MONTH = "2024-01"

cfg = bootstrap_lakehouse(spark, preferred_catalog=CATALOG_NAME, sample_rows=SAMPLE_ROWS)  # type: ignore[name-defined]
batch_id = str(uuid.uuid4())[:8]

# Enable Delta Lake Auto-Optimize & Change Data Feed defaults where supported
for conf_k, conf_v in [
    ("spark.databricks.delta.optimizeWrite.enabled", "true"),
    ("spark.databricks.delta.autoCompact.enabled", "true"),
]:
    try:
        spark.conf.set(conf_k, conf_v)  # type: ignore[name-defined]
    except Exception:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Download Public Datasets into Managed Landing Volume

# COMMAND ----------

zones_csv_posix = download_taxi_zones_with_coords(cfg.reference_landing_posix)
weather_json_posix = download_nyc_hourly_weather(
    cfg.reference_landing_posix,
    start_date=f"{YEAR_MONTH}-01",
    end_date=f"{YEAR_MONTH}-31" if YEAR_MONTH.endswith("01") else f"{YEAR_MONTH}-28",
)
fhvhv_parquet_posix = download_fhvhv_uber_parquet(
    cfg.parquet_landing_posix,
    year_month=YEAR_MONTH,
    sample_rows=SAMPLE_ROWS,
)

print(f"Taxi Zones CSV   : {zones_csv_posix}")
print(f"Weather JSON     : {weather_json_posix}")
print(f"HVFHV Parquet    : {fhvhv_parquet_posix}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Ingest Reference Datasets into `bronze.taxi_zones_raw` & `bronze.hourly_weather_raw`

# COMMAND ----------

zones_spark_file = f"{cfg.reference_landing_spark}/{os.path.basename(zones_csv_posix)}"
weather_spark_file = f"{cfg.reference_landing_spark}/{os.path.basename(weather_json_posix)}"
fhvhv_spark_file = f"{cfg.parquet_landing_spark}/{os.path.basename(fhvhv_parquet_posix)}"

bronze_zones_tbl = cfg.table_fqn("bronze", "taxi_zones_raw")
bronze_weather_tbl = cfg.table_fqn("bronze", "hourly_weather_raw")
bronze_trips_tbl = cfg.table_fqn("bronze", "fhvhv_trips_raw")

zones_raw_df = (
    spark.read.format("csv")  # type: ignore[name-defined]
    .option("header", "true")
    .schema(get_taxi_zone_schema())
    .load(zones_spark_file)
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_batch_id", F.lit(batch_id))
    .withColumn("_source_file", F.lit(zones_spark_file))
)
zones_raw_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    bronze_zones_tbl
)

weather_raw_df = (
    spark.read.format("json")  # type: ignore[name-defined]
    .schema(get_weather_hourly_schema())
    .load(weather_spark_file)
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_batch_id", F.lit(batch_id))
    .withColumn("_source_file", F.lit(weather_spark_file))
)
weather_raw_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    bronze_weather_tbl
)

print(f"Loaded {spark.table(bronze_zones_tbl).count():,} rows -> {bronze_zones_tbl}")  # type: ignore[name-defined]
print(f"Loaded {spark.table(bronze_weather_tbl).count():,} rows -> {bronze_weather_tbl}")  # type: ignore[name-defined]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Ingest NYC TLC HVFHV Trips into `bronze.fhvhv_trips_raw`
# MAGIC Notice how `HV0003` (Uber) dominates NYC rideshare volume (~72%+ of all trips)!

# COMMAND ----------

fhvhv_raw_df = (
    spark.read.format("parquet")  # type: ignore[name-defined]
    .load(fhvhv_spark_file)
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_batch_id", F.lit(batch_id))
    .withColumn("_source_file", F.lit(fhvhv_spark_file))
)

(
    fhvhv_raw_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(bronze_trips_tbl)
)

# Summarize raw volume by Rideshare Operator License
operator_summary = (
    spark.table(bronze_trips_tbl)  # type: ignore[name-defined]
    .groupBy("hvfhs_license_num")
    .agg(
        F.count("*").alias("raw_trip_count"),
        F.round(F.avg("trip_miles"), 2).alias("avg_miles"),
        F.round(F.avg("base_passenger_fare"), 2).alias("avg_base_fare_usd"),
        F.round(F.avg("driver_pay"), 2).alias("avg_driver_pay_usd"),
    )
    .withColumn(
        "operator_name",
        F.when(F.col("hvfhs_license_num") == "HV0003", F.lit("Uber"))
        .when(F.col("hvfhs_license_num") == "HV0005", F.lit("Lyft"))
        .when(F.col("hvfhs_license_num") == "HV0004", F.lit("Via"))
        .otherwise(F.lit("Other")),
    )
    .orderBy(F.col("raw_trip_count").desc())
)

operator_summary.show(truncate=False)
