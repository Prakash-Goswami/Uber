# Databricks notebook source
# MAGIC %md
# MAGIC # 08. Single-Click End-to-End Uber Lakehouse Master Runner
# MAGIC
# MAGIC Executes the **complete Uber Data Engineering, Streaming, Kimball Warehouse & Michelangelo AI/ML Platform** sequentially in a single click on Databricks (`00` $\rightarrow$ `07`), tracks stage timings, and outputs a full table & model audit report!

# COMMAND ----------

import os
import runpy
import sys
import time

for candidate in [
    os.path.abspath(os.path.join(os.getcwd(), "..", "src")),
    os.path.abspath(os.path.join(os.getcwd(), "src")),
]:
    if os.path.isdir(candidate) and candidate not in sys.path:
        sys.path.insert(0, candidate)

from uber_platform.config import bootstrap_lakehouse

# COMMAND ----------

try:
    dbutils.widgets.text("catalog_name", "uber_lakehouse", "1. Target Catalog")  # type: ignore[name-defined]
    dbutils.widgets.text("sample_rows", "150000", "2. HVFHV Sample Rows (0=Full)")  # type: ignore[name-defined]
    CATALOG_NAME = dbutils.widgets.get("catalog_name").strip() or "uber_lakehouse"  # type: ignore[name-defined]
    SAMPLE_ROWS = dbutils.widgets.get("sample_rows").strip() or "150000"  # type: ignore[name-defined]
except Exception:
    CATALOG_NAME = "uber_lakehouse"
    SAMPLE_ROWS = "150000"

NOTEBOOK_STAGES = [
    "00_setup_and_config",
    "01_public_data_ingestion_bronze",
    "02_realtime_event_simulator_and_streaming",
    "03_silver_cleansing_geospatial_scd2",
    "04_gold_dimensional_warehouse",
    "05_ml_eta_prediction",
    "06_ml_surge_pricing_forecast",
    "07_ml_cancellation_and_fraud",
]

stage_results = []
overall_t0 = time.time()

for stage in NOTEBOOK_STAGES:
    t0 = time.time()
    print(f"\n>>> [STARTING STAGE] {stage} ...")
    args = {"catalog_name": CATALOG_NAME, "sample_rows": SAMPLE_ROWS}

    ran_via_dbutils = False
    try:
        # Native Databricks child notebook execution when running interactively in Workspace
        dbutils.notebook.run(f"./{stage}", timeout_seconds=1800, arguments=args)  # type: ignore[name-defined]
        ran_via_dbutils = True
    except Exception:
        # Fallback: direct in-process Python execution (great for spark-submit / local / Serverless scripts)
        nb_dir = os.getcwd() if os.path.exists(f"{stage}.py") else os.path.join(os.getcwd(), "notebooks")
        script_path = os.path.join(nb_dir, f"{stage}.py")
        init_globals = {"spark": spark}  # type: ignore[name-defined]
        try:
            init_globals["dbutils"] = dbutils  # type: ignore[name-defined]
        except NameError:
            pass
        runpy.run_path(script_path, init_globals=init_globals)

    elapsed = round(time.time() - t0, 1)
    mode_str = "dbutils.notebook" if ran_via_dbutils else "in-process"
    stage_results.append((stage, "SUCCESS", elapsed, mode_str))
    print(f"<<< [COMPLETED] {stage} in {elapsed}s ({mode_str})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final End-to-End Platform Verification Report

# COMMAND ----------

cfg = bootstrap_lakehouse(spark, preferred_catalog=CATALOG_NAME, sample_rows=int(SAMPLE_ROWS))  # type: ignore[name-defined]
total_elapsed = round(time.time() - overall_t0, 1)

print("=" * 84)
print(f"UBER DATABRICKS LAKEHOUSE — END-TO-END EXECUTION SUMMARY ({total_elapsed}s total)")
print("=" * 84)
for stage_name, status, sec, mode in stage_results:
    print(f"  {stage_name:<45} | {status:<8} | {sec:>6.1f}s | {mode}")
print("-" * 84)

expected_tables = [
    ("bronze", "taxi_zones_raw"),
    ("bronze", "hourly_weather_raw"),
    ("bronze", "fhvhv_trips_raw"),
    ("bronze", "driver_pings_stream"),
    ("bronze", "trip_lifecycle_stream"),
    ("bronze", "driver_cdc_raw"),
    ("bronze", "rider_cdc_raw"),
    ("silver", "weather_hourly"),
    ("silver", "trips_enriched"),
    ("silver", "trips_quarantine"),
    ("silver", "hex_telemetry_5m"),
    ("silver", "trip_lifecycle_clean"),
    ("silver", "dim_driver_scd2"),
    ("silver", "dim_rider_scd2"),
    ("gold", "dim_zone"),
    ("gold", "dim_date"),
    ("gold", "dim_time"),
    ("gold", "dim_weather"),
    ("gold", "dim_driver_scd2"),
    ("gold", "dim_rider_scd2"),
    ("gold", "fact_trips"),
    ("gold", "fact_hex_supply_demand_5m"),
    ("gold", "fact_driver_daily_summary"),
    ("gold", "ml_eta_predictions"),
    ("gold", "ml_hex_surge_forecasts"),
    ("gold", "ml_trip_risk_scores"),
    ("ml_features", "trip_eta_features"),
    ("ml_features", "hex_surge_features"),
]

for layer, tbl_name in expected_tables:
    fqn = cfg.table_fqn(layer, tbl_name)
    cnt = spark.table(fqn).count()  # type: ignore[name-defined]
    print(f"  {fqn:<58} | {cnt:>10,} rows")
print("=" * 84)
