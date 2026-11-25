# 08. Step-by-Step Databricks Testing & Execution Guide

This repository is engineered to run out-of-the-box on **any Databricks environment**, including:
- **Databricks Free Edition / Trial (Serverless + Unity Catalog)**
- **Databricks Enterprise Workspace (AWS / Azure / GCP)**
- **Legacy Hive Metastore Clusters (`spark_catalog`)**

---

## Option A: Fastest Interactive Test via Databricks Git Folders (Recommended — 5 Minutes)

### Step 1: Push This Repo to GitHub & Clone in Databricks
1. Push this local Git repository to your GitHub / GitLab / Bitbucket account:
   ```bash
   git remote add origin https://github.com/<your-username>/uber-databricks-lakehouse.git
   git push -u origin main
   ```
2. Log into your **Databricks Workspace**.
3. In the left sidebar, click **Workspace** $\rightarrow$ **Users** $\rightarrow$ `<your-email>` $\rightarrow$ **Create** $\rightarrow$ **Git folder**.
4. Paste your repository URL and click **Create Git folder**.

### Step 2: Run Either One-Click Master Runner OR Individual Notebooks
Attach **Serverless Compute** (or any DBR 14.3 LTS / 15.x / 16.x cluster):
- **Single-Click Full Execution**: Open `notebooks/08_end_to_end_runner` and click **Run All**. It executes notebooks `00` through `07` in sequence and prints a complete verification report!
- **Step-by-Step Exploration** (great for learning & debugging):
  1. `notebooks/00_setup_and_config` — Creates Unity Catalog `uber_lakehouse` (or falls back to `spark_catalog`), schemas (`raw_landing`, `bronze`, `silver`, `gold`, `ml_features`), and Managed Volume `landing_zone`.
  2. `notebooks/01_public_data_ingestion_bronze` — Downloads **real NYC TLC Uber (`HV0003`) Parquet**, **NYC Taxi Zones CSV**, and **Open-Meteo Weather API JSON** into the landing volume and loads Bronze Delta tables.
  3. `notebooks/02_realtime_event_simulator_and_streaming` — Replays live driver GPS pings, trip lifecycle state transitions, and driver CDC events into streaming folders, then processes them with **Spark Structured Streaming / Auto Loader + Watermarking + Deduplication**.
  4. `notebooks/03_silver_cleansing_geospatial_scd2` — Applies DQ rules (splitting clean `silver.trips_enriched` vs `silver.trips_quarantine`), indexes coordinates into **Uber H3 Res-7 & Res-8**, and runs **SCD Type 2 `MERGE INTO`** for `dim_driver_scd2` and `dim_rider_scd2`.
  5. `notebooks/04_gold_dimensional_warehouse` — Builds the complete **Kimball Star Schema** (`dim_zone`, `dim_date`, `dim_time`, `dim_weather`, `fact_trips`, `fact_hex_supply_demand_5m`, `fact_driver_daily_summary`) with Liquid Clustering.
  6. `notebooks/05_ml_eta_prediction` — Trains, evaluates, and logs the **DeepETA PySpark `GBTRegressor`** model with **MLflow**.
  7. `notebooks/06_ml_surge_pricing_forecast` — Trains and scores the **H3 Spatio-Temporal Surge Pricing Forecaster**.
  8. `notebooks/07_ml_cancellation_and_fraud` — Runs **Cancellation Risk `GBTClassifier`** + **Long-Hauling / GPS Anomaly Detection**.

---

## Option B: Declarative Lakeflow / Delta Live Tables (DLT) Pipeline

To test Databricks' declarative **Lakeflow / Delta Live Tables** engine with built-in visual lineage and Data Quality expectation metrics:
1. In Databricks left sidebar, go to **Jobs & Pipelines** (or **Delta Live Tables**) $\rightarrow$ **Create Pipeline**.
2. Set **Pipeline name**: `uber_medallion_dlt_pipeline`.
3. Choose **Serverless** (or Core/Pro/Advanced DLT).
4. Set **Source code** to `pipelines/dlt_uber_medallion_pipeline.py` inside your Git folder.
5. Set **Target catalog**: `uber_lakehouse` and **Target schema**: `dlt_uber` (after running `00_setup_and_config` and `01_public_data_ingestion_bronze` so raw files exist in the landing volume).
6. Add configuration parameter: `uber.landing_path` = `/Volumes/uber_lakehouse/raw_landing/landing_zone` (or copy the path printed by Notebook `00`).
7. Click **Start** to watch the live DLT DAG and Data Quality pass/fail expectation counters!

---

## Option C: Automated Deployment via Databricks Asset Bundles (`databricks.yml`)

If you use the Databricks CLI locally:
```bash
# 1. Authenticate to your Databricks workspace
databricks auth login --host https://<your-workspace-url>

# 2. Validate bundle configuration
databricks bundle validate -t dev

# 3. Deploy notebooks, workflow DAG, and DLT pipeline
databricks bundle deploy -t dev

# 4. Trigger the full multi-task Databricks Workflow Job
databricks bundle run uber_end_to_end_lakehouse_job -t dev
```

---

## Option D: Databricks SQL Warehouse & Dashboards

1. Open **SQL Editor** in Databricks.
2. Select your **Serverless SQL Warehouse**.
3. Open `sql/02_analytical_warehouse_queries.sql` and run any of the **10 production analytical queries** to inspect Uber's take-rate, H3 surge maps, airport curb wait SLAs, and weather elasticity!

---

## Controlling Data Volume & Cost

In `notebooks/00_setup_and_config.py` (or via Databricks Job Widget `sample_rows`), `DEFAULT_SAMPLE_ROWS` is set to **`150,000` real Uber trips** by default so the entire end-to-end pipeline + 3 ML models finish in **~3–5 minutes** even on **Databricks Free Edition**!
Want to stress-test full scale (**~19.6 Million trips** in a single month)? Simply set widget `sample_rows = 0` (unlimited) in Notebook `01`!
