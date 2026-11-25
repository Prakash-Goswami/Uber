# Databricks notebook source
# MAGIC %md
# MAGIC # 00. Uber Lakehouse Platform — Setup, Governance & Landing Volume Initialization
# MAGIC
# MAGIC This notebook initializes the **Unity Catalog** (or fallback Hive Metastore) namespace, Medallion schemas, and Managed Landing Volume:
# MAGIC - **Catalog**: `uber_lakehouse` (auto-falls back to default UC catalog such as `workspace`/`main` on Databricks Free Edition, or `spark_catalog` on legacy clusters)
# MAGIC - **Schemas**: `raw_landing`, `bronze`, `silver`, `gold`, `ml_features`
# MAGIC - **Managed Volume**: `/Volumes/<catalog>/raw_landing/landing_zone` for raw Parquet, CSV, Weather JSON, and Streaming Telemetry micro-batches

# COMMAND ----------

import os
import sys

# Ensure `src/` is on sys.path whether run from Databricks Git Folders, Workflows, or CLI
for candidate in [
    os.path.abspath(os.path.join(os.getcwd(), "..", "src")),
    os.path.abspath(os.path.join(os.getcwd(), "src")),
]:
    if os.path.isdir(candidate) and candidate not in sys.path:
        sys.path.insert(0, candidate)

from uber_platform.config import bootstrap_lakehouse

# COMMAND ----------

# Configure interactive Databricks widgets (gracefully ignored outside Databricks UI)
try:
    dbutils.widgets.text("catalog_name", "uber_lakehouse", "1. Target Catalog")  # type: ignore[name-defined]
    dbutils.widgets.text("sample_rows", "150000", "2. HVFHV Sample Rows (0=Full)")  # type: ignore[name-defined]
    CATALOG_NAME = dbutils.widgets.get("catalog_name").strip() or "uber_lakehouse"  # type: ignore[name-defined]
    SAMPLE_ROWS = int(dbutils.widgets.get("sample_rows").strip() or "150000")  # type: ignore[name-defined]
except Exception:
    CATALOG_NAME = "uber_lakehouse"
    SAMPLE_ROWS = 150_000

cfg = bootstrap_lakehouse(
    spark=spark,  # type: ignore[name-defined]
    preferred_catalog=CATALOG_NAME,
    sample_rows=SAMPLE_ROWS,
)

print("=" * 78)
print("UBER LAKEHOUSE ENVIRONMENT INITIALIZED SUCCESSFULLY")
print("=" * 78)
print(f"Active Catalog           : {cfg.catalog} (Unity Catalog={cfg.use_unity_catalog})")
print(f"Bronze Schema            : {cfg.schema_fqn(cfg.bronze_schema)}")
print(f"Silver Schema            : {cfg.schema_fqn(cfg.silver_schema)}")
print(f"Gold Schema              : {cfg.schema_fqn(cfg.gold_schema)}")
print(f"ML Features Schema       : {cfg.schema_fqn(cfg.ml_schema)}")
print(f"Landing Volume (POSIX)   : {cfg.volume_posix_path}")
print(f"Landing Volume (Spark)   : {cfg.volume_spark_path}")
print(f"Checkpoints Root         : {cfg.checkpoints_spark}")
print(f"Default Sample Rows      : {cfg.default_sample_rows:,}")
print("=" * 78)

# COMMAND ----------

# Verify created schemas in active catalog
if cfg.use_unity_catalog:
    schemas_df = spark.sql(f"SHOW SCHEMAS IN `{cfg.catalog}`")  # type: ignore[name-defined]
else:
    schemas_df = spark.sql("SHOW DATABASES LIKE 'uber_*'")  # type: ignore[name-defined]

schemas_df.show(truncate=False)
