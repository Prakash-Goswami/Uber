"""
Environment-aware configuration for Databricks Unity Catalog, Volumes, and Legacy Hive Metastore.

Designed to work seamlessly across:
1. Databricks Free Edition / Trial (Serverless + Unity Catalog)
2. Databricks Enterprise (AWS / Azure / GCP with Unity Catalog)
3. Legacy Databricks Clusters (spark_catalog + DBFS)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


@dataclass
class LakehouseConfig:
    """Centralized configuration for all schemas, tables, volumes, and checkpoints."""

    catalog: str = "uber_lakehouse"
    use_unity_catalog: bool = True
    landing_schema: str = "raw_landing"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    ml_schema: str = "ml_features"
    volume_name: str = "landing_zone"
    fallback_base_dir: str = "/tmp/uber_lakehouse"
    default_sample_rows: int = 150_000  # Set to 0 for full ~19.6M monthly NYC HVFHV trips
    target_year_month: str = "2024-01"

    def schema_fqn(self, layer_schema: str) -> str:
        """Return `<catalog>.<schema>` on Unity Catalog, or `uber_<schema>` on Hive metastore."""
        if self.use_unity_catalog:
            return f"`{self.catalog}`.`{layer_schema}`"
        return f"`uber_{layer_schema}`"

    def table_fqn(self, layer: str, table_name: str) -> str:
        """Return fully-qualified table name for bronze/silver/gold/ml layers."""
        schema_map = {
            "raw": self.landing_schema,
            "bronze": self.bronze_schema,
            "silver": self.silver_schema,
            "gold": self.gold_schema,
            "ml": self.ml_schema,
            "ml_features": self.ml_schema,
        }
        resolved_schema = schema_map.get(layer, layer)
        return f"{self.schema_fqn(resolved_schema)}.`{table_name}`"

    @property
    def volume_posix_path(self) -> str:
        """Return POSIX filesystem path used by Python HTTP downloaders & JSON simulator."""
        if self.use_unity_catalog:
            return f"/Volumes/{self.catalog}/{self.landing_schema}/{self.volume_name}"
        if os.path.exists("/dbfs"):
            return f"/dbfs{self.fallback_base_dir}/landing_zone"
        return f"{self.fallback_base_dir}/landing_zone"

    @property
    def volume_spark_path(self) -> str:
        """Return Spark URI path used by spark.read / spark.readStream / checkpoints."""
        if self.use_unity_catalog:
            return f"/Volumes/{self.catalog}/{self.landing_schema}/{self.volume_name}"
        if os.path.exists("/dbfs"):
            return f"dbfs:{self.fallback_base_dir}/landing_zone"
        return f"file:{self.fallback_base_dir}/landing_zone"

    @property
    def parquet_landing_posix(self) -> str:
        return f"{self.volume_posix_path}/batch/fhvhv"

    @property
    def parquet_landing_spark(self) -> str:
        return f"{self.volume_spark_path}/batch/fhvhv"

    @property
    def reference_landing_posix(self) -> str:
        return f"{self.volume_posix_path}/reference"

    @property
    def reference_landing_spark(self) -> str:
        return f"{self.volume_spark_path}/reference"

    @property
    def gps_stream_posix(self) -> str:
        return f"{self.volume_posix_path}/streams/driver_gps_pings"

    @property
    def gps_stream_spark(self) -> str:
        return f"{self.volume_spark_path}/streams/driver_gps_pings"

    @property
    def trip_stream_posix(self) -> str:
        return f"{self.volume_posix_path}/streams/trip_lifecycle_events"

    @property
    def trip_stream_spark(self) -> str:
        return f"{self.volume_spark_path}/streams/trip_lifecycle_events"

    @property
    def driver_cdc_posix(self) -> str:
        return f"{self.volume_posix_path}/streams/cdc_drivers"

    @property
    def driver_cdc_spark(self) -> str:
        return f"{self.volume_spark_path}/streams/cdc_drivers"

    @property
    def rider_cdc_posix(self) -> str:
        return f"{self.volume_posix_path}/streams/cdc_riders"

    @property
    def rider_cdc_spark(self) -> str:
        return f"{self.volume_spark_path}/streams/cdc_riders"

    @property
    def checkpoints_spark(self) -> str:
        return f"{self.volume_spark_path}/_checkpoints"


def bootstrap_lakehouse(
    spark: Any,
    preferred_catalog: str = "uber_lakehouse",
    sample_rows: int = 150_000,
) -> LakehouseConfig:
    """
    Initialize Unity Catalog (or fallback catalog/schemas) and create landing directories.

    Handles permissions gracefully on Databricks Free Edition (where `CREATE CATALOG` may or
    may not be permitted; if `CREATE CATALOG` is restricted, automatically uses the active
    default Unity Catalog such as `workspace` or `main` before falling back to Hive metastore).
    """
    chosen_catalog = preferred_catalog
    use_uc = True

    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{preferred_catalog}`")
        spark.sql(f"USE CATALOG `{preferred_catalog}`")
    except Exception:
        # On Databricks Free Edition / restricted UC workspaces, a default UC catalog
        # like `workspace` or `main` already exists and allows creating schemas + volumes.
        try:
            current_cat = spark.sql("SELECT current_catalog() AS cat").collect()[0]["cat"]
            if current_cat and current_cat.lower() not in ("spark_catalog", "hive_metastore"):
                chosen_catalog = current_cat
                use_uc = True
            else:
                use_uc = False
        except Exception:
            use_uc = False

    cfg = LakehouseConfig(
        catalog=chosen_catalog,
        use_unity_catalog=use_uc,
        default_sample_rows=sample_rows,
    )

    all_schemas = [
        cfg.landing_schema,
        cfg.bronze_schema,
        cfg.silver_schema,
        cfg.gold_schema,
        cfg.ml_schema,
    ]

    for sch in all_schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema_fqn(sch)}")

    if cfg.use_unity_catalog:
        spark.sql(
            f"CREATE VOLUME IF NOT EXISTS `{cfg.catalog}`.`{cfg.landing_schema}`.`{cfg.volume_name}`"
        )

    for folder in [
        cfg.parquet_landing_posix,
        cfg.reference_landing_posix,
        cfg.gps_stream_posix,
        cfg.trip_stream_posix,
        cfg.driver_cdc_posix,
        cfg.rider_cdc_posix,
    ]:
        os.makedirs(folder, exist_ok=True)

    return cfg
