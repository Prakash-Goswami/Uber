# Databricks notebook source
# MAGIC %md
# MAGIC # 06. Michelangelo AI/ML Use Case #2 — Dynamic Surge Multiplier & H3 Demand Forecasting
# MAGIC
# MAGIC Simulates Uber's **Marketplace Spatio-Temporal Surge Pricing & Driver Repositioning** model:
# MAGIC 1. **Feature Engineering (`ml_features.hex_surge_features`)**:
# MAGIC    - Autoregressive window lags per `h3_r7` hexagon: `demand_lag_1`, `demand_lag_2`, `rolling_3_window_demand`, `demand_momentum`, `supply_lag_1`, `surge_lag_1`
# MAGIC    - Weather shocks (`precipitation_mm`, `wind_speed_kmh`, `is_adverse_weather_num`)
# MAGIC    - Cyclical temporal encodings (`hour_sin`, `hour_cos`, `dow_sin`, `dow_cos`)
# MAGIC    - **Target**: `next_window_surge_multiplier` (predicting surge in the upcoming window so Uber can alert idle drivers to reposition proactively!).
# MAGIC 2. **PySpark `GBTRegressor` + MLflow Tracking**.
# MAGIC 3. **Driver Repositioning Recommendations (`gold.ml_hex_surge_forecasts`)**.

# COMMAND ----------

import os
import sys

for candidate in [
    os.path.abspath(os.path.join(os.getcwd(), "..", "src")),
    os.path.abspath(os.path.join(os.getcwd(), "src")),
]:
    if os.path.isdir(candidate) and candidate not in sys.path:
        sys.path.insert(0, candidate)

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import Window
from pyspark.sql import functions as F

from uber_platform.config import bootstrap_lakehouse
from uber_platform.ml_utils import (
    add_cyclical_time_features,
    chronological_train_test_split,
    configure_databricks_mlflow,
    evaluate_regression_predictions,
)

# COMMAND ----------

try:
    dbutils.widgets.text("catalog_name", "uber_lakehouse", "1. Target Catalog")  # type: ignore[name-defined]
    CATALOG_NAME = dbutils.widgets.get("catalog_name").strip() or "uber_lakehouse"  # type: ignore[name-defined]
except Exception:
    CATALOG_NAME = "uber_lakehouse"

cfg = bootstrap_lakehouse(spark, preferred_catalog=CATALOG_NAME)  # type: ignore[name-defined]
mlflow = configure_databricks_mlflow(spark, "uber_michelangelo_surge_forecasting")  # type: ignore[name-defined]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Build Spatio-Temporal Lag & Lead Feature Table (`ml_features.hex_surge_features`)

# COMMAND ----------

fact_hex = spark.table(cfg.table_fqn("gold", "fact_hex_supply_demand_5m"))  # type: ignore[name-defined]
dim_weather = spark.table(cfg.table_fqn("gold", "dim_weather"))  # type: ignore[name-defined]

w_hex_time = Window.partitionBy("h3_r7").orderBy(F.col("window_start_ts").asc())
w_rolling_3 = w_hex_time.rowsBetween(-2, 0)

hex_ts_df = (
    fact_hex.join(
        F.broadcast(
            dim_weather.select(
                "weather_hour_key", "temp_celsius", "precipitation_mm", "wind_speed_kmh", "is_adverse_weather"
            )
        ),
        on="weather_hour_key",
        how="left",
    )
    .withColumn("demand_lag_1", F.coalesce(F.lag("ride_requests_demand", 1).over(w_hex_time), F.col("ride_requests_demand")))
    .withColumn("demand_lag_2", F.coalesce(F.lag("ride_requests_demand", 2).over(w_hex_time), F.col("ride_requests_demand")))
    .withColumn("supply_lag_1", F.coalesce(F.lag("active_drivers_supply", 1).over(w_hex_time), F.col("active_drivers_supply")))
    .withColumn("surge_lag_1", F.coalesce(F.lag("recommended_surge_multiplier", 1).over(w_hex_time), F.lit(1.0)))
    .withColumn("rolling_avg_demand", F.round(F.avg("ride_requests_demand").over(w_rolling_3), 2))
    .withColumn("demand_momentum", (F.col("ride_requests_demand") - F.col("demand_lag_2")).cast("double"))
    .withColumn("next_window_surge_multiplier", F.lead("recommended_surge_multiplier", 1).over(w_hex_time))
    .withColumn("next_window_demand", F.lead("ride_requests_demand", 1).over(w_hex_time))
    .filter(F.col("next_window_surge_multiplier").isNotNull())
)

hex_ts_df = add_cyclical_time_features(hex_ts_df, "window_start_ts")

surge_features_df = (
    hex_ts_df.withColumn("temp_celsius", F.coalesce(F.col("temp_celsius"), F.lit(5.0)))
    .withColumn("precipitation_mm", F.coalesce(F.col("precipitation_mm"), F.lit(0.0)))
    .withColumn("wind_speed_kmh", F.coalesce(F.col("wind_speed_kmh"), F.lit(10.0)))
    .withColumn("is_adverse_weather_num", F.when(F.col("is_adverse_weather"), F.lit(1.0)).otherwise(F.lit(0.0)))
    .select(
        "window_hex_id",
        "window_start_ts",
        "h3_r7",
        "active_drivers_supply",
        "ride_requests_demand",
        "demand_supply_ratio",
        "recommended_surge_multiplier",
        "avg_pickup_eta_sec",
        "demand_lag_1",
        "demand_lag_2",
        "supply_lag_1",
        "surge_lag_1",
        "rolling_avg_demand",
        "demand_momentum",
        "is_weekend_num",
        "hour_sin",
        "hour_cos",
        "dow_sin",
        "dow_cos",
        "temp_celsius",
        "precipitation_mm",
        "wind_speed_kmh",
        "is_adverse_weather_num",
        "next_window_demand",
        "next_window_surge_multiplier",
    )
)

surge_feat_tbl = cfg.table_fqn("ml_features", "hex_surge_features")
surge_features_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    surge_feat_tbl
)
print(f"Saved Surge Feature Table {surge_feat_tbl} ({spark.table(surge_feat_tbl).count():,} rows)")  # type: ignore[name-defined]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Train & Evaluate Spatio-Temporal Surge Forecaster (`GBTRegressor`)

# COMMAND ----------

surge_data = spark.table(surge_feat_tbl).orderBy("window_start_ts").limit(80_000)  # type: ignore[name-defined]
train_df, test_df = chronological_train_test_split(surge_data, "window_start_ts", train_ratio=0.80)

feature_cols = [
    "active_drivers_supply",
    "ride_requests_demand",
    "demand_supply_ratio",
    "recommended_surge_multiplier",
    "avg_pickup_eta_sec",
    "demand_lag_1",
    "demand_lag_2",
    "supply_lag_1",
    "surge_lag_1",
    "rolling_avg_demand",
    "demand_momentum",
    "is_weekend_num",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "temp_celsius",
    "precipitation_mm",
    "wind_speed_kmh",
    "is_adverse_weather_num",
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="keep")
gbt_surge = GBTRegressor(
    featuresCol="features",
    labelCol="next_window_surge_multiplier",
    predictionCol="forecasted_surge_multiplier",
    maxIter=30,
    maxDepth=5,
    stepSize=0.1,
    seed=42,
)
surge_pipeline = Pipeline(stages=[assembler, gbt_surge])

if mlflow is not None:
    with mlflow.start_run(run_name="h3_dynamic_surge_forecaster"):
        mlflow.log_params({"maxIter": 30, "maxDepth": 5, "horizon_min": 5})
        surge_model = surge_pipeline.fit(train_df)
        surge_preds = surge_model.transform(test_df)
        metrics = evaluate_regression_predictions(
            surge_preds,
            label_col="next_window_surge_multiplier",
            prediction_col="forecasted_surge_multiplier",
        )
        mlflow.log_metrics(metrics)
else:
    surge_model = surge_pipeline.fit(train_df)
    surge_preds = surge_model.transform(test_df)
    metrics = evaluate_regression_predictions(
        surge_preds,
        label_col="next_window_surge_multiplier",
        prediction_col="forecasted_surge_multiplier",
    )

print("=" * 68)
print("DYNAMIC SURGE FORECASTER EVALUATION (OUT-OF-TIME)")
print("=" * 68)
for k, v in metrics.items():
    print(f"  {k:<15}: {v}")
print("=" * 68)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Generate Actionable Driver Repositioning Alerts (`gold.ml_hex_surge_forecasts`)

# COMMAND ----------

surge_out_tbl = cfg.table_fqn("gold", "ml_hex_surge_forecasts")
reposition_alerts_df = (
    surge_preds.withColumn(
        "forecasted_surge_multiplier",
        F.round(F.least(F.lit(4.0), F.greatest(F.lit(1.0), F.col("forecasted_surge_multiplier"))), 2),
    )
    .withColumn(
        "dispatch_action",
        F.when(F.col("forecasted_surge_multiplier") >= 1.6, F.lit("HIGH_PRIORITY_DRIVER_INCENTIVE_ALERT"))
        .when(F.col("forecasted_surge_multiplier") >= 1.25, F.lit("NUDGE_NEARBY_IDLE_DRIVERS"))
        .otherwise(F.lit("BALANCED_NO_ACTION")),
    )
    .select(
        "window_start_ts",
        "h3_r7",
        "active_drivers_supply",
        "ride_requests_demand",
        "recommended_surge_multiplier",
        "next_window_surge_multiplier",
        "forecasted_surge_multiplier",
        "dispatch_action",
    )
)

reposition_alerts_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    surge_out_tbl
)

spark.table(surge_out_tbl).groupBy("dispatch_action").count().orderBy(F.col("count").desc()).show(truncate=False)  # type: ignore[name-defined]
spark.table(surge_out_tbl).orderBy(F.col("forecasted_surge_multiplier").desc()).show(10, truncate=False)  # type: ignore[name-defined]
