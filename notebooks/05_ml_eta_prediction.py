# Databricks notebook source
# MAGIC %md
# MAGIC # 05. Michelangelo AI/ML Use Case #1 — DeepETA Travel Duration Prediction
# MAGIC
# MAGIC Simulates Uber's **DeepETA** arrival & travel-time estimation service on Databricks:
# MAGIC 1. **Feature Store Table (`ml_features.trip_eta_features`)**:
# MAGIC    - Geospatial features: `trip_miles`, `haversine_km`, `route_circuity_ratio`, `is_airport_trip`, `same_borough_flag`
# MAGIC    - Cyclical temporal encodings: `hour_sin`, `hour_cos`, `dow_sin`, `dow_cos`, `is_weekend_num`
# MAGIC    - Historical H3 corridor baseline speed (`corridor_baseline_speed_mph`) computed **strictly on the training window** to avoid target leakage!
# MAGIC    - Real-time weather context (`temp_celsius`, `precipitation_mm`, `wind_speed_kmh`, `is_adverse_weather_num`)
# MAGIC 2. **Distributed PySpark ML Pipeline (`GBTRegressor`)**:
# MAGIC    - Trained on chronological 80% split, evaluated out-of-time on final 20% (`RMSE`, `MAE`, `R2`, `MAPE %`).
# MAGIC 3. **MLflow Experiment Tracking & Batch Scoring (`gold.ml_eta_predictions`)**.

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
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
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
mlflow = configure_databricks_mlflow(spark, "uber_michelangelo_eta_prediction")  # type: ignore[name-defined]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Assemble Point-in-Time ETA Feature Table (`ml_features.trip_eta_features`)

# COMMAND ----------

fact_trips = spark.table(cfg.table_fqn("gold", "fact_trips"))  # type: ignore[name-defined]
dim_zone = spark.table(cfg.table_fqn("gold", "dim_zone"))  # type: ignore[name-defined]
dim_weather = spark.table(cfg.table_fqn("gold", "dim_weather"))  # type: ignore[name-defined]

pu_z = dim_zone.select(F.col("location_id").alias("pu_location_id"), F.col("borough").alias("pu_borough"))
do_z = dim_zone.select(F.col("location_id").alias("do_location_id"), F.col("borough").alias("do_borough"))

base_eta_df = (
    fact_trips.filter("trip_duration_sec BETWEEN 60 AND 7200 AND trip_miles BETWEEN 0.2 AND 80.0")
    .join(F.broadcast(pu_z), on="pu_location_id", how="left")
    .join(F.broadcast(do_z), on="do_location_id", how="left")
    .join(
        F.broadcast(
            dim_weather.select(
                "weather_hour_key",
                "temp_celsius",
                "precipitation_mm",
                "wind_speed_kmh",
                "is_adverse_weather",
            )
        ),
        on="weather_hour_key",
        how="left",
    )
)

base_eta_df = add_cyclical_time_features(base_eta_df, "pickup_ts")

haversine_miles = F.greatest(F.col("haversine_km") * F.lit(0.621371), F.lit(0.15))

eta_features_df = (
    base_eta_df.withColumn("haversine_miles", F.round(haversine_miles, 3))
    .withColumn("route_circuity_ratio", F.round(F.least(F.lit(4.0), F.col("trip_miles") / haversine_miles), 3))
    .withColumn("same_borough_num", F.when(F.col("pu_borough") == F.col("do_borough"), F.lit(1.0)).otherwise(F.lit(0.0)))
    .withColumn("is_airport_num", F.when(F.col("is_airport_trip"), F.lit(1.0)).otherwise(F.lit(0.0)))
    .withColumn("is_shared_num", F.when(F.col("is_shared_trip"), F.lit(1.0)).otherwise(F.lit(0.0)))
    .withColumn("temp_celsius", F.coalesce(F.col("temp_celsius"), F.lit(5.0)))
    .withColumn("precipitation_mm", F.coalesce(F.col("precipitation_mm"), F.lit(0.0)))
    .withColumn("wind_speed_kmh", F.coalesce(F.col("wind_speed_kmh"), F.lit(10.0)))
    .withColumn("is_adverse_weather_num", F.when(F.col("is_adverse_weather"), F.lit(1.0)).otherwise(F.lit(0.0)))
    .withColumn("pu_borough", F.coalesce(F.col("pu_borough"), F.lit("Manhattan")))
    .withColumn("do_borough", F.coalesce(F.col("do_borough"), F.lit("Manhattan")))
    .select(
        "trip_id",
        "pickup_ts",
        "pickup_h3_r7",
        "dropoff_h3_r7",
        "pu_borough",
        "do_borough",
        "product_type",
        "trip_miles",
        "haversine_miles",
        "route_circuity_ratio",
        "same_borough_num",
        "is_airport_num",
        "is_shared_num",
        "surge_multiplier",
        "hour_of_day",
        "is_weekend_num",
        "hour_sin",
        "hour_cos",
        "dow_sin",
        "dow_cos",
        "temp_celsius",
        "precipitation_mm",
        "wind_speed_kmh",
        "is_adverse_weather_num",
        "trip_duration_sec",
    )
)

eta_feature_tbl = cfg.table_fqn("ml_features", "trip_eta_features")
eta_features_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    eta_feature_tbl
)
print(f"Saved Feature Table {eta_feature_tbl} ({spark.table(eta_feature_tbl).count():,} rows)")  # type: ignore[name-defined]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Leakage-Free Chronological Split + Historical Corridor Speed Priors + `GBTRegressor` Training

# COMMAND ----------

# Cap training sample at 80k rows for crisp execution on Databricks Free/Serverless
modeling_df = spark.table(eta_feature_tbl).orderBy("pickup_ts").limit(80_000)  # type: ignore[name-defined]
train_raw, test_raw = chronological_train_test_split(modeling_df, "pickup_ts", train_ratio=0.80)

# Compute H3 Origin-Destination corridor speed strictly on TRAIN set to prevent target leakage!
corridor_priors = (
    train_raw.groupBy("pickup_h3_r7", "dropoff_h3_r7")
    .agg(
        F.avg(F.col("trip_miles") / (F.col("trip_duration_sec") / 3600.0)).alias("corridor_hist_speed_mph"),
        F.count("*").alias("_cnt"),
    )
    .filter("_cnt >= 3")
    .drop("_cnt")
)
global_avg_speed = (
    train_raw.select(F.avg(F.col("trip_miles") / (F.col("trip_duration_sec") / 3600.0)).alias("s"))
    .collect()[0]["s"]
    or 18.0
)


def attach_corridor_features(df_part):
    return (
        df_part.join(corridor_priors, on=["pickup_h3_r7", "dropoff_h3_r7"], how="left")
        .withColumn(
            "corridor_hist_speed_mph",
            F.coalesce(F.col("corridor_hist_speed_mph"), F.lit(float(global_avg_speed))),
        )
        .withColumn(
            "physics_baseline_eta_sec",
            (F.col("trip_miles") / F.greatest(F.col("corridor_hist_speed_mph"), F.lit(5.0))) * F.lit(3600.0),
        )
    )


train_df = attach_corridor_features(train_raw)
test_df = attach_corridor_features(test_raw)

cat_cols = ["pu_borough", "do_borough", "product_type"]
indexers = [
    StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in cat_cols
]
encoder = OneHotEncoder(
    inputCols=[f"{c}_idx" for c in cat_cols],
    outputCols=[f"{c}_ohe" for c in cat_cols],
)

numeric_cols = [
    "trip_miles",
    "haversine_miles",
    "route_circuity_ratio",
    "corridor_hist_speed_mph",
    "physics_baseline_eta_sec",
    "same_borough_num",
    "is_airport_num",
    "is_shared_num",
    "surge_multiplier",
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

assembler = VectorAssembler(
    inputCols=numeric_cols + [f"{c}_ohe" for c in cat_cols],
    outputCol="features",
    handleInvalid="keep",
)

gbt = GBTRegressor(
    featuresCol="features",
    labelCol="trip_duration_sec",
    predictionCol="predicted_eta_sec",
    maxIter=35,
    maxDepth=6,
    stepSize=0.1,
    seed=42,
)

eta_pipeline = Pipeline(stages=[*indexers, encoder, assembler, gbt])

if mlflow is not None:
    with mlflow.start_run(run_name="deepeta_gbt_regressor"):
        mlflow.log_params({"maxIter": 35, "maxDepth": 6, "stepSize": 0.1, "train_ratio": 0.80})
        eta_model = eta_pipeline.fit(train_df)
        preds_df = eta_model.transform(test_df)
        metrics = evaluate_regression_predictions(
            preds_df, label_col="trip_duration_sec", prediction_col="predicted_eta_sec"
        )
        mlflow.log_metrics(metrics)
else:
    eta_model = eta_pipeline.fit(train_df)
    preds_df = eta_model.transform(test_df)
    metrics = evaluate_regression_predictions(
        preds_df, label_col="trip_duration_sec", prediction_col="predicted_eta_sec"
    )

print("=" * 68)
print("DEEP-ETA MODEL EVALUATION ON OUT-OF-TIME TEST SET")
print("=" * 68)
for k, v in metrics.items():
    print(f"  {k:<15}: {v}")
print("=" * 68)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Materialize Batch Predictions to `gold.ml_eta_predictions`

# COMMAND ----------

eta_pred_tbl = cfg.table_fqn("gold", "ml_eta_predictions")
scored_eta_out = (
    preds_df.withColumn("predicted_eta_sec", F.round(F.greatest(F.lit(30.0), F.col("predicted_eta_sec")), 1))
    .withColumn("eta_error_sec", F.round(F.col("predicted_eta_sec") - F.col("trip_duration_sec"), 1))
    .withColumn("abs_error_min", F.round(F.abs(F.col("eta_error_sec")) / 60.0, 2))
    .withColumn("scored_at_ts", F.current_timestamp())
    .select(
        "trip_id",
        "pickup_ts",
        "pu_borough",
        "do_borough",
        "trip_miles",
        "trip_duration_sec",
        "predicted_eta_sec",
        "eta_error_sec",
        "abs_error_min",
        "scored_at_ts",
    )
)

scored_eta_out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    eta_pred_tbl
)
spark.table(eta_pred_tbl).show(10, truncate=False)  # type: ignore[name-defined]
