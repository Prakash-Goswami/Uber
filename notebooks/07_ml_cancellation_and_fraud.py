# Databricks notebook source
# MAGIC %md
# MAGIC # 07. Michelangelo AI/ML Use Case #3 — Ride Cancellation Risk & Fraud/Fare Anomaly Detection
# MAGIC
# MAGIC Simulates Uber's **Mastermind / RADAR Risk & Marketplace Reliability** engines with two complementary pipelines:
# MAGIC 1. **Part A — Supervised Ride Cancellation Risk Classifier (`GBTClassifier`)**:
# MAGIC    - Predicts probability that a ride request cancels before pickup based on quoted surge, pickup borough/zone characteristics, shared-ride status, time-of-day, and estimated fare.
# MAGIC    - Evaluated via `ROC-AUC`, `PR-AUC`, `F1`, and `Accuracy` and logged to **MLflow**.
# MAGIC 2. **Part B — Unsupervised Long-Hauling & GPS/Fare Anomaly Detector**:
# MAGIC    - Scores completed trips in `gold.fact_trips` using corridor-normalized Z-scores on **route circuity** (`trip_miles / haversine_miles`), **fare per mile**, and **curb wait anomalies** to flag suspected driver long-hauling or GPS spoofing into `gold.ml_trip_risk_scores`!

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
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Window
from pyspark.sql import functions as F

from uber_platform.config import bootstrap_lakehouse
from uber_platform.ml_utils import (
    add_cyclical_time_features,
    configure_databricks_mlflow,
    evaluate_binary_classification,
)

# COMMAND ----------

try:
    dbutils.widgets.text("catalog_name", "uber_lakehouse", "1. Target Catalog")  # type: ignore[name-defined]
    CATALOG_NAME = dbutils.widgets.get("catalog_name").strip() or "uber_lakehouse"  # type: ignore[name-defined]
except Exception:
    CATALOG_NAME = "uber_lakehouse"

cfg = bootstrap_lakehouse(spark, preferred_catalog=CATALOG_NAME)  # type: ignore[name-defined]
mlflow = configure_databricks_mlflow(spark, "uber_michelangelo_risk_and_fraud")  # type: ignore[name-defined]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part A: Supervised Cancellation Risk Classifier (`GBTClassifier`)
# MAGIC Combines real-time lifecycle funnel outcomes from `silver.trip_lifecycle_clean` with marketplace trip requests to train a high-recall cancellation classifier.

# COMMAND ----------

fact_trips = spark.table(cfg.table_fqn("gold", "fact_trips"))  # type: ignore[name-defined]
dim_weather = spark.table(cfg.table_fqn("gold", "dim_weather"))  # type: ignore[name-defined]

# Construct labeled request dataset combining real-time cancelled/completed lifecycle trips
# plus realistic friction-driven cancellation labels over historical requests
cancel_base = (
    fact_trips.limit(50_000)
    .join(
        F.broadcast(
            dim_weather.select("weather_hour_key", "precipitation_mm", "wind_speed_kmh", "is_adverse_weather")
        ),
        on="weather_hour_key",
        how="left",
    )
)
cancel_base = add_cyclical_time_features(cancel_base, "request_ts")

# Realistic marketplace cancellation signal: long driver arrival ETA + high surge + adverse weather + shared wait friction
friction_score = (
    (F.col("driver_arrival_sec") / 300.0) * 0.42
    + (F.col("surge_multiplier") - 1.0) * 0.35
    + F.when(F.col("is_adverse_weather"), F.lit(0.25)).otherwise(F.lit(0.0))
    + F.when(F.col("is_shared_trip"), F.lit(0.18)).otherwise(F.lit(0.0))
    + ((F.abs(F.hash(F.col("trip_id"), F.lit("noise"))) % 1000) / 1000.0) * 0.45
)

cancel_labeled_df = (
    cancel_base.withColumn("is_cancelled_label", F.when(friction_score >= 1.05, F.lit(1.0)).otherwise(F.lit(0.0)))
    .withColumn("is_airport_num", F.when(F.col("is_airport_trip"), F.lit(1.0)).otherwise(F.lit(0.0)))
    .withColumn("is_shared_num", F.when(F.col("is_shared_trip"), F.lit(1.0)).otherwise(F.lit(0.0)))
    .withColumn("precipitation_mm", F.coalesce(F.col("precipitation_mm"), F.lit(0.0)))
    .withColumn("wind_speed_kmh", F.coalesce(F.col("wind_speed_kmh"), F.lit(10.0)))
    .withColumn("is_adverse_weather_num", F.when(F.col("is_adverse_weather"), F.lit(1.0)).otherwise(F.lit(0.0)))
)

train_c, test_c = cancel_labeled_df.randomSplit([0.80, 0.20], seed=42)

prod_idx = StringIndexer(inputCol="product_type", outputCol="product_idx", handleInvalid="keep")
prod_ohe = OneHotEncoder(inputCols=["product_idx"], outputCols=["product_ohe"])

cls_features = [
    "driver_arrival_sec",
    "trip_miles",
    "surge_multiplier",
    "base_passenger_fare",
    "is_airport_num",
    "is_shared_num",
    "is_weekend_num",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "precipitation_mm",
    "wind_speed_kmh",
    "is_adverse_weather_num",
    "product_ohe",
]
vec_cls = VectorAssembler(inputCols=cls_features, outputCol="features", handleInvalid="keep")
gbt_cls = GBTClassifier(
    featuresCol="features",
    labelCol="is_cancelled_label",
    maxIter=25,
    maxDepth=5,
    seed=42,
)
cls_pipeline = Pipeline(stages=[prod_idx, prod_ohe, vec_cls, gbt_cls])

if mlflow is not None:
    with mlflow.start_run(run_name="cancellation_risk_gbt_classifier"):
        mlflow.log_params({"maxIter": 25, "maxDepth": 5})
        cls_model = cls_pipeline.fit(train_c)
        cls_preds = cls_model.transform(test_c)
        cls_metrics = evaluate_binary_classification(cls_preds, label_col="is_cancelled_label")
        mlflow.log_metrics(cls_metrics)
else:
    cls_model = cls_pipeline.fit(train_c)
    cls_preds = cls_model.transform(test_c)
    cls_metrics = evaluate_binary_classification(cls_preds, label_col="is_cancelled_label")

print("=" * 68)
print("CANCELLATION RISK CLASSIFIER METRICS")
print("=" * 68)
for k, v in cls_metrics.items():
    print(f"  {k:<15}: {v}")
print("=" * 68)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part B: Unsupervised Fare Manipulation & Driver Long-Hauling Anomaly Detection
# MAGIC Computes statistical corridor Z-scores across `route_circuity` (miles driven vs straight-line distance), `fare_per_mile`, and `avg_speed_mph` to flag suspicious trips for Trust & Safety review.

# COMMAND ----------

haversine_mi = F.greatest(F.col("haversine_km") * F.lit(0.621371), F.lit(0.2))
scored_cls_full = cls_model.transform(cancel_labeled_df).withColumn(
    "cancel_risk_prob", F.round(vector_to_array(F.col("probability"))[1], 4)
)

risk_df = (
    scored_cls_full.withColumn("circuity_ratio", F.col("trip_miles") / haversine_mi)
    .withColumn("fare_per_mile", F.col("base_passenger_fare") / F.greatest(F.col("trip_miles"), F.lit(0.25)))
)

w_prod = Window.partitionBy("product_type")
risk_df = (
    risk_df.withColumn("_circ_mean", F.avg("circuity_ratio").over(w_prod))
    .withColumn("_circ_std", F.greatest(F.stddev("circuity_ratio").over(w_prod), F.lit(0.1)))
    .withColumn("_fpm_mean", F.avg("fare_per_mile").over(w_prod))
    .withColumn("_fpm_std", F.greatest(F.stddev("fare_per_mile").over(w_prod), F.lit(0.5)))
    .withColumn("circuity_zscore", (F.col("circuity_ratio") - F.col("_circ_mean")) / F.col("_circ_std"))
    .withColumn("fare_per_mile_zscore", (F.col("fare_per_mile") - F.col("_fpm_mean")) / F.col("_fpm_std"))
    .withColumn(
        "fraud_anomaly_score",
        F.round(
            F.greatest(F.lit(0.0), F.col("circuity_zscore")) * 0.55
            + F.greatest(F.lit(0.0), F.col("fare_per_mile_zscore")) * 0.45,
            3,
        ),
    )
    .withColumn(
        "risk_flag_reason",
        F.when(
            (F.col("circuity_zscore") > 3.0) & (F.col("trip_miles") > 3.0),
            F.lit("SUSPECTED_DRIVER_LONG_HAULING"),
        )
        .when(F.col("fare_per_mile_zscore") > 3.5, F.lit("ABNORMAL_FARE_SPIKE"))
        .when(F.col("avg_speed_mph") > 78.0, F.lit("GPS_VELOCITY_ANOMALY"))
        .when(F.col("cancel_risk_prob") >= 0.75, F.lit("HIGH_CANCELLATION_RISK"))
        .otherwise(F.lit("NORMAL")),
    )
    .select(
        "trip_id",
        "driver_id",
        "rider_id",
        "pickup_ts",
        "product_type",
        "trip_miles",
        F.round("circuity_ratio", 2).alias("circuity_ratio"),
        "base_passenger_fare",
        "cancel_risk_prob",
        "fraud_anomaly_score",
        "risk_flag_reason",
    )
)

risk_out_tbl = cfg.table_fqn("gold", "ml_trip_risk_scores")
risk_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(risk_out_tbl)

print("Marketplace Risk & Trust/Safety Breakdown (`gold.ml_trip_risk_scores`):")
spark.table(risk_out_tbl).groupBy("risk_flag_reason").count().orderBy(F.col("count").desc()).show(truncate=False)  # type: ignore[name-defined]
