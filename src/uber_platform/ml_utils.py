"""
Michelangelo-style Feature Engineering & MLflow Utilities for PySpark ML.

Supports:
1. Cyclical temporal sin/cos feature encoding (hour-of-day, day-of-week)
2. Leakage-free chronological out-of-time train/test splitting
3. Workspace-aware MLflow experiment initialization on Databricks
4. Standardized Regression & Classification evaluation summaries
"""

from __future__ import annotations

import math
from typing import Any, Dict, Tuple


def add_cyclical_time_features(df: Any, timestamp_col: str) -> Any:
    """Add cyclical sin/cos encodings for hour (0..23) and day_of_week (1..7)."""
    from pyspark.sql import functions as F

    hr = F.hour(F.col(timestamp_col)).cast("double")
    dow = F.dayofweek(F.col(timestamp_col)).cast("double")
    two_pi = 2.0 * math.pi

    return (
        df.withColumn("hour_of_day", hr.cast("int"))
        .withColumn("day_of_week", dow.cast("int"))
        .withColumn("is_weekend_num", F.when(dow.isin(1.0, 7.0), F.lit(1.0)).otherwise(F.lit(0.0)))
        .withColumn("hour_sin", F.sin(hr * F.lit(two_pi / 24.0)))
        .withColumn("hour_cos", F.cos(hr * F.lit(two_pi / 24.0)))
        .withColumn("dow_sin", F.sin(dow * F.lit(two_pi / 7.0)))
        .withColumn("dow_cos", F.cos(dow * F.lit(two_pi / 7.0)))
    )


def chronological_train_test_split(
    df: Any, timestamp_col: str, train_ratio: float = 0.80
) -> Tuple[Any, Any]:
    """
    Split a time-series or marketplace event DataFrame chronologically rather than randomly
    so future traffic/surge regimes never leak into historical training windows.
    """
    from pyspark.sql import functions as F

    df_epoch = df.withColumn("_split_epoch", F.unix_timestamp(F.col(timestamp_col)))
    quantiles = df_epoch.approxQuantile("_split_epoch", [train_ratio], 0.01)
    cutoff = quantiles[0] if quantiles else None

    if cutoff is None:
        return df.randomSplit([train_ratio, 1.0 - train_ratio], seed=42)

    train_df = df_epoch.filter(F.col("_split_epoch") <= F.lit(cutoff)).drop("_split_epoch")
    test_df = df_epoch.filter(F.col("_split_epoch") > F.lit(cutoff)).drop("_split_epoch")
    if test_df.limit(1).count() == 0:
        return df.randomSplit([train_ratio, 1.0 - train_ratio], seed=42)
    return train_df, test_df


def configure_databricks_mlflow(spark: Any, experiment_suffix: str) -> Any:
    """
    Configure MLflow experiment inside current Databricks user's workspace home folder
    (`/Users/<current_user>/<experiment_suffix>`), with graceful local fallback.
    """
    try:
        import mlflow  # type: ignore

        try:
            user = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
            exp_path = f"/Users/{user}/{experiment_suffix}"
            mlflow.set_experiment(exp_path)
        except Exception:
            mlflow.set_experiment(f"/{experiment_suffix}")
        return mlflow
    except ImportError:
        return None


def evaluate_regression_predictions(
    predictions_df: Any, label_col: str, prediction_col: str = "prediction"
) -> Dict[str, float]:
    """Compute RMSE, MAE, R2, and MAPE (%) for regression predictions."""
    from pyspark.ml.evaluation import RegressionEvaluator
    from pyspark.sql import functions as F

    metrics: Dict[str, float] = {}
    for m_name in ("rmse", "mae", "r2"):
        ev = RegressionEvaluator(
            labelCol=label_col, predictionCol=prediction_col, metricName=m_name
        )
        metrics[m_name] = round(float(ev.evaluate(predictions_df)), 4)

    mape_row = (
        predictions_df.select(
            F.avg(
                F.abs(F.col(label_col) - F.col(prediction_col))
                / F.greatest(F.abs(F.col(label_col)), F.lit(1.0))
            ).alias("mape")
        ).collect()[0]
    )
    metrics["mape_pct"] = round(float((mape_row["mape"] or 0.0) * 100.0), 2)
    return metrics


def evaluate_binary_classification(
    predictions_df: Any, label_col: str, raw_pred_col: str = "rawPrediction", pred_col: str = "prediction"
) -> Dict[str, float]:
    """Compute ROC-AUC, PR-AUC, F1, and Accuracy for binary classifiers."""
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

    roc_eval = BinaryClassificationEvaluator(
        labelCol=label_col, rawPredictionCol=raw_pred_col, metricName="areaUnderROC"
    )
    pr_eval = BinaryClassificationEvaluator(
        labelCol=label_col, rawPredictionCol=raw_pred_col, metricName="areaUnderPR"
    )
    f1_eval = MulticlassClassificationEvaluator(
        labelCol=label_col, predictionCol=pred_col, metricName="f1"
    )
    acc_eval = MulticlassClassificationEvaluator(
        labelCol=label_col, predictionCol=pred_col, metricName="accuracy"
    )
    return {
        "roc_auc": round(float(roc_eval.evaluate(predictions_df)), 4),
        "pr_auc": round(float(pr_eval.evaluate(predictions_df)), 4),
        "f1": round(float(f1_eval.evaluate(predictions_df)), 4),
        "accuracy": round(float(acc_eval.evaluate(predictions_df)), 4),
    }
