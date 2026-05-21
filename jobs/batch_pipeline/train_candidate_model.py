from __future__ import annotations

import math
import os
import re
import json
from pathlib import Path

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.fs as fs
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from sklearn.compose import ColumnTransformer
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://pp-minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
LAKEHOUSE_ROOT = os.getenv("LAKEHOUSE_ROOT", "s3a://pp-lakehouse")
RUN_TAG = re.sub(r"[^a-zA-Z0-9_.=-]+", "-", os.getenv("BATCH_RUN_TAG", "manual")).strip("-")
GOLD_FEATURES_PATH = "/".join([LAKEHOUSE_ROOT.rstrip("/"), "gold", "demand_features"])

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://pp-mlflow:80")
EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "pizza-pulse-batch")
MODEL_NAME = os.getenv("MODEL_NAME", "pizza_hourly_demand")
MODEL_FLAVOR = os.getenv("MODEL_FLAVOR", "lightgbm").lower()

TRAIN_SPLIT_FRACTION = float(os.getenv("TRAIN_SPLIT_FRACTION", "0.7"))
MAX_TRAINING_ROWS = int(os.getenv("MAX_TRAINING_ROWS", "500000"))
MIN_HISTORY_HOURS = int(os.getenv("MIN_HISTORY_HOURS", "168"))

CATEGORICAL_FEATURES = ["pizza_name", "pizza_size", "pizza_type"]
NUMERIC_FEATURES = [
    "unit_price",
    "ingredient_count",
    "hour_of_day",
    "day_of_week",
    "month",
    "is_weekend",
    "is_lunch_time",
    "is_dinner_time",
    "qty_last_15m",
    "qty_last_30m",
    "qty_last_1h",
    "revenue_last_1h",
    "order_count_last_1h",
    "qty_prev_15m",
    "qty_prev_1h",
    "growth_15m_vs_prev_15m",
    "growth_1h_vs_prev_1h",
    "qty_lag_1h",
    "qty_lag_24h",
    "avg_qty_same_hour_last_7d",
    "store_total_qty_last_15m",
    "store_total_qty_last_1h",
    "store_order_count_last_1h",
]
FEATURE_COLUMNS = NUMERIC_FEATURES + CATEGORICAL_FEATURES
TARGET_COLUMN = "target_quantity_next_1h"
TIME_COLUMN = "feature_time"


def require_runtime_config() -> None:
    missing = []
    if not MINIO_ACCESS_KEY:
        missing.append("MINIO_ACCESS_KEY or AWS_ACCESS_KEY_ID")
    if not MINIO_SECRET_KEY:
        missing.append("MINIO_SECRET_KEY or AWS_SECRET_ACCESS_KEY")
    if missing:
        raise RuntimeError(f"Missing required runtime config: {', '.join(missing)}")


def to_arrow_s3_path(path: str) -> str:
    for prefix in ("s3a://", "s3://"):
        if path.startswith(prefix):
            return path[len(prefix):]
    return path


def minio_filesystem() -> fs.S3FileSystem:
    endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    scheme = "https" if MINIO_ENDPOINT.startswith("https://") else "http"
    return fs.S3FileSystem(
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        endpoint_override=endpoint,
        scheme=scheme,
        region=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


def one_hot_encoder() -> OneHotEncoder:
    try:
        return OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    except TypeError:
        return OneHotEncoder(handle_unknown="ignore", sparse=False)


def build_regressor():
    if MODEL_FLAVOR == "lightgbm":
        from lightgbm import LGBMRegressor

        return LGBMRegressor(
            objective="regression",
            n_estimators=int(os.getenv("LGBM_N_ESTIMATORS", "400")),
            learning_rate=float(os.getenv("LGBM_LEARNING_RATE", "0.05")),
            num_leaves=int(os.getenv("LGBM_NUM_LEAVES", "31")),
            subsample=float(os.getenv("LGBM_SUBSAMPLE", "0.9")),
            colsample_bytree=float(os.getenv("LGBM_COLSAMPLE_BYTREE", "0.9")),
            random_state=int(os.getenv("MODEL_RANDOM_STATE", "42")),
            n_jobs=int(os.getenv("MODEL_N_JOBS", "-1")),
        )

    if MODEL_FLAVOR == "catboost":
        from catboost import CatBoostRegressor

        return CatBoostRegressor(
            iterations=int(os.getenv("CATBOOST_ITERATIONS", "500")),
            learning_rate=float(os.getenv("CATBOOST_LEARNING_RATE", "0.05")),
            depth=int(os.getenv("CATBOOST_DEPTH", "8")),
            loss_function="RMSE",
            random_seed=int(os.getenv("MODEL_RANDOM_STATE", "42")),
            verbose=False,
        )

    raise RuntimeError(f"Unsupported MODEL_FLAVOR={MODEL_FLAVOR}. Use lightgbm or catboost.")


def build_pipeline() -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[
            ("categorical", one_hot_encoder(), CATEGORICAL_FEATURES),
            ("numeric", "passthrough", NUMERIC_FEATURES),
        ],
        remainder="drop",
    )
    return Pipeline(
        steps=[
            ("preprocess", preprocessor),
            ("model", build_regressor()),
        ]
    )


def regression_metrics(y_true, y_pred) -> dict:
    clipped_pred = np.maximum(np.asarray(y_pred, dtype=float), 0.0)
    actual = np.asarray(y_true, dtype=float)
    error = actual - clipped_pred
    rmse = float(math.sqrt(np.mean(np.square(error))))
    mae = float(mean_absolute_error(actual, clipped_pred))
    wmape = float(np.sum(np.abs(error)) / max(np.sum(np.abs(actual)), 1.0))
    non_zero = np.abs(actual) > 1e-9
    mape = float(np.mean(np.abs(error[non_zero] / actual[non_zero]))) if np.any(non_zero) else 0.0
    r2 = float(r2_score(actual, clipped_pred)) if len(actual) > 1 else 0.0
    return {
        "validation_rmse": rmse,
        "validation_mae": mae,
        "validation_wmape": wmape,
        "validation_mape": mape,
        "validation_r2": r2,
    }


def prepare_training_frame() -> pd.DataFrame:
    require_runtime_config()
    dataset = ds.dataset(
        to_arrow_s3_path(GOLD_FEATURES_PATH),
        filesystem=minio_filesystem(),
        format="parquet",
        partitioning="hive",
    )

    table = dataset.to_table(columns=[TIME_COLUMN, TARGET_COLUMN, *FEATURE_COLUMNS])
    df = table.to_pandas()
    if df.empty:
        raise RuntimeError(f"No feature rows found in {GOLD_FEATURES_PATH}. Run feature engineering first.")

    df = df.dropna(subset=[TIME_COLUMN, TARGET_COLUMN])
    df[TIME_COLUMN] = pd.to_datetime(df[TIME_COLUMN])
    min_time = df[TIME_COLUMN].min()
    df = df[df[TIME_COLUMN] >= min_time + pd.Timedelta(hours=MIN_HISTORY_HOURS)]

    for column in CATEGORICAL_FEATURES:
        df[column] = df[column].fillna("unknown").astype(str)
    for column in NUMERIC_FEATURES:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
    df[TARGET_COLUMN] = pd.to_numeric(df[TARGET_COLUMN], errors="coerce").fillna(0.0)

    df = df.sort_values([TIME_COLUMN, "pizza_name", "pizza_size"]).reset_index(drop=True)
    if MAX_TRAINING_ROWS > 0 and len(df) > MAX_TRAINING_ROWS:
        df = df.tail(MAX_TRAINING_ROWS).reset_index(drop=True)

    if len(df) < 100:
        raise RuntimeError(f"Not enough rows to train a demand model: {len(df)}")

    return df


def validate_train_split_fraction() -> None:
    if not 0.0 < TRAIN_SPLIT_FRACTION < 1.0:
        raise RuntimeError(f"TRAIN_SPLIT_FRACTION must be between 0 and 1, got {TRAIN_SPLIT_FRACTION}")


def split_training_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    validate_train_split_fraction()

    feature_times = pd.Series(df[TIME_COLUMN].dropna().unique()).sort_values(ignore_index=True)
    if len(feature_times) < 2:
        raise RuntimeError("Need at least two feature_time values for chronological train/holdout split.")

    split_idx = int(len(feature_times) * TRAIN_SPLIT_FRACTION)
    split_idx = min(max(split_idx, 1), len(feature_times) - 1)
    holdout_start_time = feature_times.iloc[split_idx]

    train_df = df[df[TIME_COLUMN] < holdout_start_time].copy()
    holdout_df = df[df[TIME_COLUMN] >= holdout_start_time].copy()
    if len(train_df) < 1 or len(holdout_df) < 1:
        raise RuntimeError(
            "Chronological 70/30 split produced an empty train or holdout set: "
            f"train_rows={len(train_df)}, holdout_rows={len(holdout_df)}"
        )

    return train_df, holdout_df, f"chronological_{TRAIN_SPLIT_FRACTION:.2f}_{1.0 - TRAIN_SPLIT_FRACTION:.2f}"


def split_summary(train_df: pd.DataFrame, holdout_df: pd.DataFrame, feature_rows: int, split_strategy: str) -> dict:
    return {
        "split_strategy": split_strategy,
        "train_split_fraction_config": TRAIN_SPLIT_FRACTION,
        "holdout_split_fraction_config": 1.0 - TRAIN_SPLIT_FRACTION,
        "feature_rows": int(feature_rows),
        "train_rows": int(len(train_df)),
        "holdout_rows": int(len(holdout_df)),
        "actual_train_fraction": float(len(train_df) / feature_rows),
        "actual_holdout_fraction": float(len(holdout_df) / feature_rows),
        "train_time_start": str(train_df[TIME_COLUMN].min()),
        "train_time_end": str(train_df[TIME_COLUMN].max()),
        "holdout_time_start": str(holdout_df[TIME_COLUMN].min()),
        "holdout_time_end": str(holdout_df[TIME_COLUMN].max()),
    }


def log_split_summary(summary: dict) -> None:
    path = Path("/tmp/split_summary.json")
    path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    mlflow.log_artifact(str(path), artifact_path="split")


def log_validation_predictions(test_df: pd.DataFrame, predictions: np.ndarray) -> None:
    output = test_df[[TIME_COLUMN, "pizza_name", "pizza_size", TARGET_COLUMN]].copy()
    output["prediction"] = np.maximum(np.asarray(predictions, dtype=float), 0.0)
    output["absolute_error"] = (output[TARGET_COLUMN] - output["prediction"]).abs()

    path = Path(f"/tmp/{MODEL_FLAVOR}_validation_predictions.csv")
    output.to_csv(path, index=False)
    mlflow.log_artifact(str(path), artifact_path="validation")


def log_feature_schema() -> None:
    artifacts = {
        "feature_columns.json": FEATURE_COLUMNS,
        "categorical_columns.json": CATEGORICAL_FEATURES,
        "feature_contract.json": {
            "granularity": "1 row = pizza_name + pizza_size + 15-minute feature_time",
            "target_column": TARGET_COLUMN,
            "time_column": TIME_COLUMN,
            "leakage_rule": "Features use events at or before feature_time; target uses the next four 15-minute bins.",
            "offline_split_rule": "Train on the earliest 70% of feature_time values; compare on the remaining 30% holdout period.",
            "numeric_features": NUMERIC_FEATURES,
            "categorical_features": CATEGORICAL_FEATURES,
        },
    }
    for filename, payload in artifacts.items():
        path = Path("/tmp") / filename
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        mlflow.log_artifact(str(path), artifact_path="schema")


def log_feature_importance(model: Pipeline) -> None:
    regressor = model.named_steps["model"]
    importances = getattr(regressor, "feature_importances_", None)
    if importances is None:
        print(f"Feature importance is not available for {MODEL_FLAVOR}.")
        return

    try:
        feature_names = model.named_steps["preprocess"].get_feature_names_out()
    except Exception:
        feature_names = [f"feature_{idx}" for idx in range(len(importances))]

    importance = (
        pd.DataFrame({"feature": feature_names, "importance": importances})
        .sort_values("importance", ascending=False)
        .head(200)
    )
    path = Path(f"/tmp/{MODEL_FLAVOR}_feature_importance.csv")
    importance.to_csv(path, index=False)
    mlflow.log_artifact(str(path), artifact_path="diagnostics")


def ensure_experiment() -> str:
    client = MlflowClient()
    experiment = client.get_experiment_by_name(EXPERIMENT_NAME)
    if experiment is not None:
        return experiment.experiment_id

    try:
        return client.create_experiment(EXPERIMENT_NAME)
    except Exception:
        experiment = client.get_experiment_by_name(EXPERIMENT_NAME)
        if experiment is not None:
            return experiment.experiment_id
        raise


def main() -> None:
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    experiment_id = ensure_experiment()

    df = prepare_training_frame()
    train_df, test_df, split_strategy = split_training_frame(df)
    split_info = split_summary(train_df, test_df, len(df), split_strategy)

    x_train = train_df[FEATURE_COLUMNS]
    y_train = train_df[TARGET_COLUMN]
    x_test = test_df[FEATURE_COLUMNS]
    y_test = test_df[TARGET_COLUMN]

    model = build_pipeline()
    model.fit(x_train, y_train)
    prediction = model.predict(x_test)
    metrics = regression_metrics(y_test, prediction)

    with mlflow.start_run(experiment_id=experiment_id, run_name=f"{MODEL_FLAVOR}-{RUN_TAG}") as run:
        mlflow.set_tags(
            {
                "pipeline_step": "candidate_train",
                "batch_run_tag": RUN_TAG,
                "candidate_model": MODEL_FLAVOR,
            }
        )
        mlflow.log_params(
            {
                "model_name": MODEL_NAME,
                "candidate_model": MODEL_FLAVOR,
                "batch_run_tag": RUN_TAG,
                "train_rows": len(train_df),
                "validation_rows": len(test_df),
                "holdout_rows": len(test_df),
                "feature_rows": len(df),
                "split_strategy": split_strategy,
                "train_split_fraction": TRAIN_SPLIT_FRACTION,
                "holdout_split_fraction": 1.0 - TRAIN_SPLIT_FRACTION,
                "actual_train_fraction": split_info["actual_train_fraction"],
                "actual_holdout_fraction": split_info["actual_holdout_fraction"],
                "train_time_start": split_info["train_time_start"],
                "train_time_end": split_info["train_time_end"],
                "holdout_time_start": split_info["holdout_time_start"],
                "holdout_time_end": split_info["holdout_time_end"],
                "min_history_hours": MIN_HISTORY_HOURS,
                "max_training_rows": MAX_TRAINING_ROWS,
                "regressor": model.named_steps["model"].__class__.__name__,
                "target_column": TARGET_COLUMN,
                "feature_granularity": "15_minutes",
            }
        )
        mlflow.log_metrics(metrics)

        signature = infer_signature(x_train.head(20), model.predict(x_train.head(20)))
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            signature=signature,
            input_example=x_train.head(5),
        )
        log_validation_predictions(test_df, prediction)
        log_feature_schema()
        log_split_summary(split_info)
        log_feature_importance(model)

        print("Candidate training completed.")
        print(f"run_id={run.info.run_id}")
        print(f"candidate_model={MODEL_FLAVOR}")
        print(f"split_summary={split_info}")
        print(f"metrics={metrics}")


if __name__ == "__main__":
    main()
