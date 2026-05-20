import math
import os
import re
from pathlib import Path

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.fs as fs
from mlflow.models import infer_signature
from sklearn.compose import ColumnTransformer
from sklearn.metrics import mean_absolute_error
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

TRAIN_SPLIT_FRACTION = float(os.getenv("TRAIN_SPLIT_FRACTION", "0.8"))
MAX_TRAINING_ROWS = int(os.getenv("MAX_TRAINING_ROWS", "500000"))
MIN_HISTORY_HOURS = int(os.getenv("MIN_HISTORY_HOURS", "168"))

CATEGORICAL_FEATURES = ["pizza_id", "pizza_size", "pizza_category"]
NUMERIC_FEATURES = [
    "hour",
    "day_of_week",
    "day_of_month",
    "month",
    "is_weekend",
    "lag_1h",
    "lag_24h",
    "lag_168h",
    "rolling_mean_24h",
    "rolling_sum_24h",
    "rolling_mean_168h",
    "rolling_sum_168h",
]
FEATURE_COLUMNS = NUMERIC_FEATURES + CATEGORICAL_FEATURES
TARGET_COLUMN = "target_quantity"


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
    return {
        "validation_rmse": rmse,
        "validation_mae": mae,
        "validation_wmape": wmape,
    }


def prepare_training_frame() -> pd.DataFrame:
    require_runtime_config()
    dataset = ds.dataset(
        to_arrow_s3_path(GOLD_FEATURES_PATH),
        filesystem=minio_filesystem(),
        format="parquet",
        partitioning="hive",
    )

    table = dataset.to_table(columns=["order_hour", TARGET_COLUMN, *FEATURE_COLUMNS])
    df = table.to_pandas()
    if df.empty:
        raise RuntimeError(f"No feature rows found in {GOLD_FEATURES_PATH}. Run feature engineering first.")

    df = df.dropna(subset=["order_hour", TARGET_COLUMN])
    df["order_hour"] = pd.to_datetime(df["order_hour"])
    min_hour = df["order_hour"].min()
    df = df[df["order_hour"] >= min_hour + pd.Timedelta(hours=MIN_HISTORY_HOURS)]

    for column in CATEGORICAL_FEATURES:
        df[column] = df[column].fillna("unknown").astype(str)
    for column in NUMERIC_FEATURES:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
    df[TARGET_COLUMN] = pd.to_numeric(df[TARGET_COLUMN], errors="coerce").fillna(0.0)

    df = df.sort_values(["order_hour", "pizza_id"]).reset_index(drop=True)
    if MAX_TRAINING_ROWS > 0 and len(df) > MAX_TRAINING_ROWS:
        df = df.tail(MAX_TRAINING_ROWS).reset_index(drop=True)

    if len(df) < 100:
        raise RuntimeError(f"Not enough rows to train a demand model: {len(df)}")

    return df


def log_validation_predictions(test_df: pd.DataFrame, predictions: np.ndarray) -> None:
    output = test_df[["order_hour", "pizza_id", TARGET_COLUMN]].copy()
    output["prediction"] = np.maximum(np.asarray(predictions, dtype=float), 0.0)
    output["absolute_error"] = (output[TARGET_COLUMN] - output["prediction"]).abs()

    path = Path(f"/tmp/{MODEL_FLAVOR}_validation_predictions.csv")
    output.to_csv(path, index=False)
    mlflow.log_artifact(str(path), artifact_path="validation")


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


def main() -> None:
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    df = prepare_training_frame()
    split_idx = int(len(df) * TRAIN_SPLIT_FRACTION)
    split_idx = min(max(split_idx, 1), len(df) - 1)

    train_df = df.iloc[:split_idx]
    test_df = df.iloc[split_idx:]
    x_train = train_df[FEATURE_COLUMNS]
    y_train = train_df[TARGET_COLUMN]
    x_test = test_df[FEATURE_COLUMNS]
    y_test = test_df[TARGET_COLUMN]

    model = build_pipeline()
    model.fit(x_train, y_train)
    prediction = model.predict(x_test)
    metrics = regression_metrics(y_test, prediction)

    with mlflow.start_run(run_name=f"{MODEL_FLAVOR}-{RUN_TAG}") as run:
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
                "feature_rows": len(df),
                "train_split_fraction": TRAIN_SPLIT_FRACTION,
                "min_history_hours": MIN_HISTORY_HOURS,
                "max_training_rows": MAX_TRAINING_ROWS,
                "regressor": model.named_steps["model"].__class__.__name__,
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
        log_feature_importance(model)

        print("Candidate training completed.")
        print(f"run_id={run.info.run_id}")
        print(f"candidate_model={MODEL_FLAVOR}")
        print(f"metrics={metrics}")


if __name__ == "__main__":
    main()
