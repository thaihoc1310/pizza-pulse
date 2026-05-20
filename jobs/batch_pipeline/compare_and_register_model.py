import os
from typing import List, Optional, Tuple

import mlflow
from mlflow.entities import Run
from mlflow.tracking import MlflowClient


MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://pp-mlflow:80")
EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "pizza-pulse-batch")
MODEL_NAME = os.getenv("MODEL_NAME", "pizza_hourly_demand")
MODEL_ALIAS = os.getenv("MODEL_ALIAS", "champion")
BATCH_RUN_TAG = os.getenv("BATCH_RUN_TAG", "manual")
SELECTION_METRIC = os.getenv("MODEL_SELECTION_METRIC", "validation_rmse")
MIN_MODEL_IMPROVEMENT = float(os.getenv("MIN_MODEL_IMPROVEMENT", "0.0"))


def metric_value(run: Run, metric_name: str) -> float:
    value = run.data.metrics.get(metric_name)
    if value is None:
        raise RuntimeError(f"Run {run.info.run_id} does not have metric {metric_name}")
    return float(value)


def find_candidate_runs(client: MlflowClient, experiment_id: str) -> List[Run]:
    filter_string = (
        "attributes.status = 'FINISHED' "
        "and tags.pipeline_step = 'candidate_train' "
        f"and tags.batch_run_tag = '{BATCH_RUN_TAG}' "
        f"and params.model_name = '{MODEL_NAME}'"
    )
    runs = client.search_runs(
        experiment_ids=[experiment_id],
        filter_string=filter_string,
        order_by=[f"metrics.{SELECTION_METRIC} ASC"],
        max_results=50,
    )
    return [run for run in runs if SELECTION_METRIC in run.data.metrics]


def current_champion_metric(client: MlflowClient) -> Tuple[Optional[str], Optional[float]]:
    try:
        version = client.get_model_version_by_alias(MODEL_NAME, MODEL_ALIAS)
    except Exception as exc:
        print(f"No current champion alias found for {MODEL_NAME}: {exc}")
        return None, None

    value = version.tags.get(SELECTION_METRIC)
    if value is None:
        print(f"Champion version {version.version} has no {SELECTION_METRIC} tag; new candidate can promote.")
        return str(version.version), None

    return str(version.version), float(value)


def should_promote(candidate_metric: float, champion_metric: Optional[float]) -> bool:
    if champion_metric is None:
        return True
    return candidate_metric <= champion_metric * (1.0 - MIN_MODEL_IMPROVEMENT)


def set_version_tags(client: MlflowClient, version: str, best_run: Run, promoted: bool) -> None:
    tags = {
        "batch_run_tag": BATCH_RUN_TAG,
        "source_run_id": best_run.info.run_id,
        "candidate_model": best_run.data.tags.get("candidate_model", "unknown"),
        SELECTION_METRIC: str(metric_value(best_run, SELECTION_METRIC)),
        "validation_mae": str(best_run.data.metrics.get("validation_mae", "")),
        "validation_wmape": str(best_run.data.metrics.get("validation_wmape", "")),
        "promoted": str(promoted).lower(),
    }
    for key, value in tags.items():
        client.set_model_version_tag(MODEL_NAME, version, key, value)


def main() -> None:
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)

    experiment = client.get_experiment_by_name(EXPERIMENT_NAME)
    if experiment is None:
        raise RuntimeError(f"MLflow experiment not found: {EXPERIMENT_NAME}")

    candidates = find_candidate_runs(client, experiment.experiment_id)
    if not candidates:
        raise RuntimeError(
            f"No candidate training runs found for batch_run_tag={BATCH_RUN_TAG}, model_name={MODEL_NAME}"
        )

    best_run = min(candidates, key=lambda run: metric_value(run, SELECTION_METRIC))
    best_metric = metric_value(best_run, SELECTION_METRIC)
    best_candidate = best_run.data.tags.get("candidate_model", "unknown")

    champion_version, champion_metric = current_champion_metric(client)
    promote = should_promote(best_metric, champion_metric)

    with mlflow.start_run(run_name=f"compare-register-{BATCH_RUN_TAG}") as run:
        mlflow.set_tags(
            {
                "pipeline_step": "compare_and_register",
                "batch_run_tag": BATCH_RUN_TAG,
                "selected_run_id": best_run.info.run_id,
                "selected_candidate_model": best_candidate,
            }
        )
        mlflow.log_params(
            {
                "model_name": MODEL_NAME,
                "model_alias": MODEL_ALIAS,
                "selection_metric": SELECTION_METRIC,
                "min_model_improvement": MIN_MODEL_IMPROVEMENT,
                "champion_version_before": champion_version or "",
                "promoted": str(promote).lower(),
            }
        )
        mlflow.log_metric(f"selected_{SELECTION_METRIC}", best_metric)
        if champion_metric is not None:
            mlflow.log_metric(f"champion_{SELECTION_METRIC}", champion_metric)

        registered = mlflow.register_model(
            model_uri=f"runs:/{best_run.info.run_id}/model",
            name=MODEL_NAME,
            await_registration_for=300,
        )
        version = str(registered.version)
        set_version_tags(client, version, best_run, promote)

        if promote:
            client.set_registered_model_alias(MODEL_NAME, MODEL_ALIAS, version)

        mlflow.set_tag("registered_model_version", version)

    print("Model comparison and registration completed.")
    print(f"selected_candidate_model={best_candidate}")
    print(f"selected_run_id={best_run.info.run_id}")
    print(f"selected_{SELECTION_METRIC}={best_metric}")
    print(f"champion_version_before={champion_version}")
    print(f"champion_{SELECTION_METRIC}={champion_metric}")
    print(f"registered_version={version}")
    print(f"promoted_to_{MODEL_ALIAS}={promote}")


if __name__ == "__main__":
    main()
