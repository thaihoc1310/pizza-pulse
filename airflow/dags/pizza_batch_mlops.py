from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.task_group import TaskGroup
from kubernetes.client import models as k8s


NAMESPACE = "pizza-pulse"
KUBERNETES_CONN_ID = "kubernetes_default"
BATCH_IMAGE = "thaihoc285/ppbatch-pipeline:0.0.1"

SPARK_APPLICATIONS = {
    "etl_orders": {
        "application_file": "pizza-batch-etl.yaml",
        "application_name": "pizza-batch-etl-{{ ts_nodash | lower }}",
    },
    "feature_engineering": {
        "application_file": "pizza-batch-features.yaml",
        "application_name": "pizza-batch-features-{{ ts_nodash | lower }}",
    },
}


def spark_application_group(group_id: str, application_file: str, application_name: str) -> TaskGroup:
    with TaskGroup(group_id=group_id) as group:
        submit = SparkKubernetesOperator(
            task_id="submit",
            namespace=NAMESPACE,
            application_file=application_file,
            kubernetes_conn_id=KUBERNETES_CONN_ID,
            do_xcom_push=False,
            random_name_suffix=False,
            get_logs=False,
            delete_on_termination=False,
            reattach_on_restart=False,
        )

        monitor = SparkKubernetesSensor(
            task_id="monitor",
            namespace=NAMESPACE,
            application_name=application_name,
            kubernetes_conn_id=KUBERNETES_CONN_ID,
            attach_log=True,
        )

        submit >> monitor

    return group


def secret_env(name: str, secret_name: str, secret_key: str) -> k8s.V1EnvVar:
    return k8s.V1EnvVar(
        name=name,
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(
                name=secret_name,
                key=secret_key,
            )
        ),
    )


def batch_env_vars(extra_env: list[k8s.V1EnvVar] | None = None) -> list[k8s.V1EnvVar]:
    env = [
        k8s.V1EnvVar(name="MINIO_ENDPOINT", value="http://pp-minio:9000"),
        secret_env("MINIO_ACCESS_KEY", "pp-airflow-minio", "AWS_ACCESS_KEY_ID"),
        secret_env("MINIO_SECRET_KEY", "pp-airflow-minio", "AWS_SECRET_ACCESS_KEY"),
        secret_env("AWS_ACCESS_KEY_ID", "pp-airflow-minio", "AWS_ACCESS_KEY_ID"),
        secret_env("AWS_SECRET_ACCESS_KEY", "pp-airflow-minio", "AWS_SECRET_ACCESS_KEY"),
        k8s.V1EnvVar(name="MLFLOW_TRACKING_URI", value="http://pp-mlflow:80"),
        k8s.V1EnvVar(name="MLFLOW_EXPERIMENT_NAME", value="pizza-pulse-batch"),
        k8s.V1EnvVar(name="MLFLOW_S3_ENDPOINT_URL", value="http://pp-minio:9000"),
        k8s.V1EnvVar(name="MLFLOW_S3_IGNORE_TLS", value="true"),
        k8s.V1EnvVar(name="AWS_DEFAULT_REGION", value="us-east-1"),
        k8s.V1EnvVar(name="LAKEHOUSE_ROOT", value="s3a://pp-lakehouse"),
        k8s.V1EnvVar(name="BATCH_RUN_TAG", value="{{ ts_nodash | lower }}"),
        k8s.V1EnvVar(name="MODEL_NAME", value="pizza_hourly_demand"),
    ]
    if extra_env:
        env.extend(extra_env)
    return env


def python_batch_pod(
    task_id: str,
    pod_name: str,
    script_path: str,
    extra_env: list[k8s.V1EnvVar] | None = None,
    memory_request: str = "1Gi",
    memory_limit: str = "2Gi",
    cpu_request: str = "500m",
    cpu_limit: str = "1",
) -> KubernetesPodOperator:
    return KubernetesPodOperator(
        task_id=task_id,
        name=pod_name,
        namespace=NAMESPACE,
        image=BATCH_IMAGE,
        image_pull_policy="Always",
        cmds=["python3"],
        arguments=[script_path],
        kubernetes_conn_id=KUBERNETES_CONN_ID,
        service_account_name="pp-airflow-scheduler",
        get_logs=True,
        do_xcom_push=False,
        env_vars=batch_env_vars(extra_env),
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": cpu_request, "memory": memory_request},
            limits={"cpu": cpu_limit, "memory": memory_limit},
        ),
    )


with DAG(
    dag_id="pizza_batch_mlops",
    start_date=datetime(2026, 1, 1),
    schedule="None",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["pizza-pulse", "batch", "mlops", "mlflow"],
    template_searchpath=["/opt/airflow/dags/repo/spark-apps"],
):
    etl_orders = spark_application_group("etl_orders", **SPARK_APPLICATIONS["etl_orders"])
    feature_engineering = spark_application_group(
        "feature_engineering",
        **SPARK_APPLICATIONS["feature_engineering"],
    )
    train_lightgbm = python_batch_pod(
        task_id="train_lightgbm",
        pod_name="pizza-train-lightgbm-{{ ts_nodash | lower }}",
        script_path="/opt/spark/jobs/train_candidate_model.py",
        extra_env=[k8s.V1EnvVar(name="MODEL_FLAVOR", value="lightgbm")],
        memory_request="4Gi",
        memory_limit="8Gi",
        cpu_request="2",
        cpu_limit="4",
    )
    train_catboost = python_batch_pod(
        task_id="train_catboost",
        pod_name="pizza-train-catboost-{{ ts_nodash | lower }}",
        script_path="/opt/spark/jobs/train_candidate_model.py",
        extra_env=[k8s.V1EnvVar(name="MODEL_FLAVOR", value="catboost")],
        memory_request="4Gi",
        memory_limit="8Gi",
        cpu_request="2",
        cpu_limit="4",
    )

    compare_and_register_model = python_batch_pod(
        task_id="compare_and_register_model",
        pod_name="pizza-compare-register-{{ ts_nodash | lower }}",
        script_path="/opt/spark/jobs/compare_and_register_model.py",
        extra_env=[
            k8s.V1EnvVar(name="MODEL_ALIAS", value="champion"),
            k8s.V1EnvVar(name="MODEL_SELECTION_METRIC", value="validation_rmse"),
            k8s.V1EnvVar(name="MIN_MODEL_IMPROVEMENT", value="0.0"),
        ]
    )

    etl_orders >> feature_engineering
    feature_engineering >> train_lightgbm >> compare_and_register_model
    feature_engineering >> train_catboost >> compare_and_register_model
