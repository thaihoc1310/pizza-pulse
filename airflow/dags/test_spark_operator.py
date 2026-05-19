from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

APP_NAME = "spark-pi-test"

with DAG(
    dag_id="test_spark_operator",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pizza-pulse", "spark"],
    template_searchpath=["/opt/airflow/dags/repo/airflow/spark-apps"],
):
    submit = SparkKubernetesOperator(
        task_id="submit_spark_pi",
        namespace="pizza-pulse",
        application_file="spark-pi.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
    )

    monitor = SparkKubernetesSensor(
        task_id="monitor_spark_pi",
        namespace="pizza-pulse",
        application_name=APP_NAME,
        kubernetes_conn_id="kubernetes_default",
        attach_log=True,
    )

    submit >> monitor