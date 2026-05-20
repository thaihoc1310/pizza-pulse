from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.task_group import TaskGroup


APP_NAME = "spark-pi-{{ ts_nodash | lower }}"

with DAG(
    dag_id="test_spark_operator",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pizza-pulse", "spark"],
    template_searchpath=["/opt/airflow/dags/repo/spark-apps"],
):
    submit = SparkKubernetesOperator(
        task_id="submit",
        namespace="pizza-pulse",
        application_file="spark-pi.yaml",
        kubernetes_conn_id="kubernetes_default",

        do_xcom_push=False,
        random_name_suffix=False,
        get_logs=True,
        delete_on_termination=False,
        reattach_on_restart=False,
    )

