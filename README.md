# Pizza Pulse

Pizza Pulse is a Kubernetes-based data platform project for pizza sales analytics and ML workflows.

## Setup

Start with the Helm setup guide:

[Open the Helm setup README](helm-value/README.md)

That guide covers the required Helm repositories, namespace creation, PostgreSQL, MinIO, Spark Operator, Kafka, Kafka UI, MLflow, Airflow, port-forwarding, and cleanup.

## Repository Layout

- `helm-value/`: Helm values and Kubernetes manifests
- `airflow/dags/`: Airflow DAGs
- `spark-apps/`: SparkApplication manifests
- `jobs/`: Spark job code and Dockerfiles
- `sql/`: Database schema
- `dataset/`: Local sample data
- `scripts/`: Local helper scripts
