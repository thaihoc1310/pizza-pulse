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
- `services/pizza_backend/`: FastAPI backend for pizza catalog and online order ingestion
- `sql/`: Database schema
- `dataset/`: Local sample data
- `scripts/`: Local helper scripts

## Batch MLOps Pipeline

The `pizza_batch_mlops` DAG runs four training stages:

1. `etl_orders`: PostgreSQL orders to silver lakehouse tables.
2. `feature_engineering`: hourly pizza demand features with lag and rolling windows.
3. `train_lightgbm` and `train_catboost`: run as lightweight Kubernetes pods, train candidate boosting models in parallel, and log metrics/artifacts to MLflow.
4. `compare_and_register_model`: runs as a lightweight Kubernetes pod, selects the best candidate by validation RMSE, registers it in MLflow Model Registry, and updates the `champion` alias when it beats the current champion.

Batch training does not write prediction tables to PostgreSQL. Validation predictions are logged as MLflow artifacts for metrics/debugging; online prediction and dashboard serving can consume `models:/pizza_hourly_demand@champion` later.

Build and push the shared batch image after editing any batch job:

```bash
docker build -t manhhung1685/ppbatch-pipeline:0.0.1 jobs/batch_pipeline
docker push manhhung1685/ppbatch-pipeline:0.0.1
```

If you use a different registry or tag, update the image in `airflow/dags/pizza_batch_mlops.py` and the matching `spark-apps/pizza-batch-*.yaml` image fields.

## Online Backend

`services/pizza_backend/` provides the first online backend component:

- `GET /pizzas` lists the pizza catalog from PostgreSQL.
- `POST /orders` accepts an order with one or more line items.
- The service publishes normalized JSON to Kafka topic `pp.order.events`.
- PostgreSQL writes to `orders`, `order_items`, and ingredient stock are available but disabled by default with `POSTGRES_WRITE_ENABLED=false`.
- `scripts/pizza-backend-client.py` can list pizzas, publish a JSON order, or generate demo orders interactively through the backend API.

Build the image:

```bash
docker build -t thaihoc285/pp-backend:0.0.1 services/pizza_backend
docker push thaihoc285/pp-backend:0.0.1
```

Deploy it:

```bash
kubectl apply -f helm-value/backend.yaml -n pizza-pulse
```

See [services/pizza_backend/README.md](services/pizza_backend/README.md) for payload examples and local commands.
