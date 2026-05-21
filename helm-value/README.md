# Pizza Pulse Helm Setup

This guide installs the Kubernetes services for Pizza Pulse in the `pizza-pulse` namespace:

- PostgreSQL: database for `pizza_serving`, `mlflow`, and `airflow`
- MinIO: object storage for the lakehouse, MLflow artifacts, and Spark checkpoints
- Spark Operator: runs SparkApplication resources from Airflow
- Strimzi Kafka + Kafka topics
- Kafka UI
- MLflow
- Airflow API Server / Scheduler / DAG Processor

Run the commands below from the `helm-value/` directory:

```bash
cd helm-value
```

## 1. Prerequisites

Required tools and cluster setup:

- A running Kubernetes cluster
- `kubectl` configured for the correct context
- Helm 3
- A default StorageClass for PVCs

Quick checks:

```bash
kubectl cluster-info
kubectl get storageclass
helm version
```

## 2. Add Helm Repositories

Add all Helm repositories before installing charts:

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add minio https://charts.min.io/
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo add strimzi https://strimzi.io/charts/
helm repo add kafka-ui https://provectus.github.io/kafka-ui-charts
helm repo add community-charts https://community-charts.github.io/helm-charts
helm repo add apache-airflow https://airflow.apache.org

helm repo update
```

## 3. Create Namespace

```bash
kubectl create namespace pizza-pulse --dry-run=client -o yaml | kubectl apply -f -
```

## 4. Install PostgreSQL

```bash
helm upgrade --install pp-postgre bitnami/postgresql \
  --namespace pizza-pulse \
  -f postgres-values.yaml
```

Wait for PostgreSQL:

```bash
kubectl -n pizza-pulse rollout status statefulset/pp-postgre-postgresql
kubectl -n pizza-pulse get pods,svc | grep pp-postgre
```

Create the additional databases for MLflow and Airflow. `pizza_serving` is created by `postgres-values.yaml`.

```bash
kubectl -n pizza-pulse exec -i pp-postgre-postgresql-0 -- bash -lc 'PGPASSWORD=admin psql -U postgres -d postgres' <<'SQL'
SELECT 'CREATE DATABASE mlflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow')\gexec

SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
SQL
```

## 5. Install MinIO

```bash
helm upgrade --install pp-minio minio/minio \
  --namespace pizza-pulse \
  -f minio-values.yaml
```

Wait for MinIO:

```bash
kubectl -n pizza-pulse rollout status deployment/pp-minio
kubectl -n pizza-pulse get pods,svc | grep pp-minio
```

`minio-values.yaml` creates these buckets:

- `pp-lakehouse`
- `pp-mlflow-artifacts`
- `pp-spark-checkpoints`

The Spark bootstrap job reads data from:

```text
s3a://pp-lakehouse/bronze/raw/pizza_sales.csv
```
So, upload raw data in pp-lakehouse/bronze/raw/

Then create an access key and secret key in the MinIO Console. Use those credentials for MLflow and Airflow.

## 6. Install Spark Operator

The release name must be `pp-spark-operator` so it matches the serviceAccount used in `spark-apps/*.yaml`.

```bash
helm upgrade --install pp-spark-operator spark-operator/spark-operator \
  --namespace pizza-pulse \
  --set "spark.jobNamespaces={pizza-pulse}"
```

Check:

```bash
kubectl -n pizza-pulse get pods,svc | grep spark
kubectl api-resources | grep sparkapplications
```

## 7. Install Strimzi Kafka

Install the Strimzi operator:

```bash
helm upgrade --install pp-strimzi strimzi/strimzi-kafka-operator \
  --namespace pizza-pulse \
  --set watchNamespaces="{pizza-pulse}"
```

Wait for the operator:

```bash
kubectl -n pizza-pulse get pods | grep strimzi
```

Create the Kafka cluster and topics:

```bash
kubectl apply -f Kafka.yaml -n pizza-pulse
kubectl apply -f KafkaTopic.yaml -n pizza-pulse
```

Wait for Kafka:

```bash
kubectl -n pizza-pulse wait kafka/pp-kafka --for=condition=Ready --timeout=10m
kubectl -n pizza-pulse get kafka,kafkatopic
```

Internal Kafka bootstrap service:

```text
pp-kafka-kafka-bootstrap:9092
```

## 8. Install Kafka UI

```bash
helm upgrade --install pp-kafka-ui kafka-ui/kafka-ui \
  --namespace pizza-pulse \
  -f kafka-ui-values.yaml
```

Check:

```bash
kubectl -n pizza-pulse rollout status deployment/pp-kafka-ui
```

## 9. Configure MinIO Credentials For MLflow And Airflow

After creating the access key in the MinIO Console, export it in your shell:

```bash
export MINIO_ACCESS_KEY='<your-minio-access-key>'
export MINIO_SECRET_KEY='<your-minio-secret-key>'
```

Update `mlflow-values.yaml`:

```yaml
artifactRoot:
  s3:
    awsAccessKeyId: "<your-minio-access-key>"
    awsSecretAccessKey: "<your-minio-secret-key>"
```

Create the Airflow secret:

```bash
kubectl create secret generic pp-airflow-minio \
  -n pizza-pulse \
  --from-literal=AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -
```

## 10. Install MLflow

MLflow uses:

- PostgreSQL backend store: `pp-postgre-postgresql:5432/mlflow`
- MinIO artifacts: `s3://pp-mlflow-artifacts`
- MinIO endpoint: `http://pp-minio:9000`

```bash
helm upgrade --install pp-mlflow community-charts/mlflow \
  --namespace pizza-pulse \
  -f mlflow-values.yaml
```

Check:

```bash
kubectl -n pizza-pulse rollout status deployment/pp-mlflow
```

## 11. Install Airflow

Create the Airflow metadata secret:

```bash
kubectl create secret generic pp-airflow-metadata \
  -n pizza-pulse \
  --from-literal=connection="postgresql://postgres:admin@pp-postgre-postgresql:5432/airflow" \
  --dry-run=client -o yaml | kubectl apply -f -
```

Install Airflow:

```bash
helm upgrade --install pp-airflow apache-airflow/airflow \
  --namespace pizza-pulse \
  -f airflow-values.yaml
```

Wait for the main components:

```bash
kubectl -n pizza-pulse rollout status deployment/pp-airflow-api-server
kubectl -n pizza-pulse rollout status deployment/pp-airflow-scheduler
kubectl -n pizza-pulse rollout status deployment/pp-airflow-dag-processor
```

Grant Airflow permissions to create and watch SparkApplication resources:

```bash
kubectl apply -f airflow-rbac-spark.yaml
```

Check RBAC:

```bash
kubectl auth can-i create sparkapplications.sparkoperator.k8s.io \
  -n pizza-pulse \
  --as=system:serviceaccount:pizza-pulse:pp-airflow-scheduler
```

Expected result:

```text
yes
```

## 12. Build Batch Job Image

The `pizza_batch_mlops` DAG expects this image in `spark-apps/pizza-batch-*.yaml` and the training/compare Kubernetes pods:

```bash
cd ..
docker build -t manhhung1685/ppbatch-pipeline:0.0.1 jobs/batch_pipeline
docker push manhhung1685/ppbatch-pipeline:0.0.1
cd helm-value
```

If you use another registry or tag, update `BATCH_IMAGE` in `airflow/dags/pizza_batch_mlops.py` and the image fields in `spark-apps/pizza-batch-*.yaml`.

## 13. Port Forward

Use the repo-root script to forward services for local access. If you are in `helm-value/`, run:

```bash
../scripts/port-forward.sh start
../scripts/port-forward.sh status
```

Default local endpoints:

| Service | URL / Endpoint |
| --- | --- |
| PostgreSQL | `localhost:5432` |
| MinIO API | `http://localhost:9000` |
| MinIO Console | `http://localhost:9001` |
| MLflow | `http://localhost:5000` |
| Kafka bootstrap | `localhost:9092` |
| Kafka UI | `http://localhost:8082` |
| Airflow API Server | `http://localhost:8080` |

Stop port-forwarding:

```bash
../scripts/port-forward.sh stop
```

Note: Kafka port-forwarding here uses the bootstrap service `pp-kafka-kafka-bootstrap`. Some local Kafka clients may fail because brokers advertise internal Kubernetes DNS names. For stable local Kafka clients, configure a Strimzi external listener.

## 14. Useful Commands

Show all resources in the namespace:

```bash
kubectl -n pizza-pulse get all
```

Show services:

```bash
kubectl -n pizza-pulse get svc
```

Exec into the Airflow scheduler:

```bash
kubectl exec -it deploy/pp-airflow-scheduler -n pizza-pulse -- bash
```

Check the Airflow metadata secret:

```bash
kubectl get secret pp-airflow-metadata -n pizza-pulse -o yaml
```

Check that a Spark image contains the required jars:

```bash
kubectl run jar-check \
  -n pizza-pulse \
  --rm -it \
  --image=<your-spark-bootstrap-image> \
  --restart=Never \
  -- bash -lc "ls -l /opt/spark/jars"
```

## 15. Cleanup

Stop port-forwarding first:

```bash
../scripts/port-forward.sh stop
```

Delete Kafka custom resources before uninstalling the Strimzi operator:

```bash
kubectl delete -f KafkaTopic.yaml -n pizza-pulse --ignore-not-found
kubectl delete -f Kafka.yaml -n pizza-pulse --ignore-not-found
```

Uninstall releases:

```bash
helm uninstall pp-airflow -n pizza-pulse
helm uninstall pp-mlflow -n pizza-pulse
helm uninstall pp-kafka-ui -n pizza-pulse
helm uninstall pp-strimzi -n pizza-pulse
helm uninstall pp-spark-operator -n pizza-pulse
helm uninstall pp-minio -n pizza-pulse
helm uninstall pp-postgre -n pizza-pulse
```

Delete the namespace if you want to remove everything in it:

```bash
kubectl delete namespace pizza-pulse
```
