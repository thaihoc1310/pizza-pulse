Namespace: pizza-pulse

Helm releases:
pp-minio
pp-postgre
pp-kafka
pp-kafka-ui
pp-spark-operator
pp-mlflow
pp-airflow
pp-streamlit

PostgreSQL DB:
mlflow
airflow
pizza_serving

MinIO buckets:
pp-lakehouse
pp-mlflow-artifacts
pp-spark-checkpoints

Kafka topics:
pp.order.events
pp.demand.predictions
pp.ingredient.alerts

Batch MLOps DAG:
pizza_batch_mlops

Batch job image:
thaihoc285/ppbatch-pipeline:0.0.1

Batch SparkApplications:
pizza-batch-etl.yaml
pizza-batch-features.yaml

Batch KubernetesPodOperator jobs:
train_lightgbm
train_catboost
compare_and_register_model

Lakehouse paths:
s3a://pp-lakehouse/silver/order_line_items
s3a://pp-lakehouse/silver/hourly_demand
s3a://pp-lakehouse/gold/demand_features

MLflow:
tracking URI: http://pp-mlflow:80
experiment: pizza-pulse-batch
registered model: pizza_hourly_demand
model alias: champion
