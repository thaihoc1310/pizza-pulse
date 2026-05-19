helm create ns pizza-pulse
kubectl --namespace pizza-pulse port-forward $POD_NAME 8080:8080

helm upgrade --install pp-postgre bitnami/postgresql \
  --namespace pizza-pulse \
  -f postgres-values.yaml

helm upgrade --install pp-minio minio/minio \
  --namespace pizza-pulse \
  -f minio-values.yaml

helm upgrade --install spark-operator spark-operator/spark-operator \
  --namespace pizza-pulse \
  --set "spark.jobNamespaces={pizza-pulse}"


helm upgrade --install pp-strimzi strimzi/strimzi-kafka-operator \
  --namespace pizza-pulse \
  --set watchNamespaces="{pizza-pulse}"

kubectl apply -f Kafka.yaml -n pizza-pulse
kubectl apply -f KafkaTopic.yaml -n pizza-pulse

helm upgrade --install pp-kafka-ui kafka-ui/kafka-ui \
  -n pizza-pulse \
  -f kafka-ui-values.yaml

helm upgrade --install pp-mlflow community-charts/mlflow \
  -n pizza-pulse \
  -f mlflow-values.yaml

kubectl create secret generic pp-airflow-metadata \
  -n pizza-pulse \
  --from-literal=connection="postgresql://postgres:${POSTGRES_PASSWORD}@pp-postgre-postgresql:5432/airflow"

helm upgrade --install pp-airflow apache-airflow/airflow \
  -n pizza-pulse \
  -f airflow-values.yaml

kubectl apply -f airflow-rbac-spark.yaml
kubectl auth can-i create sparkapplications.sparkoperator.k8s.io \
  -n pizza-pulse \
  --as=system:serviceaccount:pizza-pulse:pp-airflow-scheduler
  
kubectl get secret pp-airflow-metadata -n pizza-pulse -o yaml
kubectl --namespace pizza-pulse  port-forward svc/pp-minio-console 9001:9001
kubectl exec -it deploy/pp-airflow-scheduler -n pizza-pulse -- bash