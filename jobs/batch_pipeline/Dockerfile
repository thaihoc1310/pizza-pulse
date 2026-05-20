FROM spark:3.5.0

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libgomp1 \
        python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir \
    "boto3>=1.34,<2" \
    "catboost>=1.2,<2" \
    "lightgbm>=4.0,<5" \
    "mlflow>=2.12,<3" \
    "numpy>=1.24,<2" \
    "pandas>=2.0,<3" \
    "pyarrow>=14,<17" \
    "scikit-learn>=1.3,<2"

RUN mkdir -p /opt/spark/jobs

COPY common.py /opt/spark/jobs/common.py
COPY etl_orders.py /opt/spark/jobs/etl_orders.py
COPY feature_engineering.py /opt/spark/jobs/feature_engineering.py
COPY train_candidate_model.py /opt/spark/jobs/train_candidate_model.py
COPY compare_and_register_model.py /opt/spark/jobs/compare_and_register_model.py

ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar /opt/spark/jars/postgresql-42.7.3.jar
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/hadoop-aws-3.3.4.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

RUN chmod 644 /opt/spark/jobs/*.py \
    && chmod 644 /opt/spark/jars/postgresql-42.7.3.jar \
        /opt/spark/jars/hadoop-aws-3.3.4.jar \
        /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

USER spark
