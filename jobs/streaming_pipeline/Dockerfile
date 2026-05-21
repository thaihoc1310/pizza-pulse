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
    "psycopg[binary]>=3.1,<4" \
    "pyarrow>=14,<17" \
    "scikit-learn>=1.3,<2"

RUN mkdir -p /opt/spark/jobs

COPY realtime_contracts.py /opt/spark/jobs/realtime_contracts.py
COPY pizza_streaming_inference.py /opt/spark/jobs/pizza_streaming_inference.py

ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar /opt/spark/jars/postgresql-42.7.3.jar
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/hadoop-aws-3.3.4.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/kafka-clients-3.4.1.jar
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/commons-pool2-2.11.1.jar

RUN chmod 644 /opt/spark/jobs/*.py \
    && chmod 644 /opt/spark/jars/*.jar

USER spark
