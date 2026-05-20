FROM spark:3.5.0

USER root

RUN mkdir -p /opt/spark/jobs

COPY bootstrap_pizza_db.py /opt/spark/jobs/bootstrap_pizza_db.py

ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar /opt/spark/jars/postgresql-42.7.3.jar
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/hadoop-aws-3.3.4.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

RUN chmod 644 /opt/spark/jars/postgresql-42.7.3.jar \
    /opt/spark/jars/hadoop-aws-3.3.4.jar \
    /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    && chmod 644 /opt/spark/jobs/bootstrap_pizza_db.py

USER spark
