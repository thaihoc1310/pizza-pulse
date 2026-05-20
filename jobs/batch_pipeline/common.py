import os
import re
from typing import List, Optional

from pyspark.sql import SparkSession


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "pp-postgre-postgresql")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "pizza_serving")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://pp-minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")

LAKEHOUSE_ROOT = os.getenv("LAKEHOUSE_ROOT", "s3a://pp-lakehouse")
PIPELINE_TIMEZONE = os.getenv("PIPELINE_TIMEZONE", "Asia/Bangkok")
RUN_TAG = re.sub(r"[^a-zA-Z0-9_.=-]+", "-", os.getenv("BATCH_RUN_TAG", "manual")).strip("-")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


def require_runtime_config() -> None:
    missing = []
    if not POSTGRES_PASSWORD:
        missing.append("POSTGRES_PASSWORD")
    if not MINIO_ACCESS_KEY:
        missing.append("MINIO_ACCESS_KEY or AWS_ACCESS_KEY_ID")
    if not MINIO_SECRET_KEY:
        missing.append("MINIO_SECRET_KEY or AWS_SECRET_ACCESS_KEY")

    if missing:
        raise RuntimeError(f"Missing required runtime config: {', '.join(missing)}")


def build_spark(app_name: str) -> SparkSession:
    require_runtime_config()

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", PIPELINE_TIMEZONE)
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "64"))
        .config("spark.sql.crossJoin.enabled", "true")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def jdbc_options(dbtable: Optional[str] = None) -> dict:
    options = {
        "url": JDBC_URL,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }
    if dbtable:
        options["dbtable"] = dbtable
    return options


def read_jdbc_table(spark: SparkSession, table_name: str):
    return spark.read.format("jdbc").options(**jdbc_options(table_name)).load()


def lakehouse_path(*parts: str) -> str:
    clean = [part.strip("/") for part in parts if part]
    return "/".join([LAKEHOUSE_ROOT.rstrip("/"), *clean])


def save_parquet(df, path: str, mode: str = "overwrite", partition_by: Optional[List[str]] = None) -> None:
    writer = df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)
