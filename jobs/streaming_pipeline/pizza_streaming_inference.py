from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
from typing import Any

sys.path.append(str(Path(__file__).resolve().parent))

import numpy as np
import pandas as pd
import psycopg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    countDistinct,
    date_trunc,
    dayofmonth,
    dayofweek,
    explode,
    expr,
    from_json,
    hour,
    lit,
    month,
    sum as spark_sum,
    to_json,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from realtime_contracts import (
    CATEGORICAL_FEATURES,
    FEATURE_COLUMNS,
    NUMERIC_FEATURES,
    ChampionModelCache,
    ingredient_alert_event,
    json_dumps,
    prediction_event,
)


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "pp-postgre-postgresql")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "pizza_serving")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "pp-kafka-kafka-bootstrap:9092")
KAFKA_ORDER_TOPIC = os.getenv("KAFKA_ORDER_TOPIC", "pp.order.events")
KAFKA_PREDICTION_TOPIC = os.getenv("KAFKA_PREDICTION_TOPIC", "pp.demand.predictions")
KAFKA_INGREDIENT_ALERT_TOPIC = os.getenv("KAFKA_INGREDIENT_ALERT_TOPIC", "pp.ingredient.alerts")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://pp-minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
LAKEHOUSE_ROOT = os.getenv("LAKEHOUSE_ROOT", "s3a://pp-lakehouse")
SILVER_HOURLY_DEMAND_PATH = "/".join([LAKEHOUSE_ROOT.rstrip("/"), "silver", "hourly_demand"])
CHECKPOINT_LOCATION = os.getenv(
    "STREAMING_CHECKPOINT_LOCATION",
    "s3a://pp-spark-checkpoints/pizza-streaming-inference",
)
PIPELINE_TIMEZONE = os.getenv("PIPELINE_TIMEZONE", "Asia/Ho_Chi_Minh")

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://pp-mlflow:80")
MODEL_NAME = os.getenv("MODEL_NAME", "pizza_hourly_demand")
MODEL_ALIAS = os.getenv("MODEL_ALIAS", "champion")
MODEL_REFRESH_SECONDS = int(os.getenv("MODEL_REFRESH_SECONDS", "60"))
TRIGGER_PROCESSING_TIME = os.getenv("STREAMING_TRIGGER_PROCESSING_TIME", "15 seconds")


ORDER_ITEM_SCHEMA = StructType(
    [
        StructField("order_details_id", LongType(), False),
        StructField("pizza_id", StringType(), False),
        StructField("quantity", DoubleType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("total_price", DoubleType(), False),
    ]
)

ORDER_EVENT_SCHEMA = StructType(
    [
        StructField("schema_version", IntegerType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField(
            "order",
            StructType(
                [
                    StructField("order_id", LongType(), False),
                    StructField("order_ts", StringType(), False),
                    StructField("source", StringType(), True),
                    StructField("items", ArrayType(ORDER_ITEM_SCHEMA), False),
                ]
            ),
            False,
        ),
    ]
)

REALTIME_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS online_hourly_demand (
    order_hour TIMESTAMP NOT NULL,
    pizza_id TEXT NOT NULL REFERENCES pizza(pizza_id),
    pizza_name TEXT,
    pizza_size TEXT,
    pizza_category TEXT,
    quantity NUMERIC(12, 2) NOT NULL,
    revenue NUMERIC(12, 2) NOT NULL,
    order_count BIGINT NOT NULL,
    last_event_ts TIMESTAMP,
    updated_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (order_hour, pizza_id)
);

CREATE TABLE IF NOT EXISTS demand_predictions (
    target_hour TIMESTAMP NOT NULL,
    pizza_id TEXT NOT NULL REFERENCES pizza(pizza_id),
    pizza_name TEXT,
    pizza_size TEXT,
    pizza_category TEXT,
    predicted_quantity NUMERIC(12, 4) NOT NULL,
    model_name TEXT NOT NULL,
    model_alias TEXT NOT NULL,
    model_version TEXT NOT NULL,
    predicted_at TIMESTAMP NOT NULL,
    feature_json JSONB,
    updated_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (target_hour, pizza_id)
);

CREATE TABLE IF NOT EXISTS ingredient_risk_predictions (
    target_hour TIMESTAMP NOT NULL,
    ingredient_id INT NOT NULL REFERENCES ingredients(ingredient_id),
    ingredient_name TEXT NOT NULL,
    predicted_usage NUMERIC(12, 4) NOT NULL,
    current_stock NUMERIC(12, 4) NOT NULL,
    projected_stock NUMERIC(12, 4) NOT NULL,
    severity TEXT NOT NULL,
    model_name TEXT NOT NULL,
    model_alias TEXT NOT NULL,
    model_version TEXT NOT NULL,
    predicted_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (target_hour, ingredient_id)
);
"""


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


def build_spark() -> SparkSession:
    require_runtime_config()
    return (
        SparkSession.builder.appName("pizza-streaming-inference")
        .config("spark.sql.session.timeZone", PIPELINE_TIMEZONE)
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "16"))
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


def postgres_connect():
    return psycopg.connect(
        host=POSTGRES_HOST,
        port=int(POSTGRES_PORT),
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def jdbc_options(dbtable: str) -> dict[str, str]:
    return {
        "url": JDBC_URL,
        "dbtable": dbtable,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }


def read_jdbc_table(spark: SparkSession, table_name: str) -> DataFrame:
    return spark.read.format("jdbc").options(**jdbc_options(table_name)).load()


def read_jdbc_query(spark: SparkSession, sql: str) -> DataFrame:
    return spark.read.format("jdbc").options(**jdbc_options(f"({sql}) AS q")).load()


def ensure_realtime_tables() -> None:
    with postgres_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(REALTIME_TABLE_DDL)


def parse_order_events(raw: DataFrame) -> DataFrame:
    parsed = raw.select(
        from_json(col("value").cast("string"), ORDER_EVENT_SCHEMA).alias("event")
    ).filter(col("event").isNotNull())

    return (
        parsed.filter(col("event.event_type") == lit("order_created"))
        .select(
            col("event.event_id").alias("event_id"),
            to_timestamp(col("event.event_ts")).alias("event_ts"),
            col("event.order.order_id").alias("order_id"),
            to_timestamp(col("event.order.order_ts")).alias("order_ts"),
            explode(col("event.order.items")).alias("item"),
        )
        .select(
            "event_id",
            "event_ts",
            "order_id",
            "order_ts",
            date_trunc("hour", col("order_ts")).alias("order_hour"),
            col("item.order_details_id").alias("order_details_id"),
            col("item.pizza_id").alias("pizza_id"),
            col("item.quantity").cast("double").alias("quantity"),
            col("item.unit_price").cast("double").alias("unit_price"),
            col("item.total_price").cast("double").alias("total_price"),
        )
        .filter(
            col("order_details_id").isNotNull()
            & col("order_id").isNotNull()
            & col("order_ts").isNotNull()
            & col("pizza_id").isNotNull()
            & (col("quantity") > 0)
        )
    )


def recompute_online_hourly_demand(affected_pairs: list[tuple[datetime, str]]) -> None:
    if not affected_pairs:
        return

    placeholders = ",".join(["(%s::timestamp, %s::text)"] * len(affected_pairs))
    params: list[Any] = []
    for order_hour, pizza_id in affected_pairs:
        params.extend([order_hour, pizza_id])

    sql = f"""
    WITH affected(order_hour, pizza_id) AS (
        VALUES {placeholders}
    ),
    aggregated AS (
        SELECT
            a.order_hour,
            oi.pizza_id,
            p.pizza_name,
            p.pizza_size,
            p.pizza_category,
            SUM(oi.quantity) AS quantity,
            SUM(oi.total_price) AS revenue,
            COUNT(DISTINCT oi.order_id) AS order_count,
            MAX(o.created_at) AS last_event_ts
        FROM order_items oi
        JOIN orders o
            ON oi.order_id = o.order_id
        JOIN affected a
            ON date_trunc('hour', o.order_ts) = a.order_hour
            AND oi.pizza_id = a.pizza_id
        LEFT JOIN pizza p
            ON oi.pizza_id = p.pizza_id
        GROUP BY
            a.order_hour,
            oi.pizza_id,
            p.pizza_name,
            p.pizza_size,
            p.pizza_category
    )
    INSERT INTO online_hourly_demand (
        order_hour,
        pizza_id,
        pizza_name,
        pizza_size,
        pizza_category,
        quantity,
        revenue,
        order_count,
        last_event_ts
    )
    SELECT
        order_hour,
        pizza_id,
        pizza_name,
        pizza_size,
        pizza_category,
        quantity,
        revenue,
        order_count,
        last_event_ts
    FROM aggregated
    ON CONFLICT (order_hour, pizza_id) DO UPDATE
    SET
        pizza_name = EXCLUDED.pizza_name,
        pizza_size = EXCLUDED.pizza_size,
        pizza_category = EXCLUDED.pizza_category,
        quantity = EXCLUDED.quantity,
        revenue = EXCLUDED.revenue,
        order_count = EXCLUDED.order_count,
        last_event_ts = EXCLUDED.last_event_ts,
        updated_at = now()
    """

    with postgres_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)


def affected_pairs(line_items: DataFrame) -> list[tuple[datetime, str]]:
    rows = line_items.select("order_hour", "pizza_id").distinct().collect()
    return [(row["order_hour"], row["pizza_id"]) for row in rows]


def read_historical_hourly(spark: SparkSession, start_hour: datetime, end_hour: datetime) -> DataFrame:
    schema = StructType(
        [
            StructField("order_hour", TimestampType(), True),
            StructField("pizza_id", StringType(), True),
            StructField("quantity", DoubleType(), True),
        ]
    )
    try:
        return (
            spark.read.parquet(SILVER_HOURLY_DEMAND_PATH)
            .select(
                col("order_hour").cast("timestamp"),
                col("pizza_id"),
                col("quantity").cast("double"),
            )
            .filter((col("order_hour") >= lit(start_hour)) & (col("order_hour") <= lit(end_hour)))
        )
    except Exception as exc:
        print(f"Could not read historical hourly demand from {SILVER_HOURLY_DEMAND_PATH}: {exc}")
        return spark.createDataFrame([], schema)


def read_online_hourly(spark: SparkSession, start_hour: datetime, end_hour: datetime) -> DataFrame:
    return read_jdbc_query(
        spark,
        f"""
        SELECT
            date_trunc('hour', o.order_ts)::timestamp AS order_hour,
            oi.pizza_id,
            SUM(oi.quantity)::double precision AS quantity
        FROM order_items oi
        JOIN orders o
            ON oi.order_id = o.order_id
        WHERE o.order_ts >= TIMESTAMP '{start_hour:%Y-%m-%d %H:%M:%S}'
          AND o.order_ts <= TIMESTAMP '{end_hour:%Y-%m-%d %H:%M:%S}'
        GROUP BY date_trunc('hour', o.order_ts), oi.pizza_id
        """
    )


def build_feature_frame(spark: SparkSession, target_hours: DataFrame, pizzas: DataFrame) -> DataFrame:
    bounds = target_hours.agg({"target_hour": "min"}).withColumnRenamed(
        "min(target_hour)", "min_target_hour"
    ).crossJoin(
        target_hours.agg({"target_hour": "max"}).withColumnRenamed(
            "max(target_hour)", "max_target_hour"
        )
    ).first()
    if not bounds or not bounds["min_target_hour"] or not bounds["max_target_hour"]:
        return spark.createDataFrame([], StructType([]))

    min_target = bounds["min_target_hour"]
    max_target = bounds["max_target_hour"]
    history_start = min_target - timedelta(hours=168)
    history_end = max_target - timedelta(hours=1)

    history = (
        read_historical_hourly(spark, history_start, history_end)
        .unionByName(read_online_hourly(spark, history_start, history_end))
        .groupBy("order_hour", "pizza_id")
        .agg(spark_sum("quantity").alias("quantity"))
    )

    targets = (
        target_hours.crossJoin(
            pizzas.select(
                "pizza_id",
                "pizza_name",
                "pizza_size",
                "pizza_category",
            )
        )
        .withColumn("hour", hour(col("target_hour")))
        .withColumn("day_of_week", dayofweek(col("target_hour")))
        .withColumn("day_of_month", dayofmonth(col("target_hour")))
        .withColumn("month", month(col("target_hour")))
        .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), lit(1)).otherwise(lit(0)))
    )

    features = add_lag_feature(targets, history, 1, "lag_1h")
    features = add_lag_feature(features, history, 24, "lag_24h")
    features = add_lag_feature(features, history, 168, "lag_168h")
    features = add_rolling_features(features, history, 24)
    features = add_rolling_features(features, history, 168)

    for column in NUMERIC_FEATURES:
        features = features.withColumn(column, coalesce(col(column).cast("double"), lit(0.0)))
    for column in CATEGORICAL_FEATURES:
        features = features.withColumn(column, coalesce(col(column).cast("string"), lit("unknown")))

    return features


def add_lag_feature(features: DataFrame, history: DataFrame, lag_hours: int, output_col: str) -> DataFrame:
    joined = features.alias("f").join(
        history.alias("h"),
        (col("f.pizza_id") == col("h.pizza_id"))
        & (col("h.order_hour") == col("f.target_hour") - expr(f"INTERVAL {lag_hours} HOURS")),
        "left",
    )
    return joined.select("f.*", coalesce(col("h.quantity"), lit(0.0)).alias(output_col))


def add_rolling_features(features: DataFrame, history: DataFrame, hours_count: int) -> DataFrame:
    key_columns = [
        "target_hour",
        "pizza_id",
        "pizza_name",
        "pizza_size",
        "pizza_category",
        "hour",
        "day_of_week",
        "day_of_month",
        "month",
        "is_weekend",
        "lag_1h",
        "lag_24h",
        "lag_168h",
    ]
    existing_rolling = [
        column
        for column in [
            "rolling_sum_24h",
            "rolling_mean_24h",
            "rolling_sum_168h",
            "rolling_mean_168h",
        ]
        if column in features.columns
    ]
    key_columns.extend(existing_rolling)

    rolling = (
        features.alias("f")
        .join(
            history.alias("h"),
            (col("f.pizza_id") == col("h.pizza_id"))
            & (col("h.order_hour") >= col("f.target_hour") - expr(f"INTERVAL {hours_count} HOURS"))
            & (col("h.order_hour") < col("f.target_hour")),
            "left",
        )
        .groupBy(*[col(f"f.{column}").alias(column) for column in key_columns])
        .agg(coalesce(spark_sum(col("h.quantity")), lit(0.0)).alias(f"rolling_sum_{hours_count}h"))
    )
    return rolling.withColumn(
        f"rolling_mean_{hours_count}h",
        col(f"rolling_sum_{hours_count}h") / lit(float(hours_count)),
    )


def predict_features(features: DataFrame, model_cache: ChampionModelCache) -> list[dict[str, Any]]:
    if features.limit(1).count() == 0:
        return []

    pdf = features.toPandas()
    if pdf.empty:
        return []

    for column in NUMERIC_FEATURES:
        pdf[column] = pd.to_numeric(pdf[column], errors="coerce").fillna(0.0)
    for column in CATEGORICAL_FEATURES:
        pdf[column] = pdf[column].fillna("unknown").astype(str)

    cached = model_cache.get()
    predictions = np.maximum(cached.model.predict(pdf[FEATURE_COLUMNS]), 0.0)
    predicted_at = datetime.now(timezone.utc).replace(tzinfo=None)

    records = []
    for idx, predicted_quantity in enumerate(predictions):
        row = pdf.iloc[idx]
        feature_json = {
            column: (None if pd.isna(row[column]) else row[column])
            for column in FEATURE_COLUMNS
        }
        records.append(
            {
                "target_hour": pd.Timestamp(row["target_hour"]).to_pydatetime().replace(tzinfo=None),
                "pizza_id": row["pizza_id"],
                "pizza_name": row.get("pizza_name"),
                "pizza_size": row.get("pizza_size"),
                "pizza_category": row.get("pizza_category"),
                "predicted_quantity": float(predicted_quantity),
                "model_name": MODEL_NAME,
                "model_alias": MODEL_ALIAS,
                "model_version": cached.version,
                "predicted_at": predicted_at,
                "feature_json": json_dumps(feature_json),
            }
        )
    return records


def write_predictions(records: list[dict[str, Any]]) -> None:
    if not records:
        return

    with postgres_connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO demand_predictions (
                    target_hour,
                    pizza_id,
                    pizza_name,
                    pizza_size,
                    pizza_category,
                    predicted_quantity,
                    model_name,
                    model_alias,
                    model_version,
                    predicted_at,
                    feature_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (target_hour, pizza_id) DO UPDATE
                SET
                    pizza_name = EXCLUDED.pizza_name,
                    pizza_size = EXCLUDED.pizza_size,
                    pizza_category = EXCLUDED.pizza_category,
                    predicted_quantity = EXCLUDED.predicted_quantity,
                    model_name = EXCLUDED.model_name,
                    model_alias = EXCLUDED.model_alias,
                    model_version = EXCLUDED.model_version,
                    predicted_at = EXCLUDED.predicted_at,
                    feature_json = EXCLUDED.feature_json,
                    updated_at = now()
                """,
                [
                    (
                        row["target_hour"],
                        row["pizza_id"],
                        row["pizza_name"],
                        row["pizza_size"],
                        row["pizza_category"],
                        row["predicted_quantity"],
                        row["model_name"],
                        row["model_alias"],
                        row["model_version"],
                        row["predicted_at"],
                        row["feature_json"],
                    )
                    for row in records
                ],
            )


def prediction_records_to_spark(spark: SparkSession, records: list[dict[str, Any]]) -> DataFrame:
    rows = [
        {
            key: value
            for key, value in record.items()
            if key != "feature_json"
        }
        for record in records
    ]
    return spark.createDataFrame(rows)


def build_ingredient_risks(spark: SparkSession, predictions: DataFrame) -> DataFrame:
    pizza_ingredients = (
        read_jdbc_table(spark, "pizza_ingredients")
        .select("pizza_id", col("ingredient_id").cast("int"), col("unit_amount").cast("double"))
    )
    ingredients = (
        read_jdbc_table(spark, "ingredients")
        .select(
            col("ingredient_id").cast("int"),
            "ingredient_name",
            col("current_stock").cast("double"),
        )
    )

    usage = (
        predictions.join(pizza_ingredients, on="pizza_id", how="inner")
        .withColumn("ingredient_usage", col("predicted_quantity") * col("unit_amount"))
        .groupBy(
            "target_hour",
            "ingredient_id",
            "model_name",
            "model_alias",
            "model_version",
            "predicted_at",
        )
        .agg(spark_sum("ingredient_usage").alias("predicted_usage"))
    )

    return (
        usage.join(ingredients, on="ingredient_id", how="inner")
        .withColumn("projected_stock", col("current_stock") - col("predicted_usage"))
        .withColumn(
            "severity",
            when(col("projected_stock") < 0, lit("critical"))
            .when(col("predicted_usage") >= col("current_stock") * lit(0.8), lit("warning"))
            .otherwise(lit("ok")),
        )
        .select(
            "target_hour",
            "ingredient_id",
            "ingredient_name",
            "predicted_usage",
            "current_stock",
            "projected_stock",
            "severity",
            "model_name",
            "model_alias",
            "model_version",
            "predicted_at",
        )
    )


def write_ingredient_risks(risks: DataFrame) -> list[dict[str, Any]]:
    rows = [row.asDict() for row in risks.collect()]
    if not rows:
        return []

    with postgres_connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO ingredient_risk_predictions (
                    target_hour,
                    ingredient_id,
                    ingredient_name,
                    predicted_usage,
                    current_stock,
                    projected_stock,
                    severity,
                    model_name,
                    model_alias,
                    model_version,
                    predicted_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (target_hour, ingredient_id) DO UPDATE
                SET
                    ingredient_name = EXCLUDED.ingredient_name,
                    predicted_usage = EXCLUDED.predicted_usage,
                    current_stock = EXCLUDED.current_stock,
                    projected_stock = EXCLUDED.projected_stock,
                    severity = EXCLUDED.severity,
                    model_name = EXCLUDED.model_name,
                    model_alias = EXCLUDED.model_alias,
                    model_version = EXCLUDED.model_version,
                    predicted_at = EXCLUDED.predicted_at,
                    updated_at = now()
                """,
                [
                    (
                        row["target_hour"],
                        row["ingredient_id"],
                        row["ingredient_name"],
                        row["predicted_usage"],
                        row["current_stock"],
                        row["projected_stock"],
                        row["severity"],
                        row["model_name"],
                        row["model_alias"],
                        row["model_version"],
                        row["predicted_at"],
                    )
                    for row in rows
                ],
            )
    return rows


def write_prediction_events_to_kafka(spark: SparkSession, records: list[dict[str, Any]]) -> None:
    if not records:
        return
    events = [
        {
            "key": str(record["pizza_id"]),
            "value": json_dumps(prediction_event(record)),
        }
        for record in records
    ]
    write_kafka_events(spark, events, KAFKA_PREDICTION_TOPIC)


def write_alert_events_to_kafka(spark: SparkSession, records: list[dict[str, Any]]) -> None:
    events = [
        {
            "key": str(record["ingredient_id"]),
            "value": json_dumps(ingredient_alert_event(record)),
        }
        for record in records
        if record["severity"] != "ok"
    ]
    write_kafka_events(spark, events, KAFKA_INGREDIENT_ALERT_TOPIC)


def write_kafka_events(spark: SparkSession, events: list[dict[str, str]], topic: str) -> None:
    if not events:
        return
    (
        spark.createDataFrame(events)
        .select(col("key").cast("string").alias("key"), col("value").cast("string").alias("value"))
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", topic)
        .save()
    )


def process_batch(batch: DataFrame, batch_id: int, model_cache: ChampionModelCache) -> None:
    if batch.limit(1).count() == 0:
        return

    spark = batch.sparkSession
    print(f"Processing streaming batch_id={batch_id}")
    line_items = batch.cache()
    line_item_count = line_items.count()
    pairs = affected_pairs(line_items)
    recompute_online_hourly_demand(pairs)

    target_hours = line_items.select((col("order_hour") + expr("INTERVAL 1 HOUR")).alias("target_hour")).distinct()
    pizzas = read_jdbc_table(spark, "pizza").select(
        "pizza_id",
        "pizza_name",
        "pizza_size",
        "pizza_category",
    )
    features = build_feature_frame(spark, target_hours, pizzas)
    prediction_records = predict_features(features, model_cache)
    write_predictions(prediction_records)
    write_prediction_events_to_kafka(spark, prediction_records)

    if prediction_records:
        prediction_df = prediction_records_to_spark(spark, prediction_records)
        risk_df = build_ingredient_risks(spark, prediction_df)
        risk_records = write_ingredient_risks(risk_df)
        write_alert_events_to_kafka(spark, risk_records)

    line_items.unpersist()
    print(
        f"Completed streaming batch_id={batch_id}, "
        f"line_items={line_item_count}, predictions={len(prediction_records)}"
    )


def main() -> None:
    ensure_realtime_tables()
    spark = build_spark()
    model_cache = ChampionModelCache(
        tracking_uri=MLFLOW_TRACKING_URI,
        model_name=MODEL_NAME,
        alias=MODEL_ALIAS,
        refresh_seconds=MODEL_REFRESH_SECONDS,
    )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_ORDER_TOPIC)
        .option("startingOffsets", os.getenv("KAFKA_STARTING_OFFSETS", "latest"))
        .option("failOnDataLoss", "false")
        .load()
    )

    line_items = parse_order_events(raw)
    query = (
        line_items.writeStream.foreachBatch(
            lambda batch, batch_id: process_batch(batch, batch_id, model_cache)
        )
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime=TRIGGER_PROCESSING_TIME)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
