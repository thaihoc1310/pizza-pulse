from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math
import os
from pathlib import Path
import sys
from typing import Any

sys.path.append(str(Path(__file__).resolve().parent))

import numpy as np
import pandas as pd
import psycopg
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    cos,
    date_trunc,
    dayofmonth,
    dayofweek,
    explode,
    expr,
    from_json,
    greatest,
    hour,
    lit,
    month,
    pow as spark_pow,
    regexp_replace,
    sin,
    sum as spark_sum,
    to_timestamp,
    to_date,
    when,
    year as spark_year,
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
    DOUBLE_FEATURES,
    FEATURE_COLUMNS,
    INTEGER_FEATURES,
    ChampionModelCache,
    ingredient_alert_event,
    integer_quantity,
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

GENERATOR_WEEKDAY_PIZZAS = float(os.getenv("GENERATOR_WEEKDAY_PIZZAS", "500"))
GENERATOR_WEEKEND_PIZZAS = float(os.getenv("GENERATOR_WEEKEND_PIZZAS", "650"))
GENERATOR_ANNUAL_GROWTH = float(os.getenv("GENERATOR_ANNUAL_GROWTH", "0.035"))
GENERATOR_OPEN_HOUR = int(os.getenv("GENERATOR_OPEN_HOUR", "10"))
GENERATOR_CLOSE_HOUR = int(os.getenv("GENERATOR_CLOSE_HOUR", "23"))
GENERATOR_START_YEAR = int(os.getenv("GENERATOR_START_YEAR", "2015"))

MONTH_FACTORS = {
    1: 0.94,
    2: 1.02,
    3: 1.00,
    4: 1.03,
    5: 1.08,
    6: 1.05,
    7: 1.07,
    8: 1.02,
    9: 1.04,
    10: 1.08,
    11: 1.12,
    12: 1.18,
}

WEEKDAY_FACTORS = {
    1: 0.96,
    2: 0.90,
    3: 0.93,
    4: 0.97,
    5: 1.02,
    6: 1.10,
    7: 1.06,
}

HOUR_WEIGHTS = {
    8: 0.05,
    9: 0.10,
    10: 0.50,
    11: 1.70,
    12: 3.30,
    13: 2.30,
    14: 0.95,
    15: 0.70,
    16: 1.00,
    17: 2.40,
    18: 3.90,
    19: 3.70,
    20: 2.40,
    21: 1.35,
    22: 0.75,
    23: 0.25,
}

HOLIDAY_MEAN_UNITS = {
    "new_year_day": 1500.0,
    "valentines_day": 1300.0,
    "st_patricks_day": 1100.0,
    "cinco_de_mayo": 1200.0,
    "independence_day": 1700.0,
    "halloween": 1400.0,
    "christmas_eve": 1500.0,
    "christmas_day": 1100.0,
    "new_year_eve": 1900.0,
    "super_bowl": 1800.0,
    "memorial_day_weekend": 1250.0,
    "labor_day_weekend": 1250.0,
    "mothers_day": 1150.0,
    "fathers_day": 1200.0,
    "thanksgiving_eve": 1500.0,
    "thanksgiving_day": 1100.0,
    "black_friday": 1300.0,
    "holiday_week": 1200.0,
}

CATEGORY_WEIGHTS = {
    "Classic": 1.22,
    "Chicken": 1.08,
    "Supreme": 0.92,
    "Veggie": 0.78,
}

SIZE_WEIGHTS = {
    "S": 0.62,
    "M": 1.55,
    "L": 1.20,
    "XL": 0.12,
    "XXL": 0.04,
}

LONG_TAIL_FAMILY_WEIGHT = 0.45

FAMILY_WEIGHTS = {
    "classic_dlx": 28.0,
    "pepperoni": 24.0,
    "bbq_ckn": 20.0,
    "hawaiian": 16.0,
    "cali_ckn": 14.0,
    "thai_ckn": 13.0,
    "four_cheese": 10.0,
    "ital_supr": 9.0,
    "spicy_ital": 8.5,
    "southw_ckn": 8.0,
    "five_cheese": 7.5,
    "mexicana": 7.0,
    "big_meat": 6.5,
    "pep_msh_pep": 6.0,
    "ckn_alfredo": 4.0,
    "ckn_pesto": 3.8,
    "napolitana": 3.4,
    "ital_cpcllo": 3.2,
    "sicilian": 3.0,
    "peppr_salami": 2.7,
    "spinach_fet": 2.2,
    "soppressata": 2.0,
    "calabrese": 1.5,
    "spinach_supr": 1.3,
    "veggie_veg": 1.2,
    "prsc_argla": 1.1,
    "spin_pesto": 1.0,
    "mediterraneo": 0.9,
    "ital_veggie": 0.85,
    "green_garden": 0.75,
    "the_greek": 0.55,
    "brie_carre": 0.30,
}

SUPER_BOWL_FAMILIES = {"pepperoni", "classic_dlx", "bbq_ckn", "big_meat", "pep_msh_pep"}
VALENTINES_FAMILIES = {"brie_carre", "five_cheese", "four_cheese", "spinach_fet"}
SUMMER_FAMILIES = {"hawaiian", "bbq_ckn", "cali_ckn"}
GRILLING_HOLIDAY_FAMILIES = {"bbq_ckn", "cali_ckn", "hawaiian", "pepperoni"}


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
    predicted_quantity BIGINT NOT NULL,
    model_name TEXT NOT NULL,
    model_alias TEXT NOT NULL,
    model_version TEXT NOT NULL,
    predicted_at TIMESTAMP NOT NULL,
    feature_json JSONB,
    updated_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (target_hour, pizza_id)
);

ALTER TABLE demand_predictions
    ALTER COLUMN predicted_quantity TYPE BIGINT
    USING ROUND(predicted_quantity::numeric)::BIGINT;

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

REFRESH_INGREDIENT_RISKS_SQL = """
WITH ingredient_usage AS (
    SELECT
        dp.target_hour,
        pi.ingredient_id,
        i.ingredient_name,
        SUM(dp.predicted_quantity::numeric * pi.unit_amount)::numeric(12, 4) AS predicted_usage,
        i.current_stock::numeric(12, 4) AS current_stock,
        MAX(dp.model_name) AS model_name,
        MAX(dp.model_alias) AS model_alias,
        MAX(dp.model_version) AS model_version,
        MAX(dp.predicted_at) AS predicted_at
    FROM demand_predictions dp
    JOIN pizza_ingredients pi
        ON dp.pizza_id = pi.pizza_id
    JOIN ingredients i
        ON pi.ingredient_id = i.ingredient_id
    GROUP BY
        dp.target_hour,
        pi.ingredient_id,
        i.ingredient_name,
        i.current_stock
),
scored AS (
    SELECT
        target_hour,
        ingredient_id,
        ingredient_name,
        predicted_usage,
        current_stock,
        (current_stock - predicted_usage)::numeric(12, 4) AS projected_stock,
        CASE
            WHEN current_stock - predicted_usage < 0 THEN 'critical'
            WHEN predicted_usage >= current_stock * 0.8 THEN 'warning'
            ELSE 'ok'
        END AS severity,
        model_name,
        model_alias,
        model_version,
        predicted_at
    FROM ingredient_usage
)
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
SELECT
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
FROM scored
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
    updated_at = now();
"""


def map_value(key_col, mapping: dict, default: float):
    result = lit(float(default))
    for key, value in reversed(list(mapping.items())):
        result = when(key_col == lit(key), lit(float(value))).otherwise(result)
    return result


def configured_hour_weight_sum(is_weekend: bool, is_holiday: bool) -> float:
    total = 0.0
    for hour_value, weight in HOUR_WEIGHTS.items():
        if hour_value < GENERATOR_OPEN_HOUR or hour_value > GENERATOR_CLOSE_HOUR:
            continue
        adjusted = weight
        if is_weekend and hour_value in {17, 18, 19, 20, 21}:
            adjusted *= 1.12
        if is_holiday and hour_value in {12, 13, 17, 18, 19, 20}:
            adjusted *= 1.18
        total += adjusted
    return total


HOUR_WEIGHT_SUMS = {
    (False, False): configured_hour_weight_sum(False, False),
    (True, False): configured_hour_weight_sum(True, False),
    (False, True): configured_hour_weight_sum(False, True),
    (True, True): configured_hour_weight_sum(True, True),
}


def hour_weight_total_expr():
    return (
        when(
            (col("is_weekend") == 1) & (col("is_holiday") == 1),
            lit(HOUR_WEIGHT_SUMS[(True, True)]),
        )
        .when(col("is_weekend") == 1, lit(HOUR_WEIGHT_SUMS[(True, False)]))
        .when(col("is_holiday") == 1, lit(HOUR_WEIGHT_SUMS[(False, True)]))
        .otherwise(lit(HOUR_WEIGHT_SUMS[(False, False)]))
    )


def add_generator_calendar_features(df: DataFrame, timestamp_col: str) -> DataFrame:
    two_pi = lit(2.0 * math.pi)
    feature_date = to_date(col(timestamp_col))

    df = (
        df.withColumn("hour", hour(col(timestamp_col)))
        .withColumn("day_of_week", dayofweek(col(timestamp_col)))
        .withColumn("day_of_month", dayofmonth(col(timestamp_col)))
        .withColumn("month", month(col(timestamp_col)))
        .withColumn("year", spark_year(col(timestamp_col)))
        .withColumn("_feature_date", feature_date)
        .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), lit(1)).otherwise(lit(0)))
        .withColumn(
            "is_open_hour",
            when(
                (col("hour") >= lit(GENERATOR_OPEN_HOUR))
                & (col("hour") <= lit(GENERATOR_CLOSE_HOUR))
                & (map_value(col("hour"), HOUR_WEIGHTS, 0.0) > lit(0.0)),
                lit(1),
            ).otherwise(lit(0)),
        )
        .withColumn("is_lunch_peak", when(col("hour").between(11, 14), lit(1)).otherwise(lit(0)))
        .withColumn("is_dinner_peak", when(col("hour").between(17, 20), lit(1)).otherwise(lit(0)))
        .withColumn("is_peak_hour", when(col("hour").isin(12, 18, 19), lit(1)).otherwise(lit(0)))
        .withColumn("_feb_first_sunday", expr("next_day(date_sub(make_date(year, 2, 1), 1), 'Sun')"))
        .withColumn(
            "_super_bowl",
            expr("date_add(_feb_first_sunday, CASE WHEN year >= 2022 THEN 7 ELSE 0 END)"),
        )
        .withColumn("_memorial_day", expr("next_day(date_sub(last_day(make_date(year, 5, 1)), 7), 'Mon')"))
        .withColumn("_labor_day", expr("next_day(date_sub(make_date(year, 9, 1), 1), 'Mon')"))
        .withColumn("_thanksgiving", expr("date_add(next_day(date_sub(make_date(year, 11, 1), 1), 'Thu'), 21)"))
        .withColumn("_mothers_day", expr("date_add(next_day(date_sub(make_date(year, 5, 1), 1), 'Sun'), 7)"))
        .withColumn("_fathers_day", expr("date_add(next_day(date_sub(make_date(year, 6, 1), 1), 'Sun'), 14)"))
    )

    df = df.withColumn(
        "holiday_name",
        when((col("month") == 1) & (col("day_of_month") == 1), lit("new_year_day"))
        .when((col("month") == 2) & (col("day_of_month") == 14), lit("valentines_day"))
        .when((col("month") == 3) & (col("day_of_month") == 17), lit("st_patricks_day"))
        .when((col("month") == 5) & (col("day_of_month") == 5), lit("cinco_de_mayo"))
        .when((col("month") == 7) & (col("day_of_month") == 4), lit("independence_day"))
        .when((col("month") == 10) & (col("day_of_month") == 31), lit("halloween"))
        .when((col("month") == 12) & (col("day_of_month") == 24), lit("christmas_eve"))
        .when((col("month") == 12) & (col("day_of_month") == 25), lit("christmas_day"))
        .when((col("month") == 12) & (col("day_of_month") == 31), lit("new_year_eve"))
        .when(col("_feature_date") == col("_super_bowl"), lit("super_bowl"))
        .when(
            (col("_feature_date") >= expr("date_sub(_memorial_day, 2)"))
            & (col("_feature_date") <= col("_memorial_day")),
            lit("memorial_day_weekend"),
        )
        .when(
            (col("_feature_date") >= expr("date_sub(_labor_day, 2)"))
            & (col("_feature_date") <= col("_labor_day")),
            lit("labor_day_weekend"),
        )
        .when(col("_feature_date") == col("_mothers_day"), lit("mothers_day"))
        .when(col("_feature_date") == col("_fathers_day"), lit("fathers_day"))
        .when(col("_feature_date") == expr("date_sub(_thanksgiving, 1)"), lit("thanksgiving_eve"))
        .when(col("_feature_date") == col("_thanksgiving"), lit("thanksgiving_day"))
        .when(col("_feature_date") == expr("date_add(_thanksgiving, 1)"), lit("black_friday"))
        .when((col("month") == 12) & col("day_of_month").between(26, 30), lit("holiday_week"))
        .otherwise(lit("none")),
    )

    df = (
        df.withColumn("holiday_mean_units", map_value(col("holiday_name"), HOLIDAY_MEAN_UNITS, 0.0))
        .withColumn("is_holiday", when(col("holiday_name") != lit("none"), lit(1)).otherwise(lit(0)))
        .withColumn("is_major_holiday", when(col("holiday_mean_units") >= lit(1500.0), lit(1)).otherwise(lit(0)))
        .withColumn("month_factor", map_value(col("month"), MONTH_FACTORS, 1.0))
        .withColumn("weekday_factor", map_value(col("day_of_week"), WEEKDAY_FACTORS, 1.0))
        .withColumn("years_since_2015", (col("year") - lit(GENERATOR_START_YEAR)).cast("double"))
        .withColumn(
            "annual_growth_factor",
            lit(1.0) + lit(GENERATOR_ANNUAL_GROWTH) * col("years_since_2015"),
        )
        .withColumn(
            "_base_daily_units",
            when(col("is_weekend") == 1, lit(GENERATOR_WEEKEND_PIZZAS)).otherwise(lit(GENERATOR_WEEKDAY_PIZZAS)),
        )
        .withColumn(
            "daily_demand_prior",
            when(col("is_holiday") == 1, col("holiday_mean_units") * col("annual_growth_factor")).otherwise(
                col("_base_daily_units")
                * col("annual_growth_factor")
                * col("month_factor")
                * col("weekday_factor")
            ),
        )
        .withColumn("_base_hour_weight", map_value(col("hour"), HOUR_WEIGHTS, 0.0))
        .withColumn(
            "hour_weight",
            when(col("is_open_hour") == 1, col("_base_hour_weight")).otherwise(lit(0.0)),
        )
        .withColumn(
            "hour_weight",
            col("hour_weight")
            * when((col("is_weekend") == 1) & col("hour").isin(17, 18, 19, 20, 21), lit(1.12)).otherwise(lit(1.0))
            * when((col("is_holiday") == 1) & col("hour").isin(12, 13, 17, 18, 19, 20), lit(1.18)).otherwise(lit(1.0)),
        )
        .withColumn("_hour_weight_total", hour_weight_total_expr())
        .withColumn(
            "hour_demand_prior",
            when(col("_hour_weight_total") > 0, col("daily_demand_prior") * col("hour_weight") / col("_hour_weight_total")).otherwise(lit(0.0)),
        )
        .withColumn(
            "daypart",
            when(col("is_open_hour") == 0, lit("closed"))
            .when(col("hour").between(11, 14), lit("lunch"))
            .when(col("hour").between(17, 20), lit("dinner"))
            .when(col("hour").between(21, 23), lit("late"))
            .when(col("hour").between(15, 16), lit("afternoon"))
            .otherwise(lit("open")),
        )
        .withColumn("hour_sin", sin(two_pi * col("hour") / lit(24.0)))
        .withColumn("hour_cos", cos(two_pi * col("hour") / lit(24.0)))
        .withColumn("dow_sin", sin(two_pi * (col("day_of_week") - lit(1)) / lit(7.0)))
        .withColumn("dow_cos", cos(two_pi * (col("day_of_week") - lit(1)) / lit(7.0)))
        .withColumn("month_sin", sin(two_pi * (col("month") - lit(1)) / lit(12.0)))
        .withColumn("month_cos", cos(two_pi * (col("month") - lit(1)) / lit(12.0)))
    )

    return df.drop(
        "_feature_date",
        "_feb_first_sunday",
        "_super_bowl",
        "_memorial_day",
        "_labor_day",
        "_thanksgiving",
        "_mothers_day",
        "_fathers_day",
        "_base_daily_units",
        "_base_hour_weight",
        "_hour_weight_total",
    )


def add_generator_pizza_features(df: DataFrame, timestamp_col: str) -> DataFrame:
    df = df.withColumn(
        "pizza_family",
        coalesce(col("pizza_family"), regexp_replace(col("pizza_id"), "_(?:s|m|l|xl|xxl)$", "")),
    ).withColumn("unit_price", coalesce(col("unit_price").cast("double"), lit(16.0)))

    price_weight = spark_pow(lit(16.0) / greatest(col("unit_price"), lit(0.01)), lit(0.35))
    df = df.withColumn(
        "pizza_base_weight",
        map_value(col("pizza_category"), CATEGORY_WEIGHTS, 1.0)
        * map_value(col("pizza_size"), SIZE_WEIGHTS, 1.0)
        * map_value(col("pizza_family"), FAMILY_WEIGHTS, LONG_TAIL_FAMILY_WEIGHT)
        * price_weight,
    )

    context_multiplier = (
        when((col("hour").between(11, 14)) & col("pizza_size").isin("S", "M"), lit(1.10)).otherwise(lit(1.0))
        * when((col("hour").between(17, 20)) & col("pizza_size").isin("M", "L", "XL"), lit(1.12)).otherwise(lit(1.0))
        * when((col("is_weekend") == 1) & col("pizza_size").isin("L", "XL", "XXL"), lit(1.10)).otherwise(lit(1.0))
        * when((col("month").isin(6, 7, 8)) & col("pizza_family").isin(*SUMMER_FAMILIES), lit(1.18)).otherwise(lit(1.0))
        * when((col("month").isin(11, 12)) & col("pizza_category").isin("Classic", "Supreme"), lit(1.08)).otherwise(lit(1.0))
        * when((col("month") == 1) & col("pizza_category").isin("Veggie", "Chicken"), lit(1.08)).otherwise(lit(1.0))
        * when((col("holiday_name") == "super_bowl") & col("pizza_family").isin(*SUPER_BOWL_FAMILIES), lit(1.45)).otherwise(lit(1.0))
        * when((col("holiday_name") == "valentines_day") & col("pizza_family").isin(*VALENTINES_FAMILIES), lit(1.35)).otherwise(lit(1.0))
        * when((col("holiday_name") == "cinco_de_mayo") & col("pizza_family").isin("mexicana", "southw_ckn"), lit(1.55)).otherwise(lit(1.0))
        * when(
            col("holiday_name").isin("independence_day", "memorial_day_weekend", "labor_day_weekend")
            & col("pizza_family").isin(*GRILLING_HOLIDAY_FAMILIES),
            lit(1.30),
        ).otherwise(lit(1.0))
        * when(
            col("holiday_name").isin("new_year_eve", "new_year_day", "holiday_week")
            & col("pizza_category").isin("Classic", "Supreme"),
            lit(1.16),
        ).otherwise(lit(1.0))
    )

    partition_window = Window.partitionBy(timestamp_col)
    return (
        df.withColumn("pizza_context_weight", col("pizza_base_weight") * context_multiplier)
        .withColumn("_pizza_context_weight_sum", spark_sum("pizza_context_weight").over(partition_window))
        .withColumn(
            "pizza_context_share",
            when(col("_pizza_context_weight_sum") > 0, col("pizza_context_weight") / col("_pizza_context_weight_sum")).otherwise(lit(0.0)),
        )
        .withColumn("pizza_hour_demand_prior", col("hour_demand_prior") * col("pizza_context_share"))
        .drop("_pizza_context_weight_sum")
    )


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
            cur.execute(REFRESH_INGREDIENT_RISKS_SQL)


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
    end_exclusive = end_hour + timedelta(hours=1)
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
          AND o.order_ts < TIMESTAMP '{end_exclusive:%Y-%m-%d %H:%M:%S}'
        GROUP BY date_trunc('hour', o.order_ts), oi.pizza_id
        """
    )


def merge_hourly_history(historical: DataFrame, online: DataFrame) -> DataFrame:
    return (
        historical.alias("h")
        .join(
            online.alias("o"),
            (col("h.order_hour") == col("o.order_hour")) & (col("h.pizza_id") == col("o.pizza_id")),
            "full_outer",
        )
        .select(
            coalesce(col("o.order_hour"), col("h.order_hour")).alias("order_hour"),
            coalesce(col("o.pizza_id"), col("h.pizza_id")).alias("pizza_id"),
            coalesce(col("o.quantity"), col("h.quantity"), lit(0.0)).alias("quantity"),
        )
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

    history = merge_hourly_history(
        read_historical_hourly(spark, history_start, history_end),
        read_online_hourly(spark, history_start, history_end),
    )

    targets = (
        target_hours.crossJoin(
            pizzas.select(
                "pizza_id",
                "pizza_name",
                "pizza_size",
                "pizza_category",
                "pizza_family",
                "unit_price",
            )
        )
    )
    targets = add_generator_pizza_features(add_generator_calendar_features(targets, "target_hour"), "target_hour")

    features = add_lag_feature(targets, history, 1, "lag_1h")
    features = add_lag_feature(features, history, 24, "lag_24h")
    features = add_lag_feature(features, history, 168, "lag_168h")
    features = add_rolling_features(features, history, 24)
    features = add_rolling_features(features, history, 168)

    for column in INTEGER_FEATURES:
        features = features.withColumn(column, coalesce(col(column).cast("int"), lit(0)))
    for column in DOUBLE_FEATURES:
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
    key_columns = features.columns

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


def integer_prediction_quantities(predictions: Any) -> np.ndarray:
    return np.asarray([integer_quantity(prediction) for prediction in predictions], dtype=np.int64)


def predict_features(features: DataFrame, model_cache: ChampionModelCache) -> list[dict[str, Any]]:
    if features.limit(1).count() == 0:
        return []

    pdf = features.toPandas()
    if pdf.empty:
        return []

    for column in INTEGER_FEATURES:
        pdf[column] = pd.to_numeric(pdf[column], errors="coerce").fillna(0).astype("int32")
    for column in DOUBLE_FEATURES:
        pdf[column] = pd.to_numeric(pdf[column], errors="coerce").fillna(0.0).astype("float64")
    for column in CATEGORICAL_FEATURES:
        pdf[column] = pdf[column].fillna("unknown").astype(str)

    cached = model_cache.get()
    raw_predictions = np.maximum(cached.model.predict(pdf[FEATURE_COLUMNS]), 0.0)
    if "is_open_hour" in pdf.columns:
        raw_predictions = np.where(pdf["is_open_hour"].astype("int32") == 1, raw_predictions, 0.0)
    predictions = integer_prediction_quantities(raw_predictions)
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
                "predicted_quantity": int(predicted_quantity),
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
        .withColumn("ingredient_usage", col("predicted_quantity").cast("long") * col("unit_amount"))
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
        regexp_replace(col("pizza_id"), "_(?:s|m|l|xl|xxl)$", "").alias("pizza_family"),
        col("unit_price").cast("double").alias("unit_price"),
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
