import os
from datetime import timedelta

from pyspark.sql import Window
from pyspark.sql.functions import (
    ceil,
    coalesce,
    col,
    countDistinct,
    current_timestamp,
    date_format,
    dayofweek,
    expr,
    from_unixtime,
    hour,
    lag,
    lead,
    lit,
    month,
    sum as spark_sum,
    to_date,
    to_timestamp,
    unix_timestamp,
    when,
    max as spark_max,
    min as spark_min,
)

from common import RUN_TAG, build_spark, lakehouse_path, save_parquet


SILVER_LINE_ITEMS_PATH = lakehouse_path("silver", "order_line_items")
GOLD_FEATURES_PATH = lakehouse_path("gold", "demand_features")
FEATURE_INTERVAL_MINUTES = int(os.getenv("FEATURE_INTERVAL_MINUTES", "15"))
MAX_FEATURE_LOOKBACK_DAYS = int(os.getenv("MAX_FEATURE_LOOKBACK_DAYS", "730"))
BIN_SECONDS = FEATURE_INTERVAL_MINUTES * 60
GROUP_COLUMNS = ["pizza_name", "pizza_size"]


def safe_growth(current_column: str, previous_column: str):
    return when(
        col(previous_column) > 0,
        (col(current_column) - col(previous_column)) / col(previous_column),
    ).otherwise(col(current_column).cast("double"))


def main() -> None:
    spark = build_spark("pizza-feature-engineering-15m")

    orders = (
        spark.read.parquet(SILVER_LINE_ITEMS_PATH)
        .select(
            col("order_id").cast("long"),
            col("pizza_id"),
            col("pizza_name"),
            col("pizza_size"),
            col("pizza_type"),
            col("quantity").cast("double"),
            col("event_time").cast("timestamp"),
            col("unit_price").cast("double"),
            col("catalog_unit_price").cast("double"),
            col("total_price").cast("double"),
        )
        .filter(
            col("event_time").isNotNull()
            & col("pizza_name").isNotNull()
            & col("pizza_size").isNotNull()
            & col("quantity").isNotNull()
        )
    )

    max_row = orders.agg(spark_max("event_time").alias("max_event_time")).first()
    if not max_row or not max_row["max_event_time"]:
        raise RuntimeError(f"No rows found in {SILVER_LINE_ITEMS_PATH}. Run ETL first.")

    if MAX_FEATURE_LOOKBACK_DAYS > 0:
        cutoff = max_row["max_event_time"] - timedelta(days=MAX_FEATURE_LOOKBACK_DAYS)
        orders = orders.filter(col("event_time") >= lit(cutoff))

    bounds = orders.agg(
        spark_min("event_time").alias("min_event_time"),
        spark_max("event_time").alias("max_event_time"),
    ).first()
    if not bounds or not bounds["min_event_time"] or not bounds["max_event_time"]:
        raise RuntimeError(f"No usable event_time rows found in {SILVER_LINE_ITEMS_PATH}.")

    start_epoch = int(bounds["min_event_time"].timestamp() // BIN_SECONDS * BIN_SECONDS)
    end_epoch = int((bounds["max_event_time"].timestamp() + BIN_SECONDS - 1) // BIN_SECONDS * BIN_SECONDS)

    time_grid = spark.range(start_epoch, end_epoch + BIN_SECONDS, BIN_SECONDS).select(
        to_timestamp(from_unixtime("id")).alias("feature_time")
    )

    pizzas = orders.select(*GROUP_COLUMNS).dropDuplicates(GROUP_COLUMNS)
    grid = pizzas.crossJoin(time_grid)

    orders_with_bin = orders.withColumn(
        "feature_time",
        to_timestamp(from_unixtime(ceil(unix_timestamp("event_time") / lit(BIN_SECONDS)) * lit(BIN_SECONDS))),
    )

    demand_15m = orders_with_bin.groupBy(*GROUP_COLUMNS, "feature_time").agg(
        spark_sum("quantity").cast("double").alias("qty_last_15m"),
        spark_sum("total_price").cast("double").alias("revenue_15m"),
        countDistinct("order_id").cast("double").alias("order_count_15m"),
    )

    static = orders.groupBy(*GROUP_COLUMNS).agg(
        coalesce(
            expr("percentile_approx(unit_price, 0.5)"),
            expr("percentile_approx(catalog_unit_price, 0.5)"),
            lit(0.0),
        ).cast("double").alias("unit_price"),
        coalesce(expr("last(pizza_type, true)"), lit("unknown")).alias("pizza_type"),
        lit(0.0).cast("double").alias("ingredient_count"),
    )

    features = (
        grid.join(demand_15m, [*GROUP_COLUMNS, "feature_time"], "left")
        .fillna({"qty_last_15m": 0.0, "revenue_15m": 0.0, "order_count_15m": 0.0})
        .join(static, GROUP_COLUMNS, "left")
        .fillna({"pizza_type": "unknown", "unit_price": 0.0, "ingredient_count": 0.0})
    )

    pizza_window = Window.partitionBy(*GROUP_COLUMNS).orderBy("feature_time")
    features = (
        features.withColumn("qty_last_30m", spark_sum("qty_last_15m").over(pizza_window.rowsBetween(-1, 0)))
        .withColumn("qty_last_1h", spark_sum("qty_last_15m").over(pizza_window.rowsBetween(-3, 0)))
        .withColumn("revenue_last_1h", spark_sum("revenue_15m").over(pizza_window.rowsBetween(-3, 0)))
        .withColumn("order_count_last_1h", spark_sum("order_count_15m").over(pizza_window.rowsBetween(-3, 0)))
        .withColumn("qty_prev_15m", coalesce(lag("qty_last_15m", 1).over(pizza_window), lit(0.0)))
        .withColumn("qty_prev_1h", coalesce(lag("qty_last_1h", 4).over(pizza_window), lit(0.0)))
        .withColumn("growth_15m_vs_prev_15m", safe_growth("qty_last_15m", "qty_prev_15m"))
        .withColumn("growth_1h_vs_prev_1h", safe_growth("qty_last_1h", "qty_prev_1h"))
        .withColumn("qty_lag_1h", coalesce(lag("qty_last_15m", 4).over(pizza_window), lit(0.0)))
        .withColumn("qty_lag_24h", coalesce(lag("qty_last_15m", 96).over(pizza_window), lit(0.0)))
        .withColumn(
            "avg_qty_same_hour_last_7d",
            (
                coalesce(lag("qty_last_1h", 96).over(pizza_window), lit(0.0))
                + coalesce(lag("qty_last_1h", 192).over(pizza_window), lit(0.0))
                + coalesce(lag("qty_last_1h", 288).over(pizza_window), lit(0.0))
                + coalesce(lag("qty_last_1h", 384).over(pizza_window), lit(0.0))
                + coalesce(lag("qty_last_1h", 480).over(pizza_window), lit(0.0))
                + coalesce(lag("qty_last_1h", 576).over(pizza_window), lit(0.0))
                + coalesce(lag("qty_last_1h", 672).over(pizza_window), lit(0.0))
            )
            / lit(7.0),
        )
        .withColumn(
            "target_quantity_next_1h",
            coalesce(lead("qty_last_15m", 1).over(pizza_window), lit(0.0))
            + coalesce(lead("qty_last_15m", 2).over(pizza_window), lit(0.0))
            + coalesce(lead("qty_last_15m", 3).over(pizza_window), lit(0.0))
            + coalesce(lead("qty_last_15m", 4).over(pizza_window), lit(0.0)),
        )
    )

    store_15m = orders_with_bin.groupBy("feature_time").agg(
        spark_sum("quantity").cast("double").alias("store_qty_15m"),
        countDistinct("order_id").cast("double").alias("store_order_count_15m"),
    )
    store_window = Window.orderBy("feature_time")
    store_features = (
        time_grid.join(store_15m, "feature_time", "left")
        .fillna({"store_qty_15m": 0.0, "store_order_count_15m": 0.0})
        .withColumn("store_total_qty_last_15m", col("store_qty_15m"))
        .withColumn("store_total_qty_last_1h", spark_sum("store_qty_15m").over(store_window.rowsBetween(-3, 0)))
        .withColumn(
            "store_order_count_last_1h",
            spark_sum("store_order_count_15m").over(store_window.rowsBetween(-3, 0)),
        )
        .select("feature_time", "store_total_qty_last_15m", "store_total_qty_last_1h", "store_order_count_last_1h")
    )

    features = (
        features.join(store_features, "feature_time", "left")
        .withColumn("hour_of_day", hour("feature_time"))
        .withColumn("day_of_week", dayofweek("feature_time") - lit(1))
        .withColumn("month", month("feature_time"))
        .withColumn("is_weekend", when(col("day_of_week").isin(5, 6), lit(1)).otherwise(lit(0)))
        .withColumn("is_lunch_time", when(col("hour_of_day").between(11, 13), lit(1)).otherwise(lit(0)))
        .withColumn("is_dinner_time", when(col("hour_of_day").between(18, 21), lit(1)).otherwise(lit(0)))
        .withColumn("feature_date_key", date_format(to_date("feature_time"), "yyyy-MM-dd"))
        .withColumn("batch_run_tag", lit(RUN_TAG))
        .withColumn("feature_generated_at", current_timestamp())
        .select(
            "feature_time",
            "pizza_name",
            "pizza_size",
            "pizza_type",
            "unit_price",
            "ingredient_count",
            "hour_of_day",
            "day_of_week",
            "month",
            "is_weekend",
            "is_lunch_time",
            "is_dinner_time",
            "qty_last_15m",
            "qty_last_30m",
            "qty_last_1h",
            "revenue_last_1h",
            "order_count_last_1h",
            "qty_prev_15m",
            "qty_prev_1h",
            "growth_15m_vs_prev_15m",
            "growth_1h_vs_prev_1h",
            "qty_lag_1h",
            "qty_lag_24h",
            "avg_qty_same_hour_last_7d",
            "store_total_qty_last_15m",
            "store_total_qty_last_1h",
            "store_order_count_last_1h",
            "target_quantity_next_1h",
            "feature_date_key",
            "batch_run_tag",
            "feature_generated_at",
        )
        .filter(col("feature_time") <= lit(bounds["max_event_time"] - timedelta(hours=1)))
        .fillna(0)
        .orderBy("feature_time", "pizza_name", "pizza_size")
    )

    save_parquet(features, GOLD_FEATURES_PATH, partition_by=["feature_date_key"])

    print("15-minute feature engineering completed.")
    print(f"features_path={GOLD_FEATURES_PATH}")
    print(f"feature_rows={features.count()}")
    print(f"min_event_time={bounds['min_event_time']}")
    print(f"max_event_time={bounds['max_event_time']}")

    spark.stop()


if __name__ == "__main__":
    main()
