import os
from datetime import timedelta

from pyspark.sql import Window
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    current_timestamp,
    date_format,
    dayofmonth,
    dayofweek,
    explode,
    expr,
    hour,
    lag,
    lit,
    month,
    sequence,
    max as spark_max,
    sum as spark_sum,
    to_date,
    when,
)

from common import RUN_TAG, build_spark, lakehouse_path, save_parquet


SILVER_HOURLY_DEMAND_PATH = lakehouse_path("silver", "hourly_demand")
GOLD_FEATURES_PATH = lakehouse_path("gold", "demand_features")
MAX_FEATURE_LOOKBACK_DAYS = int(os.getenv("MAX_FEATURE_LOOKBACK_DAYS", "730"))


def main() -> None:
    spark = build_spark("pizza-feature-engineering")

    hourly = (
        spark.read.parquet(SILVER_HOURLY_DEMAND_PATH)
        .select(
            col("order_hour").cast("timestamp"),
            col("pizza_id"),
            col("pizza_name"),
            col("pizza_size"),
            col("pizza_category"),
            col("quantity").cast("double"),
            col("revenue").cast("double"),
            col("order_count").cast("long"),
        )
        .filter(col("order_hour").isNotNull() & col("pizza_id").isNotNull())
    )

    max_row = hourly.agg(spark_max("order_hour").alias("max_order_hour")).first()
    if not max_row or not max_row["max_order_hour"]:
        raise RuntimeError(f"No rows found in {SILVER_HOURLY_DEMAND_PATH}. Run ETL first.")

    if MAX_FEATURE_LOOKBACK_DAYS > 0:
        cutoff = max_row["max_order_hour"] - timedelta(days=MAX_FEATURE_LOOKBACK_DAYS)
        hourly = hourly.filter(col("order_hour") >= lit(cutoff))

    bounds = hourly.agg(
        {"order_hour": "min"},
    ).withColumnRenamed("min(order_hour)", "min_order_hour")
    bounds = bounds.crossJoin(
        hourly.agg({"order_hour": "max"}).withColumnRenamed("max(order_hour)", "max_order_hour")
    )

    row = bounds.first()
    if not row or not row["min_order_hour"] or not row["max_order_hour"]:
        raise RuntimeError(f"No rows found in {SILVER_HOURLY_DEMAND_PATH}. Run ETL first.")

    hours = bounds.select(
        explode(
            sequence(
                col("min_order_hour"),
                col("max_order_hour"),
                expr("interval 1 hour"),
            )
        ).alias("order_hour")
    )

    pizzas = hourly.select(
        "pizza_id",
        "pizza_name",
        "pizza_size",
        "pizza_category",
    ).dropDuplicates(["pizza_id"])

    dense = (
        pizzas.crossJoin(hours)
        .join(
            hourly,
            on=["pizza_id", "pizza_name", "pizza_size", "pizza_category", "order_hour"],
            how="left",
        )
        .fillna({"quantity": 0.0, "revenue": 0.0, "order_count": 0})
    )

    pizza_hour = Window.partitionBy("pizza_id").orderBy("order_hour")

    features = (
        dense.withColumn("hour", hour(col("order_hour")))
        .withColumn("day_of_week", dayofweek(col("order_hour")))
        .withColumn("day_of_month", dayofmonth(col("order_hour")))
        .withColumn("month", month(col("order_hour")))
        .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), lit(1)).otherwise(lit(0)))
        .withColumn("lag_1h", coalesce(lag("quantity", 1).over(pizza_hour), lit(0.0)))
        .withColumn("lag_24h", coalesce(lag("quantity", 24).over(pizza_hour), lit(0.0)))
        .withColumn("lag_168h", coalesce(lag("quantity", 168).over(pizza_hour), lit(0.0)))
        .withColumn(
            "rolling_mean_24h",
            coalesce(avg("quantity").over(pizza_hour.rowsBetween(-24, -1)), lit(0.0)),
        )
        .withColumn(
            "rolling_sum_24h",
            coalesce(spark_sum("quantity").over(pizza_hour.rowsBetween(-24, -1)), lit(0.0)),
        )
        .withColumn(
            "rolling_mean_168h",
            coalesce(avg("quantity").over(pizza_hour.rowsBetween(-168, -1)), lit(0.0)),
        )
        .withColumn(
            "rolling_sum_168h",
            coalesce(spark_sum("quantity").over(pizza_hour.rowsBetween(-168, -1)), lit(0.0)),
        )
        .withColumn("target_quantity", col("quantity").cast("double"))
        .withColumn("feature_date_key", date_format(to_date(col("order_hour")), "yyyy-MM-dd"))
        .withColumn("batch_run_tag", lit(RUN_TAG))
        .withColumn("feature_generated_at", current_timestamp())
    )

    save_parquet(features, GOLD_FEATURES_PATH, partition_by=["feature_date_key"])

    print("Feature engineering completed.")
    print(f"features_path={GOLD_FEATURES_PATH}")
    print(f"feature_rows={features.count()}")
    print(f"min_order_hour={row['min_order_hour']}")
    print(f"max_order_hour={row['max_order_hour']}")

    spark.stop()


if __name__ == "__main__":
    main()
