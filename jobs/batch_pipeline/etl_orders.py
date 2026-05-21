from pyspark.sql.functions import (
    col,
    countDistinct,
    current_timestamp,
    date_format,
    date_trunc,
    dayofmonth,
    dayofweek,
    hour,
    lit,
    month,
    sum as spark_sum,
    to_date,
)

from common import RUN_TAG, build_spark, lakehouse_path, read_jdbc_table, save_parquet


SILVER_LINE_ITEMS_PATH = lakehouse_path("silver", "order_line_items")
SILVER_HOURLY_DEMAND_PATH = lakehouse_path("silver", "hourly_demand")


def main() -> None:
    spark = build_spark("pizza-batch-etl-orders")

    orders = (
        read_jdbc_table(spark, "orders")
        .select(
            col("order_id").cast("long"),
            col("order_ts").cast("timestamp"),
        )
        .filter(col("order_id").isNotNull() & col("order_ts").isNotNull())
    )

    order_items = (
        read_jdbc_table(spark, "order_items")
        .select(
            col("order_details_id").cast("long"),
            col("order_id").cast("long"),
            col("pizza_id"),
            col("quantity").cast("double"),
            col("unit_price").cast("double"),
            col("total_price").cast("double"),
        )
        .filter(col("order_details_id").isNotNull() & col("order_id").isNotNull())
    )

    pizza = (
        read_jdbc_table(spark, "pizza")
        .select(
            col("pizza_id"),
            col("pizza_name"),
            col("pizza_size"),
            col("pizza_category").alias("pizza_type"),
            col("unit_price").cast("double").alias("catalog_unit_price"),
        )
        .dropDuplicates(["pizza_id"])
    )

    line_items = (
        order_items.join(orders, on="order_id", how="inner")
        .join(pizza, on="pizza_id", how="left")
        .withColumn("event_time", col("order_ts"))
        .withColumn("order_hour", date_trunc("hour", col("event_time")))
        .withColumn("order_date", to_date(col("event_time")))
        .withColumn("hour", hour(col("event_time")))
        .withColumn("day_of_week", dayofweek(col("event_time")))
        .withColumn("day_of_month", dayofmonth(col("event_time")))
        .withColumn("month", month(col("event_time")))
        .withColumn("unit_price", col("unit_price").cast("double"))
        .withColumn("total_price", col("total_price").cast("double"))
        .withColumn("quantity", col("quantity").cast("double"))
        .withColumn("batch_run_tag", lit(RUN_TAG))
        .withColumn("processed_at", current_timestamp())
        .select(
            "order_details_id",
            "order_id",
            "pizza_id",
            "pizza_name",
            "pizza_size",
            "pizza_type",
            "quantity",
            "event_time",
            "unit_price",
            "catalog_unit_price",
            "total_price",
            "order_hour",
            "order_date",
            "hour",
            "day_of_week",
            "day_of_month",
            "month",
            "batch_run_tag",
            "processed_at",
        )
    )

    if line_items.limit(1).count() == 0:
        raise RuntimeError("No order line items found in PostgreSQL.")

    hourly_demand = (
        line_items.groupBy(
            "order_hour",
            "order_date",
            "pizza_id",
            "pizza_name",
            "pizza_size",
            "pizza_type",
        )
        .agg(
            spark_sum("quantity").alias("quantity"),
            spark_sum("total_price").alias("revenue"),
            countDistinct("order_id").alias("order_count"),
        )
        .withColumn("hour", hour(col("order_hour")))
        .withColumn("day_of_week", dayofweek(col("order_hour")))
        .withColumn("day_of_month", dayofmonth(col("order_hour")))
        .withColumn("month", month(col("order_hour")))
        .withColumn("batch_run_tag", lit(RUN_TAG))
        .withColumn("processed_at", current_timestamp())
        .withColumn("order_date_key", date_format(col("order_date"), "yyyy-MM-dd"))
    )

    save_parquet(line_items, SILVER_LINE_ITEMS_PATH, partition_by=["order_date"])
    save_parquet(hourly_demand, SILVER_HOURLY_DEMAND_PATH, partition_by=["order_date_key"])

    print("Batch ETL completed.")
    print(f"line_items_path={SILVER_LINE_ITEMS_PATH}")
    print(f"hourly_demand_path={SILVER_HOURLY_DEMAND_PATH}")
    print(f"line_items={line_items.count()}")
    print(f"hourly_demand_rows={hourly_demand.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
