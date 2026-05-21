import os
import math
from datetime import timedelta

from pyspark.sql import Window
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    cos,
    current_timestamp,
    date_format,
    dayofmonth,
    dayofweek,
    explode,
    expr,
    greatest,
    hour,
    lag,
    lit,
    month,
    pow as spark_pow,
    regexp_replace,
    sequence,
    sin,
    max as spark_max,
    sum as spark_sum,
    to_date,
    when,
    year as spark_year,
)

from common import RUN_TAG, build_spark, lakehouse_path, save_parquet
from feature_contract import DOUBLE_FEATURES, INTEGER_FEATURES


SILVER_HOURLY_DEMAND_PATH = lakehouse_path("silver", "hourly_demand")
GOLD_FEATURES_PATH = lakehouse_path("gold", "demand_features")
MAX_FEATURE_LOOKBACK_DAYS = int(os.getenv("MAX_FEATURE_LOOKBACK_DAYS", "730"))

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
    "Classic": 1.15,
    "Chicken": 1.06,
    "Supreme": 0.98,
    "Veggie": 0.90,
}

SIZE_WEIGHTS = {
    "S": 0.92,
    "M": 1.30,
    "L": 1.12,
    "XL": 0.18,
    "XXL": 0.06,
}

FAMILY_WEIGHTS = {
    "classic_dlx": 1.45,
    "pepperoni": 1.38,
    "bbq_ckn": 1.32,
    "thai_ckn": 1.28,
    "hawaiian": 1.25,
    "cali_ckn": 1.22,
    "four_cheese": 1.18,
    "ital_supr": 1.16,
    "spicy_ital": 1.14,
    "southw_ckn": 1.12,
    "five_cheese": 1.10,
    "mexicana": 1.08,
    "big_meat": 1.05,
    "brie_carre": 0.35,
    "the_greek": 0.65,
    "green_garden": 0.82,
}

SUPER_BOWL_FAMILIES = {"pepperoni", "classic_dlx", "bbq_ckn", "big_meat", "pep_msh_pep"}
VALENTINES_FAMILIES = {"brie_carre", "five_cheese", "four_cheese", "spinach_fet"}
SUMMER_FAMILIES = {"hawaiian", "bbq_ckn", "cali_ckn"}
GRILLING_HOLIDAY_FAMILIES = {"bbq_ckn", "cali_ckn", "hawaiian", "pepperoni"}


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


def add_generator_calendar_features(df, timestamp_col: str):
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


def add_generator_pizza_features(df, timestamp_col: str):
    df = df.withColumn(
        "pizza_family",
        coalesce(col("pizza_family"), regexp_replace(col("pizza_id"), "_(?:s|m|l|xl|xxl)$", "")),
    ).withColumn("unit_price", coalesce(col("unit_price").cast("double"), lit(16.0)))

    price_weight = spark_pow(lit(16.0) / greatest(col("unit_price"), lit(0.01)), lit(0.35))
    df = df.withColumn(
        "pizza_base_weight",
        map_value(col("pizza_category"), CATEGORY_WEIGHTS, 1.0)
        * map_value(col("pizza_size"), SIZE_WEIGHTS, 1.0)
        * map_value(col("pizza_family"), FAMILY_WEIGHTS, 1.0)
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
            col("pizza_family"),
            col("unit_price").cast("double"),
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
        "pizza_family",
        "unit_price",
    ).dropDuplicates(["pizza_id"])

    dense = (
        pizzas.crossJoin(hours)
        .join(
            hourly,
            on=["pizza_id", "pizza_name", "pizza_size", "pizza_category", "pizza_family", "unit_price", "order_hour"],
            how="left",
        )
        .fillna({"quantity": 0.0, "revenue": 0.0, "order_count": 0})
    )

    pizza_hour = Window.partitionBy("pizza_id").orderBy("order_hour")

    features = (
        add_generator_pizza_features(add_generator_calendar_features(dense, "order_hour"), "order_hour")
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

    for column in INTEGER_FEATURES:
        features = features.withColumn(column, coalesce(col(column).cast("int"), lit(0)))
    for column in DOUBLE_FEATURES:
        features = features.withColumn(column, coalesce(col(column).cast("double"), lit(0.0)))

    save_parquet(features, GOLD_FEATURES_PATH, partition_by=["feature_date_key"])

    print("Feature engineering completed.")
    print(f"features_path={GOLD_FEATURES_PATH}")
    print(f"feature_rows={features.count()}")
    print(f"min_order_hour={row['min_order_hour']}")
    print(f"max_order_hour={row['max_order_hour']}")

    spark.stop()


if __name__ == "__main__":
    main()
