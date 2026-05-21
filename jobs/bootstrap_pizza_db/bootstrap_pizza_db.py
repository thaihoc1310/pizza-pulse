import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    concat_ws,
    explode,
    split,
    to_timestamp,
    trim,
)
from pyspark.sql.types import IntegerType, LongType


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "pp-postgre-postgresql")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "pizza_serving")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://pp-minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    raise RuntimeError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY are required")
INPUT_PATH = os.getenv(
    "INPUT_PATH",
    "s3a://pp-lakehouse/bronze/raw/pizza_sales.csv",
)

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

SCHEMA_DDL = [
    """
    CREATE TABLE IF NOT EXISTS orders (
        order_id BIGINT PRIMARY KEY,
        order_ts TIMESTAMP NOT NULL,
        created_at TIMESTAMP DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pizza (
        pizza_id TEXT PRIMARY KEY,
        pizza_name TEXT NOT NULL,
        pizza_size TEXT NOT NULL,
        pizza_category TEXT,
        unit_price NUMERIC(10, 2),
        created_at TIMESTAMP DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS order_items (
        order_details_id BIGINT PRIMARY KEY,
        order_id BIGINT NOT NULL REFERENCES orders(order_id),
        pizza_id TEXT NOT NULL REFERENCES pizza(pizza_id),
        quantity INT NOT NULL,
        unit_price NUMERIC(10, 2) NOT NULL,
        total_price NUMERIC(10, 2) NOT NULL,
        created_at TIMESTAMP DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ingredients (
        ingredient_id SERIAL PRIMARY KEY,
        ingredient_name TEXT UNIQUE NOT NULL,
        current_stock NUMERIC(10, 2) NOT NULL DEFAULT 100,
        created_at TIMESTAMP DEFAULT now(),
        updated_at TIMESTAMP DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pizza_ingredients (
        pizza_id TEXT NOT NULL REFERENCES pizza(pizza_id),
        ingredient_id INT NOT NULL REFERENCES ingredients(ingredient_id),
        unit_amount NUMERIC(10, 2) DEFAULT 1.0,
        PRIMARY KEY (pizza_id, ingredient_id)
    )
    """,
]


def execute_sql(spark: SparkSession, sql: str) -> None:
    jvm = spark.sparkContext._gateway.jvm
    conn = jvm.java.sql.DriverManager.getConnection(
        JDBC_URL,
        POSTGRES_USER,
        POSTGRES_PASSWORD,
    )
    try:
        stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()
    finally:
        conn.close()


def ensure_schema(spark: SparkSession) -> None:
    for ddl in SCHEMA_DDL:
        execute_sql(spark, ddl)


def write_jdbc(df, table_name: str) -> None:
    (
        df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def main() -> None:
    if not POSTGRES_PASSWORD:
        raise RuntimeError("POSTGRES_PASSWORD is required")

    spark = (
        SparkSession.builder.appName("pizza-bootstrap-db")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    raw = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(INPUT_PATH)
    )

    cleaned = (
        raw
        .withColumn("order_details_id", col("order_details_id").cast("double").cast(LongType()))
        .withColumn("order_id", col("order_id").cast("double").cast(LongType()))
        .withColumnRenamed("pizza_name_id", "pizza_id")
        .withColumn("quantity", col("quantity").cast("double").cast(IntegerType()))
        .withColumn("unit_price", col("unit_price").cast("double"))
        .withColumn("total_price", col("total_price").cast("double"))
        .withColumn(
            "order_ts",
            coalesce(
                to_timestamp(
                    concat_ws(" ", col("order_date"), col("order_time")),
                    "M/d/yyyy H:mm:ss",
                ),
                to_timestamp(
                    concat_ws(" ", col("order_date"), col("order_time")),
                    "d-M-yyyy H:mm:ss",
                ),
            ),
        )
        .select(
            "order_details_id",
            "order_id",
            "pizza_id",
            "quantity",
            "unit_price",
            "total_price",
            "pizza_size",
            "pizza_category",
            "pizza_ingredients",
            "pizza_name",
            "order_ts",
        )
    )

    orders = (
        cleaned.select("order_id", "order_ts")
        .dropDuplicates(["order_id"])
    )

    pizza = (
        cleaned.select(
            "pizza_id",
            "pizza_name",
            "pizza_size",
            "pizza_category",
            "unit_price",
        )
        .dropDuplicates(["pizza_id"])
    )

    order_items = (
        cleaned.select(
            "order_details_id",
            "order_id",
            "pizza_id",
            "quantity",
            "unit_price",
            "total_price",
        )
        .dropDuplicates(["order_details_id"])
    )

    pizza_ingredient_names = (
        cleaned.select("pizza_id", "pizza_ingredients")
        .dropDuplicates(["pizza_id"])
        .withColumn("ingredient_name", explode(split(col("pizza_ingredients"), ",")))
        .withColumn("ingredient_name", trim(col("ingredient_name")))
        .filter(col("ingredient_name") != "")
        .select("pizza_id", "ingredient_name")
        .dropDuplicates()
    )

    ingredients = (
        pizza_ingredient_names.select("ingredient_name")
        .dropDuplicates(["ingredient_name"])
    )

    ensure_schema(spark)

    # Initial load only: clear tables first to make rerun deterministic.
    execute_sql(
        spark,
        """
        TRUNCATE TABLE
            order_items,
            pizza_ingredients,
            ingredients,
            pizza,
            orders
        RESTART IDENTITY CASCADE;
        """,
    )

    write_jdbc(orders, "orders")
    write_jdbc(pizza, "pizza")
    write_jdbc(order_items, "order_items")
    write_jdbc(ingredients, "ingredients")

    # Need ingredient_id from PostgreSQL after inserting ingredients.
    pg_ingredients = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "ingredients")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    pizza_ingredients = (
        pizza_ingredient_names.join(pg_ingredients, on="ingredient_name", how="inner")
        .select("pizza_id", "ingredient_id")
        .dropDuplicates(["pizza_id", "ingredient_id"])
        .withColumn("unit_amount", col("ingredient_id") * 0 + 1.0)
    )

    write_jdbc(pizza_ingredients, "pizza_ingredients")

    print("Bootstrap pizza database completed.")
    print(f"orders: {orders.count()}")
    print(f"order_items: {order_items.count()}")
    print(f"pizza: {pizza.count()}")
    print(f"ingredients: {ingredients.count()}")
    print(f"pizza_ingredients: {pizza_ingredients.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
