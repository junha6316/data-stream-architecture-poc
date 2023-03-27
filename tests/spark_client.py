from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create a SparkSession
spark = (
    SparkSession.builder.appName("crm-analysis")
    .config("spark.redis.host", "localhost")
    .config("spark.redis.port", "6379")
    .getOrCreate()
)
purchase_data = (
    spark.read.format("org.apache.spark.sql.redis")
    .option("table", "purchase_data")
    .option("key.column", "id")
    .option(
        "predicates",
        f"product_id IN (1)",
    )
    .load()
)


# to use redis  --packages org.apache.spark:spark-redis_2.12:2.4.5
