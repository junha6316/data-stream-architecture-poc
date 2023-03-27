from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create a SparkSession
spark = SparkSession.builder.appName("crm-analysis").getOrCreate()

# Define the schema for the CRM messages
crm_message_schema = StructType([StructField("message_id", IntegerType(), True)])

# Define the window duration for the analysis
window_duration = "14 days"

# Read CRM message ids from Kafka
crm_message_ids = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "crm_message")
    .option("startingOffsets", "earliest")
    .load()
    .select(col("key").cast("int").alias("message_id"))
)

# Read CRM messages from Redis and filter by message ID
crm_messages = (
    spark.read.format("org.apache.spark.sql.redis")
    .option("table", "crm_messages")
    .option("key.column", "message_id")
    .option(
        "predicates",
        f"message_id IN ({crm_message_ids.select('message_id').distinct().collect()})",
    )
    .load()
)

# Read product data from Redis and filter by target product ID
product_data = (
    spark.read.format("org.apache.spark.sql.redis")
    .option("table", "product_data")
    .option("key.column", "id")
    .option(
        "predicates",
        f"id IN ({crm_messages.select('target_product_id').distinct().collect()})",
    )
    .load()
)

# Read purchase data from Redis and filter by target product ID and purchase date
purchase_data = (
    spark.read.format("org.apache.spark.sql.redis")
    .option("table", "purchase_data")
    .option("key.column", "id")
    .option(
        "predicates",
        f"product_id IN ({crm_messages.select('target_product_id').distinct().collect()})",
    )
    .load()
    .filter(col("created_at").between(window.start, window.end))
)

# Read user data from Redis and join with purchase data
user_data = (
    spark.read.format("org.apache.spark.sql.redis")
    .option("table", "user_data")
    .option("key.column", "phone_number")
    .option(
        "predicates",
        f"phone_number IN ({crm_messages.select('phone_number').distinct().collect()})",
    )
    .load()
    .withColumnRenamed("phone_number", "crm_phone_number")
    .join(
        purchase_data.select("user_id", "crm_phone_number"), "crm_phone_number", "inner"
    )
    .withColumnRenamed("user_id", "id")
    .drop("crm_phone_number")
)

# Join CRM messages with user, product, and purchase data
joined_data = (
    crm_messages.join(user_data, "phone_number")
    .join(product_data, "id")
    .join(purchase_data, "user_id", "inner")
)

# Define the window for the analysis
window = window(col("created_at"), window_duration)

# Perform analysis on the data
crm_purchase_counts = joined_data.groupBy("target_product_id").agg(
    countDistinct(
        when(col("created_at").between(window.start, window.end), col("user_id"))
    ).alias("crm_purchase_count"),
    sum(when(col("created_at").between(window.start, window.end), col("price"))).alias(
        "crm_total_purchase_value"
    ),
)

# Write the results back to Redis
crm_purchase_counts.write.format("org.apache.spark.sql.redis").option(
    "table", "crm_purchase_counts"
).option("key.column", "target_product_id").mode("overwrite").save()

# Start the streaming query
query = crm_purchase_counts.writeStream.format("console").outputMode("complete").start()
query.awaitTermination()
