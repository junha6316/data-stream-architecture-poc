from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


class MyApp:
    def run(self):
        spark = (
            SparkSession.builder.appName("crm-analysis")
            .config("spark.redis.host", "host.docker.internal")
            .config("spark.redis.port", "6379")
            .config("spark.redis.timeout", 10000)
            .config("redis.timeout", 10000)
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("DEBUG")

        crm_message_ids = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "kafka:29092")
            .option("subscribe", "crm_message")
            .option("startingOffsets", "earliest")
            .load()
            .selectExpr(
                "CAST(key AS STRING)",
            )
            .select(col("key").cast("int").alias("message_id"))
        )

        distinct_message_ids = crm_message_ids.distinct()
        distinct_message_ids.createOrReplaceTempView(
            "distinct_message_ids"
        )  # SQL로 작업하기 위해 dataframe으로

        crm_messages = (
            spark.read.format("org.apache.spark.sql.redis")
            .option("table", "crm_messages")
            .option("infer.schema", "true")
            .option(
                "predicates",
                f"message_id IN (SELECT message_id FROM {distinct_message_ids})",
            )
            .load()
        )

        product_data = (
            spark.read.format("org.apache.spark.sql.redis")
            .option("table", "products")
            .option("infer.schema", "true")
            .option("predicates", "id IN (SELECT target_product_id FROM crm_messages)")
            .load()
            .withColumnRenamed("id", "product_id")
        )

        window_duration = "14 days"
        target_window = window(col("created_at"), window_duration)

        purchase_data = (
            spark.read.format("org.apache.spark.sql.redis")
            .option("table", "purchase_logs")
            .option("infer.schema", "true")
            .option(
                "predicates",
                f"product_id IN ({crm_messages.select('target_product_id').distinct().collect()})",
            )
            .load()
            .filter(col("created_at").between(target_window.start, target_window.end))
            .join(product_data, "product_id", "inner")
        )

        user_data = (
            spark.read.format("org.apache.spark.sql.redis")
            .option("table", "users")
            .option("infer.schema", "true")
            .option(
                "predicates",
                f"phone_number IN ({crm_messages.select('phone_number').distinct().collect()})",
            )
            .load()
            .withColumnRenamed("phone_number", "crm_phone_number")
            .withColumnRenamed("id", "user_id")
            .join(
                purchase_data.select(
                    "user_id",
                ),
                "user_id",
                "inner",
            )
            .withColumnRenamed("crm_phone_number", "phone_number")
        )

        joined_data: DataFrame = crm_messages.join(user_data, "phone_number").join(
            purchase_data, "user_id", "inner"
        )

        joined_data.show()

        crm_purchase_counts = joined_data.groupBy("target_product_id").agg(
            countDistinct(
                when(
                    col("created_at").between(target_window.start, target_window.end),
                    col("user_id"),
                )
            ).alias("crm_purchase_count"),
            sum(
                when(
                    col("created_at").between(target_window.start, target_window.end),
                    col("price"),
                )
            ).alias("crm_total_purchase_value"),
        )

        crm_purchase_counts.write.format("org.apache.spark.sql.redis").option(
            "table", "crm_purchase_counts"
        ).option("key.column", "target_product_id").mode("overwrite").save()

        query = (
            crm_purchase_counts.writeStream.format("console")
            .outputMode("complete")
            .start()
        )
        query.awaitTermination()


if __name__ == "__main__":
    MyApp().run()
