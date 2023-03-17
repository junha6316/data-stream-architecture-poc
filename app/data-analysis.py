from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.streaming import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf

from uuid import uuid1
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, udf, col
from pyspark.sql.types import StringType


# define the schema for the incoming data
schema = StructType(
    [
        StructField("device", StringType(), True),
        StructField("temp", DoubleType(), True),
        StructField("humd", DoubleType(), True),
        StructField("pres", DoubleType(), True),
    ]
)


# define a function to create UUIDs
def make_uuid():
    return str(uuid1())


# sudo로 실행시켜야함
if __name__ == "__main__":
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2",
        "org.apache.kafka:kafka-clients:3.2.1",
        "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0",
    ]

    # Create Spark session
    spark = (
        SparkSession.builder.appName("Stream Handler")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.cassandra.connection.host", "localhost")
        .getOrCreate()
    )
    # Read from Kafka
    input_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "weather")
        .load()
    )

    # Convert bytes to string and parse JSON
    raw_df = input_df.selectExpr("CAST(value AS STRING)").alias("value")
    expanded_df = raw_df.select(from_json(col("value"), schema).alias("json")).select(
        col("json.device").alias("device"),
        col("json.temp").alias("temp"),
        col("json.humd").alias("humd"),
        col("json.pres").alias("pres"),
    )

    # Group by and aggregate
    summary_df = expanded_df.groupBy("device").agg(
        avg("temp"), avg("humd"), avg("pres")
    )

    # Create a UDF that generates UUIDs
    def make_uuid():
        return str(uuid1())

    make_uuid_udf = udf(make_uuid, StringType())

    # Add UUIDs and rename columns
    summary_with_ids = (
        summary_df.withColumn("uuid", make_uuid_udf())
        .withColumnRenamed("avg(temp)", "temp")
        .withColumnRenamed("avg(humd)", "humd")
        .withColumnRenamed("avg(pres)", "pres")
    )

    # Write DataFrame to Cassandra
    query = (
        summary_with_ids.writeStream.trigger(processingTime="5 seconds")
        .foreachBatch(
            lambda batch_df, batch_id: batch_df.write.format(
                "org.apache.spark.sql.cassandra"
            )
            .options(table="weather_data", keyspace="my_keyspace")
            .mode("append")
            .save()
        )
        .outputMode("update")
        .start()
    )

    # Until KeyboardInterrupt
    query.awaitTermination()

    # wait for the query to terminate
