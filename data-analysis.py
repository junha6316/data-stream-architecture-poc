from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.streaming import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from cassandra.cluster import Cluster
from uuid import uuid1

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


if __name__ == "__main__":

    scala_version = "2.12"
    spark_version = "3.1.2"
    # TODO: Ensure match above values match the correct versions
    packages = [
        f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}",
        "org.apache.kafka:kafka-clients:3.2.1",
        "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0",
    ]

    # initialize Spark
    spark = (
        SparkSession.builder.appName("Stream Handler")
        .config("spark.cassandra.connection.host", "localhost:9042")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.jars.packages", ",".join(packages))
        .getOrCreate()
    )

    # read from Kafka
    input_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:29092")
        .option("subscribe", "weather")
        .load()
    )

    # convert from bytes to string
    raw_df = input_df.selectExpr("CAST(value AS STRING)").select(
        from_json("value", schema).alias("data")
    )

    # select the fields we want to use
    expanded_df = raw_df.selectExpr(
        "data.device", "data.temp", "data.humd", "data.pres"
    )

    # groupby and aggregate
    summary_df = (
        expanded_df.groupBy("device")
        .agg(avg("temp"), avg("humd"), avg("pres"))
        .withColumn("uuid", udf(make_uuid, StringType())())
    )

    # write to Cassandra
    query = summary_df.writeStream.outputMode("update").foreachBatch(
        lambda batch_df, batch_id: batch_df.write.format(
            "org.apache.spark.sql.cassandra"
        )
        .option("table", "weather")
        .option("keyspace", "stuff")
        .mode("append")
        .save()
    )

    # start the query
    query.start()

    # wait for the query to terminate
