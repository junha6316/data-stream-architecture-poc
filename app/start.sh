#!/bin/bash
pip install -r /app/requirements.txt
sleep 10 
/opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 --jars /app/jars/spark-redis_2.12-3.1.0-SNAPSHOT.jar --conf spark.ui.enabled=true /app/data-analysis.py
