#!/bin/bash


pip install -r /app/requirements.txt
sleep 10 
/opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --conf spark.ui.enabled=true /app/data-analysis.py
