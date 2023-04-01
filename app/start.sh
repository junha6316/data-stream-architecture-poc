#!/bin/bash
apt-get update && apt-get install -y sudo
rm -rf /opt/bitnami/spark/.ivy2/cache
chown -R $USER:$USER /.cache/pip
chmod -R 755 /.cache/pip
pip install -r /app/requirements.txt
sleep 10 
/opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 \
    --jars /app/jars/commons-pool2-2.0.jar,/app/jars/jedis-3.4.1.jar,/app/jars/spark-redis_2.12-3.0.0.jar,/app/jars/slf4j-api-1.7.30.jar \
    --conf spark.ui.enabled=true \
     /app/data-analysis.py
