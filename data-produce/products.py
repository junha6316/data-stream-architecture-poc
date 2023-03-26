from kafka import KafkaProducer
import json
import os

# Get the Kafka producer host and port from environment variables
producer_host = os.environ.get("KAFKA_PRODUCER_HOST", "localhost")
producer_port = os.environ.get("KAFKA_PRODUCER_PORT", "9092")
producer_bootstrap_servers = [f"{producer_host}:{producer_port}"]

producer = KafkaProducer(bootstrap_servers=producer_bootstrap_servers)

# Load product data from a JSON file
with open("product_data.json", "r") as f:
    product_data_list = json.load(f)

for product_data in product_data_list:
    message = json.dumps(product_data).encode("utf-8")
    producer.send("product", message)
    print(f"Sent product data: {product_data}")

producer.flush()
