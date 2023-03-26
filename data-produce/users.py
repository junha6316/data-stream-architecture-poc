from kafka import KafkaProducer
import json
import os

# Get the Kafka producer host and port from environment variables
producer_host = os.environ.get("KAFKA_PRODUCER_HOST", "localhost")
producer_port = os.environ.get("KAFKA_PRODUCER_PORT", "9092")
producer_bootstrap_servers = [f"{producer_host}:{producer_port}"]

producer = KafkaProducer(bootstrap_servers=producer_bootstrap_servers)

# Load user data from a JSON file
with open("user_data.json", "r") as f:
    user_data_list = json.load(f)

for user_data in user_data_list:
    message = json.dumps(user_data).encode("utf-8")
    producer.send("user", message)
    print(f"Sent user data: {user_data}")

producer.flush()
