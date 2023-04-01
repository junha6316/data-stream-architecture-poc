from kafka import KafkaProducer

# Kafka topic to produce messages to
topic = "crm_message"

# Kafka broker configuration
conf = {
    "bootstrap_servers": "localhost:29092",  # Replace with your broker's address
}

# Create Kafka producer instance
producer = KafkaProducer(**conf)

# Produce a message to the Kafka topic
producer.send(topic, key=b"key", value=b"1")

# Wait for any outstanding messages to be delivered
producer.flush()

# Close the Kafka producer instance
producer.close()
