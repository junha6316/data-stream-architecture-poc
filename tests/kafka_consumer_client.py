from kafka import KafkaConsumer

# Define the Kafka consumer configuration
bootstrap_servers = "localhost:29092"
topic_name = "weather"

# Create a Kafka consumer instance
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda m: m.decode("utf-8"),
    auto_offset_reset="earliest",  # Start consuming from the beginning of the topic
    group_id=None,  # Set to None for a simple consumer, not part of a consumer group
    enable_auto_commit=False,  # Do not commit offsets automatically, since this is a simple example
)

# Consume and print messages from Kafka topic
try:
    for message in consumer:
        print(f"Received message: {message.value}")
except KeyboardInterrupt:
    print("Stopping the Kafka consumer")
finally:
    consumer.close()
