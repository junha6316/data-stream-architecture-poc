from faker import Faker
import random
import json
from kafka import KafkaProducer
import os

faker = Faker()


# Load user data from a JSON file
with open("./mock_data/user_data.json", "r") as f:
    user_data_list = json.load(f)

# Load product data from a JSON file
with open("./mock_data/product_data.json", "r") as f:
    product_data_list = json.load(f)

crm_message_list = []

for i in range(1, 1000001):
    phone_number = random.choice(user_data_list)["phone_number"]
    send_at = faker.date_time_this_month()
    message_id = random.choice([i for i in range(1, 100)])
    target_product = random.choice(product_data_list)
    target_product_id = target_product["id"]
    crm_message = {
        "id": i,
        "message_id": message_id,  # message 종류
        "phone_number": phone_number,
        "send_at": send_at.strftime("%Y-%m-%d %H:%M:%S"),
        "target_product_id": target_product_id,
    }
    message = json.dumps(crm_message).encode("utf-8")
    crm_message_list.append(crm_message)
    print(f"Generated CRM message: {crm_message}")

with open("./mock_data/crm_messages.json", "w") as f:
    json.dump(crm_message_list, f)
