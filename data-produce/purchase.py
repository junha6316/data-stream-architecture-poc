from faker import Faker
import random
import json


faker = Faker()

# Load user data from a JSON file
with open("./mock_data/user_data.json", "r") as f:
    user_data_list = json.load(f)

# Load product data from a JSON file
with open("./mock_data/product_data.json", "r") as f:
    product_data_list = json.load(f)

purchase_log_list = []

for i in range(1, 100001):
    purchase_id = i
    user_id = random.choice(user_data_list)["id"]
    product = random.choice(product_data_list)
    product_id = product["id"]
    price = product["price"]
    created_at = faker.date_time_this_month()
    purchase_log = {
        "id": purchase_id,
        "user_id": user_id,
        "product_id": product_id,
        "price": price,
        "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
    }
    message = json.dumps(purchase_log).encode("utf-8")
    purchase_log_list.append(purchase_log)
    print(f"Generated purchase log: {purchase_log}")


# Save purchase logs to a JSON file
with open("purchase_log.json", "w") as f:
    json.dump(purchase_log_list, f)
