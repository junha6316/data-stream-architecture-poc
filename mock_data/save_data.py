import json
import redis
import sys


def save_json_to_redis(table_name: str, file_path: str):
    try:
        with open(file_path) as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found")
        return

    try:
        r = redis.Redis(host="localhost", port=6379, db=0)
        print(
            f"Connected to Redis at {r.connection_pool.connection_kwargs['host']}:{r.connection_pool.connection_kwargs['port']}"
        )
    except redis.ConnectionError:
        print("Error: Failed to connect to Redis")
        return

    for item in data:
        try:
            item_json = json.dumps(item)
            r.hset(table_name, item["id"], item_json)
        except (KeyError, TypeError):
            print(f"Error: Invalid JSON item {item}")

    print(f"Finished saving {len(data)} items to Redis table '{table_name}'")


if __name__ == "__main__":
    # Get command-line arguments
    if len(sys.argv) != 3:
        print("Usage: python save_json_to_redis.py [table_name] [file_path]")
    else:
        table_name = sys.argv[1]
        file_path = sys.argv[2]

        # Save JSON data to Redis
        save_json_to_redis(table_name, file_path)
