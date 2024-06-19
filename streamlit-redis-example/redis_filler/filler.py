import redis
import time
import json
import random

# Redis connection
redis_client = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)

def fill_redis():
    while True:
        # Simulate adding data to Redis

        # purchases_per_minute
        data = {}
        random_hour = random.randint(0, 23)
        random_minute = random.randint(0, 59)
        random_second = random.randint(0, 59)
        window_start = f"2024-06-19 {random_hour:02d}:{random_minute:02d}:{random_second:02d}"
        window_end = f"2024-06-19 {random_hour:02d}:{random_minute + 1:02d}:{random_second:02d}"
        for store_id in range(1, 11):
            data[store_id] = [(window_start, window_end, random.randint(1, 1000))]

        # Send the data to Redis
        for store_id, values in data.items():
            for window_start, window_end, count in values:
                key = f"{store_id}"
                package = json.dumps({"window_start": window_start, "window_end": window_end, "count": count})
                redis_client.hset("purchases_per_minute", key, package)

        ###############################################################################################################

        # revenue_per_minute
        data = {}
        random_hour = random.randint(0, 23)
        random_minute = random.randint(0, 59)
        random_second = random.randint(0, 59)
        window_start = f"2024-06-19 {random_hour:02d}:{random_minute:02d}:{random_second:02d}"
        window_end = f"2024-06-19 {random_hour:02d}:{random_minute + 1:02d}:{random_second:02d}"
        for store_id in range(1, 11):
            data[store_id] = [(window_start, window_end, random.uniform(10000, 100000))]

        # Send the data to Redis
        for store_id, values in data.items():
            for window_start, window_end, value in values:
                key = f"{store_id}"
                package = json.dumps({"window_start": window_start, "window_end": window_end, "value": value})
                redis_client.hset("revenue_per_minute", key, package)

        ###############################################################################################################

        for i in range(10):
            redis_client.zadd("most_viewed_products", {f"product_{random.randint(1, 1000)}": random.randint(1, 1000)})
        redis_client.sadd("sold_out_products", f"product_{random.randint(1, 1000)}")

        print("Data added to Redis at", time.ctime())
        
        time.sleep(2)  # Parameterized sleep

if __name__ == "__main__":
    fill_redis()
