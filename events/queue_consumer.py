import redis
import time
import os

# Connect to the Redis server
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PORT = os.environ.get('REDIS_PORT')

redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)

# Function to dequeue an item
def dequeue_item():
    item = redis_client.lpop('events')
    if item:
        print(f"Dequeued: {item.decode('utf-8')}")
    else:
        print("Queue is empty")

if __name__ == "__main__":
    while True:
        dequeue_item()
        time.sleep(1)  # Sleep for a second before checking the queue again
