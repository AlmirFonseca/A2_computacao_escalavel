import os
import redis
from celery import Celery

# Get environment variables of rabbitmq broker
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS')
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_PORT = os.environ.get('RABBITMQ_PORT')
REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PORT = os.environ.get('REDIS_PORT')


app = Celery('tasks', 
             broker=f'amqp://{RABBITMQ_USER}:{RABBITMQ_PASS}@{RABBITMQ_HOST}:{RABBITMQ_PORT}//')

# backend='redis://'+REDIS_HOST+':'+REDIS_PORT+'/0'


# Define a redis to save the messages
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)



@app.task
def save_event(message: str):
    # Save the message in redis, append the message to the list
    redis_client.rpush('events', message)
    print(f"Message saved: {message}")
