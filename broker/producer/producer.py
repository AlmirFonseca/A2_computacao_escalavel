import pika
import time
import random
import os

rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')

# Establish connection to RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare a queue named 'logs'
channel.queue_declare(queue='logs')

# List of possible log messages
log_messages = [
    "User login",
    "User logout",
    "User visited homepage",
    "User updated profile",
    "User made a purchase",
]

# Function to simulate producing logs
def produce_logs():
    while True:
        log_message = random.choice(log_messages)
        channel.basic_publish(exchange='',
                              routing_key='logs',
                              body=log_message)
        print(f"Sent log message: {log_message}")
        time.sleep(5)  # Simulate some delay

# Start producing logs
produce_logs()
