# A example of a consumer that listens to the events from rabbitmq
# and prints them to the console.
import pika
import os

# RabbitMQ connection parameters
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_PORT = os.environ.get('RABBITMQ_PORT')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS')
RABBITMQ_QUEUE = os.environ.get('RABBITMQ_QUEUE')

# Setup RabbitMQ connection and channel
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)))

# Get existing channel queue
channel = connection.channel()
channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)


def callback(ch, method, properties, body):
    print(f"Received log: {body}")

channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback)
channel.start_consuming()