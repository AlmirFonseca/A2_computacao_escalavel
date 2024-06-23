# log_receiver.py

import pika
import os
import time

# Get environment variables of rabbitmq broker
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS')
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_PORT = os.environ.get('RABBITMQ_PORT')


RABBIT_MQ_URL =f'amqp://{RABBITMQ_USER}:{RABBITMQ_PASS}@{RABBITMQ_HOST}:{RABBITMQ_PORT}//'


def process_log(ch, method, properties, body):
    print("=====================================================")

    print(f"!!!!!!!!!!!!!!!!!!!Received and processing cupom: {body} !!!!!!!!!!!!!!!!!!!!!!!")
    print("=====================================================")
    ch.basic_ack(delivery_tag=method.delivery_tag)

# def start_consuming():
#     while True:
#         try: 
#             connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
#             print("Trying to connect")
#             channel = connection.channel()
#             queue_name = 'cupom'
#             channel.queue_declare(queue=queue_name, durable=True)
#             channel.basic_qos(prefetch_count=1)
#             channel.basic_consume(queue=queue_name, on_message_callback=process_log)
#             print('Waiting for logs. To exit press CTRL+C')
#             channel.start_consuming()
#         except pika.exceptions.AMQPConnectionError as e:
#             print(f"Connection to RabbitMQ failed: {e}. Retrying in 5 seconds...")
#             time.sleep(5)
#         except KeyboardInterrupt:
#             print("Log receiver interrupted. Exiting.")
#             break
#         except Exception as e:
#             print(f"An error occurred: {e}. Retrying in 5 seconds...")
#             time.sleep(5)

if __name__ == '__main__':
    print("MAIN")
    # start_consuming()
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    queue_name = 'cupom'
    channel.queue_declare(queue=queue_name, durable=True)
    
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=process_log)
    
    print('Waiting for logs. To exit press CTRL+C')
    channel.start_consuming()
    print("consuming")