import pika

# Establish connection to RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare the same queue named 'logs'
channel.queue_declare(queue='logs')

# Callback function to process incoming messages
def callback(ch, method, properties, body):
    print(f"Received log message: {body.decode()}")

# Set up consuming from 'logs' queue
channel.basic_consume(queue='logs',
                      on_message_callback=callback,
                      auto_ack=True)

print('Waiting for log messages. To exit press CTRL+C')
channel.start_consuming()
