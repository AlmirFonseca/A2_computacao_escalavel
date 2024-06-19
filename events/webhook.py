from flask import Flask, request, jsonify
import pika
import json

app = Flask(__name__)

# RabbitMQ connection parameters
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5672
RABBITMQ_QUEUE = 'logs_queue'

# Setup RabbitMQ connection and channel
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT))
channel = connection.channel()
channel.queue_declare(queue=RABBITMQ_QUEUE)

def publish_to_rabbitmq(log_entry):
    # Publish log entry to RabbitMQ queue
    channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE, body=log_entry)
    print(f"Published log to RabbitMQ: {log_entry}")

@app.route('/log', methods=['POST'])
def log():
    log_entry = request.data.decode('utf-8')
    publish_to_rabbitmq(log_entry)
    return jsonify({"status": "received"}), 200

@app.route('/')
def index():
    return "Flask server running!"

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)
