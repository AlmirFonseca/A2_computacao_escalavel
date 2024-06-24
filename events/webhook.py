from flask import Flask, request, jsonify
from confluent_kafka import Producer
import confluent_kafka.admin as kafka_admin
import os

app = Flask(__name__)


def create_topic(broker_url, topic_name, num_partitions=1, replication_factor=1):
    print(f'Creating topic: {topic_name}')
    try:
        admin = kafka_admin.AdminClient({'bootstrap.servers': broker_url})
        topic = kafka_admin.NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        response = admin.create_topics([topic])

        for topic, future in response.items():
            try:
                future.result()
                print(f'Topic {topic} created')
            except Exception as e:
                print(f'Failed to create topic {topic}: {e}')
    except Exception as e:
        print(f'An error occurred connecting to Kafka: {e}')


def create_producer(broker_url):
    print('Creating producer')
    try:
        producer = Producer({'bootstrap.servers': broker_url})
        return producer
    except Exception as e:
        print(f'An error occurred creating producer: {e}')
        return None


# Configurações do Kafka
BROKER_URL = os.environ.get('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = os.environ.get('OUTPUT_TOPIC')

# Criar tópico e produtor no início
create_topic(BROKER_URL, TOPIC_NAME)
producer = create_producer(BROKER_URL)


@app.route('/log', methods=['POST'])
def log():
    log_entry = request.data.decode('utf-8')
    print(f"Received log entry: {log_entry}")

    if producer:
        try:
            producer.produce(TOPIC_NAME, value=log_entry)
            producer.flush()
            print("Message sent to Kafka")
        except Exception as e:
            print(f"Failed to send message to Kafka: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500
    else:
        print("Kafka producer is not available")
        return jsonify({"status": "error", "message": "Kafka producer is not available"}), 500

    return jsonify({"status": "received"}), 200


@app.route('/')
def index():
    return "Flask server running!"


if __name__ == "__main__":
    # Iniciar o servidor Flask
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)