import os
import time
from confluent_kafka import Consumer, Producer
import psycopg2
import redis
import threading

# Configuração do Redis
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = os.environ.get('REDIS_PORT', 6379)
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)

# Configuração do PostgreSQL
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

db_conn_params = {
    'dbname': DB_NAME,
    'user': DB_USER,
    'password': DB_PASSWORD,
    'host': DB_HOST,
    'port': DB_PORT
}

# Configuração do Kafka
BROKER_URL = os.environ.get('KAFKA_BROKER', 'localhost:9092')
INPUT_TOPIC = os.environ.get('INPUT_TOPIC')
OUTPUT_TOPIC = os.environ.get('OUTPUT_TOPIC')

def create_producer(broker_url):
    try:
        producer = Producer({'bootstrap.servers': broker_url})
        return producer
    except Exception as e:
        print(f'An error occurred creating producer: {e}')
        return None

producer = create_producer(BROKER_URL)

# Função para consumir mensagens de um tópico Kafka
def consume_messages(broker_url, topic_name):
    print(f'Consuming messages from topic: {topic_name}')
    try:
        consumer = Consumer({
            'bootstrap.servers': broker_url,
            'group.id': 'event_subscriber',
            'auto.offset.reset': 'earliest'
        })
        
        consumer.subscribe([topic_name])

        while True:
            message = consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                print(f'Error consuming message: {message.error()}')
            else:
                msg_value = message.value().decode()
                print(f'Message received: {msg_value}')
                cache_message(msg_value)
    except Exception as e:
        print(f'An error occurred consuming messages: {e}')
    finally:
        consumer.close()

def cache_message(message):
    try:
        redis_client.rpush('events_cache', message)
        print("Message cached in Redis")
    except Exception as e:
        print(f'Error caching message in Redis: {e}')

def dequeue_all_itens(conn, producer, output_topic):
    items = redis_client.lrange('events_cache', 0, -1)
    redis_client.delete('events_cache')

    if items:
        messages = [item.decode('utf-8') for item in items]

        try:
            cursor = conn.cursor()
            cursor.executemany("INSERT INTO events (message) VALUES (%s)", [(msg,) for msg in messages])
            conn.commit()
            cursor.close()
            print(f"Inserted {len(messages)} items into database")
        except Exception as e:
            print(f'Error inserting into database: {e}')
            return
        
        try:
            producer.produce(output_topic, value=str(messages))
            producer.flush()
            print(f"Sent batch of {len(messages)} messages to Kafka topic '{output_topic}'")
        except Exception as e:
            print(f'Error sending messages to Kafka: {e}')
    else:
        print("No items to dequeue")

if __name__ == '__main__':
    # Criação da tabela 'events' no PostgreSQL
    try:
        conn = psycopg2.connect(**db_conn_params)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS events")
        cursor.execute("CREATE TABLE IF NOT EXISTS events (id SERIAL PRIMARY KEY, message TEXT)")
        conn.commit()
        cursor.close()
        conn.close()
        print("Database setup complete")
    except Exception as e:
        print(f"Error setting up database: {e}")
        exit(1)

    # Iniciar consumo de mensagens
    kafka_consumer_thread = threading.Thread(target=consume_messages, args=(BROKER_URL, INPUT_TOPIC))
    kafka_consumer_thread.start()

    # Processamento em batch do Redis para PostgreSQL e Kafka
    while True:
        try:
            conn = psycopg2.connect(**db_conn_params)
            print("Connected to the database")
            dequeue_all_itens(conn, producer, OUTPUT_TOPIC)
            conn.close()
        except Exception as e:
            print(f'Error: {e}')
        
        time.sleep(5)  # Sleep for 5 seconds before checking the queue again
