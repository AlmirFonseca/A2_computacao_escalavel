from confluent_kafka import Consumer
import os

# Function to consume messages from a Kafka topic
def consume_messages(broker_url, topic_name):
    print(f'Consuming messages from topic: {topic_name}')
    try:
        try:
            subscriber = Consumer({'bootstrap.servers': broker_url, 'group.id': 'event_subscriber'})
        except Exception as e:
            print(f'An error occurred connecting to Kafka: {e}')
            return
        
        subscriber.subscribe([topic_name])

        while True:
            message = subscriber.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                print(f'Error consuming message: {message.error()}')
            else:
                print(f'Message received: {message.value().decode()}')
    except Exception as e:
        print(f'An error occurred consuming messages: {e}')


if __name__ == '__main__':
    # Get environment variables
    BROKER_URL = os.environ.get('KAFKA_BROKER')
    consume_messages(BROKER_URL, 'events')