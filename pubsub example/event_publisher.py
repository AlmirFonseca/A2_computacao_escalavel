from confluent_kafka import Producer
import confluent_kafka.admin as kafka_admin
import os
import time


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


if __name__ == '__main__':
    # Get environment variables
    BROKER_URL = os.environ.get('KAFKA_BROKER')
    create_topic(BROKER_URL, 'events')
    producer = create_producer(BROKER_URL)

    if producer:
        print('Producer created successfully')
        while True:
            print('Producing messages')
            producer.produce('events', value='Hello, Kafka!')
            producer.flush()
            time.sleep(0.1)