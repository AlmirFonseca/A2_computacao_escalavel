import redis
import time
import os
import psycopg2
# import bonus_system to receive events list

from bonus_system import receive_batch_events


REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PORT = os.environ.get('REDIS_PORT')
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)

# postgres
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')

db_conn_params = {
    'dbname': DB_NAME,
    'user': DB_USER,
    'password': DB_PASSWORD,
    'host': DB_HOST
}

# def process_data_list(data_list):
#     processed_data = []
#     for item in data_list:
#         # Access each element using keys or tuple indices
#         id_number, guid, user, timestamp, action, details = item.split(';')
        
#         # Example processing logic:
#         processed_item = {
#             'id_number': id_number,
#             'guid': guid,
#             'user': user,
#             'timestamp': timestamp,
#             'action': action,
#             'details': details.strip()  # Remove trailing newline characters
#         }
        
#         processed_data.append(processed_item)
    
#     return processed_data

def send_to_bonus_system(list_events):
    print("=====================================")
    # send the list of events to the redis queue
    for event in list_events:
        redis_client.rpush('events_bonus', event)
        print(f"Event sent to bonus system: {event}")

    receive_batch_events.delay('events_bonus')

# Function to dequeue an item
def dequeue_all_itens(conn):
    # itens = []
    # while True:
    #     item = redis_client.lpop('events')
    #     if item is None:
    #         break
    #     else:
    #         print(f"Dequeued item: {item.decode('utf-8')}")
    #     itens.append(item.decode('utf-8'))

    itens = redis_client.lrange('events', 0, -1)
    redis_client.delete('events')
        
    if itens is not None:

        send_to_bonus_system(itens)

        # execute many
        cursor.executemany("INSERT INTO events (message) VALUES (%s)", [(item,) for item in itens])
        conn.commit()

        print(f"Dequeued {len(itens)} itens")
    else:
        print("No itens to dequeue")
    




if __name__ == "__main__":
    # create tables events
    try:
        conn = psycopg2.connect(**db_conn_params)
        cursor = conn.cursor()
        # drop table events if exists
        cursor.execute("DROP TABLE IF EXISTS events")
        cursor.execute("CREATE TABLE IF NOT EXISTS events (id SERIAL PRIMARY KEY, message TEXT)")
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")
        exit(1)
    while True:
        try:
            conn = psycopg2.connect(**db_conn_params)
            cursor = conn.cursor()
            print("Connected to the database")
        except Exception as e:
            print(f"Error: {e}")
            exit(1)
        

        dequeue_all_itens(conn)
        conn.close()
        time.sleep(1)  # Sleep for a second before checking the queue again
