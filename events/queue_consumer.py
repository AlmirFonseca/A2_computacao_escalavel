import redis
import time
import os
import psycopg2

# Connect to the Redis server
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PORT = os.environ.get('REDIS_PORT')

# postgres
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_HOST = os.environ.get('DB_HOST')

redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)
db_conn_params = {
    'dbname': DB_NAME,
    'user': DB_USER,
    'password': DB_PASSWORD,
    'host': DB_HOST,
    'port': 5432
}


# Function to dequeue an item
def dequeue_all_itens(conn):
    itens = []
    while True:
        item = redis_client.lpop('events')
        if item is None:
            break
        else:
            print(f"Dequeued item: {item.decode('utf-8')}")
        itens.append(item.decode('utf-8'))
        
    if itens is not None:
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
