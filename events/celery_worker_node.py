from celery import Celery
import psycopg2
# from psycopg2 import pool
import json
import os
# Run with "celery -A mock_celery worker --loglevel=INFO --concurrency=10"


# app = Celery('tasks', broker='pyamqp://guest@localhost//')
# localhost
app = Celery('tasks', broker='localhost')

# with open('config.json') as f:
#     config = json.load(f)

# Path do driver JDBC do PostgreSQL
jdbc_driver_path = "../jdbc/postgresql-42.7.3.jar"

DB_NAME = 'mydatabase'
DB_USER = 'myuser'
DB_PASSWORD = 'mypassword'
DB_HOST = 'postgres'
DB_PORT = '5432'

# Propriedades de conexão com o banco de dados
db_properties = {
    'dbname': DB_NAME,
    'user': DB_USER,
    'password': DB_PASSWORD,
    'host': DB_HOST,
    'port': DB_PORT
}

# db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, **db_properties)


@app.task
def store_user_behavior(message: str):
    """ Takes the message from the webhook and stores it in the database

    Args:
        message (str): The message from the webhook, in JSON format
    """
    print(message)
    # message = json.loads(message)

    # # Get a connection from the pool
    # conn = db_pool.getconn()

    # try:
    #     # Open a cursor to perform database operations
    #     cur = conn.cursor()

    #     # Execute a query
    #     cur.execute(
    #         "INSERT INTO webhook (shop_id, user_id, product_id, behavior, datetime) VALUES (%s, %s, %s, %s, %s)",
    #         (message['shop_id'], message['user_id'], message['product_id'], message['behavior'],
    #          message['datetime'])
    #     )

    #     # Commit the transaction
    #     conn.commit()

    # finally:
    #     # Close the cursor and the connection (returns it to the pool)
    #     if cur is not None:
    #         cur.close()
    #     db_pool.putconn(conn)