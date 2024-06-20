from celery import Celery
# Run with "celery -A mock_celery worker --loglevel=INFO --concurrency=10"

import redis
import json

# Redis connection
# redis_client = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)
# app = Celery('tasks', broker='pyamqp://guest@localhost//')
# localhost
app = Celery('tasks', broker='localhost', backend='redis')

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
    
    # creates a file
    with open('message.txt', 'w') as f:
        f.write(message)
    print(message, "Hello!!")
    # messagee = json.loads(message)
    return message