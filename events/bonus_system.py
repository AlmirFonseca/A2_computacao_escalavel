from celery import Celery, group, chain, chord
from datetime import datetime, timedelta
import json
import os
import redis
import psycopg2
from psycopg2.extras import RealDictCursor

# Get environment variables of rabbitmq broker
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS')
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_PORT = os.environ.get('RABBITMQ_PORT')
REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PORT = os.environ.get('REDIS_PORT')

# postgres
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

app = Celery('tasks', 
             broker=f'amqp://{RABBITMQ_USER}:{RABBITMQ_PASS}@{RABBITMQ_HOST}:{RABBITMQ_PORT}//', broker_connection_retry_on_startup=True)

# Bonus thresholds
X = 100.00  # Minimum billing value in the last 10 minutes
Y = 500.00  # Minimum billing value in the last 6 hours

# In-memory database simulation to record purchases
purchases = []

redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)
# remove the cache 
# redis_client.delete('product_prices')

def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

def fetch_product_prices():
    # Try to get product prices from Redis cache
    cached_product_prices = redis_client.get('product_prices')
    if cached_product_prices:
        # Convert the cached data from JSON to a dictionary
        product_prices = json.loads(cached_product_prices)
    else:
        print(f"\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n Fetching product prices from database!!!")
        # Fetch from database if not in cache
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("SELECT id, price FROM conta_verde.products")
        products = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Convert the result to a dictionary
        product_prices = {product['id']: int(product['price']) for product in products}
        
        # Cache the product prices in Redis with a TTL of 10 seconds
        redis_client.setex('product_prices', timedelta(seconds=10), json.dumps(product_prices))
    
    return product_prices

@app.task
def save_event_message(message: str):
    # Save the message in redis, append the message to the list
    redis_client.rpush('events', message)
    process_data_list(message)
    return {"status": "event saved"}

@app.task
def receive_batch_events(events_list_id):
    events = redis_client.lrange('events_bonus', 0, -1)
    redis_client.delete(events_list_id)

    for event in events:
        print(f"Event: {event}")
        process_data_list(event)
    
    return {"status": "events processed"}

@app.task
def process_data_list(item):
    
    timestamp, store, type, id_user, action, details = item.split(';')
    # Continue the processing if the action starts with BUY
    # 1719127301109864497;0_dc5578e4-e904-44d9-8de9-7264ccdb4b18;User;142688210535;CLICK;BUY-2070149575-447.
    if not details.startswith('BUY'):
        return {"status": "no BUY action"}
    
    parts = details.split('-')

    # extrract product id and price
    product_id, product_price = parts[0], parts[1]

    processed_item = {
        'timestamp': datetime.fromtimestamp(int(timestamp) / 1e9),  # Assuming microseconds
        'store_id': store,
        'user_id': id_user,
        'product': product_id,
        'amount': float(product_price)
    }
    # print("\ntype of processed itm timestamp", type(processed_item['timestamp']))
    
        
    print(f"\nitem: {processed_item}\n")
    return save_event(processed_item)
    

@app.task
def save_event(message):
    global purchases
    purchases.append(message)
    print(f"Purchase recorded: {message}")
    user_id = message['user_id']
    print(f"User ID: {user_id}")
    
    if check_criteria(user_id):
        coupon = generate_coupon(user_id)
        notify_ecommerce(user_id, coupon)
        return {"status": "coupon generated", "coupon": coupon}
    else:
        print("No coupon generated")
        return {"status": "no coupon"}

def check_criteria(user_id):
    global purchases
    now = datetime.now()

    # Calculate total amount spent in the last 10 minutes
    # total_10_minutes = sum(purchase['amount'] for purchase in purchases if purchase['user_id'] == user_id and purchase['timestamp'] > now - timedelta(seconds=10))
    total_10_minutes = 0
    for purchase in purchases:
        if purchase['user_id'] == user_id and purchase['timestamp'] > now - timedelta(seconds=10):
            total_10_minutes += purchase['amount']

    if total_10_minutes > X:
        print(f"Total amount spent in the last 10 minutes: {total_10_minutes}")
    # Calculate total amount spent in the last 6 hours
    total_6_hours = sum(purchase['amount'] for purchase in purchases if purchase['user_id'] == user_id and purchase['timestamp'] > now - timedelta(minutes=6))
    
    return total_10_minutes > X and total_6_hours > Y

def generate_coupon(user_id):
    # Logic to generate a unique coupon code
    coupon = f"COUPON-{user_id}-{int(datetime.now().timestamp())}"
    return coupon

def notify_ecommerce(user_id, coupon):
    # Simulate notification to e-commerce
    print(f"Notifying e-commerce: User {user_id} received the coupon {coupon}")
