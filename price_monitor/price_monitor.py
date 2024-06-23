import os
import psycopg2
import json
import redis
from datetime import datetime, timedelta
import time

# Configure Redis connection
redis_client = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)

# Database connection settings
conn_params = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT')
}

def get_price_deals(months, discount_percent):
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30 * months)
    
    cursor.execute("""
        SELECT product_id, AVG(price) as average_price
        FROM conta_verde.price_history
        WHERE recorded_at BETWEEN %s AND %s
        GROUP BY product_id
    """, (start_date, end_date))
    
    avg_prices = cursor.fetchall()
    deals = []
    for product_id, avg_price in avg_prices:
        threshold_price = avg_price * (1 - discount_percent / 100)
        cursor.execute("""
            SELECT p.id, p.name, p.price
            FROM conta_verde.products p
            WHERE p.id = %s AND p.price < %s
        """, (product_id, threshold_price))
        
        product = cursor.fetchone()
        if product:
            deals.append(product)
    
    cursor.close()
    conn.close()
    return deals

def handle_message(message):
    try:
        print(" -> Received message:", message)
        data = message['data']
        if data:
            job_data = json.loads(data)
            deals = get_price_deals(job_data['time_window'], job_data['discount_percentage'])
            result = {
                'status': 'success',
                'task_name': "price_monitor_job_results",
                'deals': [{'id': deal[0], 'name': deal[1], 'price': deal[2]} for deal in deals],
                'timestamp': datetime.now().isoformat()
            }
            redis_client.publish('price_monitor_job_results', json.dumps(result))
            print(" <- Published results:", result)
    except Exception as e:
        print(f"Error processing the message: {e}")

def subscribe_to_channel():
    pubsub = redis_client.pubsub()
    pubsub.subscribe(**{'price_monitor_channel': handle_message})
    pubsub.run_in_thread(sleep_time=0.01)

if __name__ == '__main__':
    subscribe_to_channel()
    print("Real Price Monitor Service Running...")
    while True:
        pass
