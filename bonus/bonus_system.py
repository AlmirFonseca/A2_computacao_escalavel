import psycopg2
import redis
from datetime import datetime, timedelta
import random
import string
import json
import os

# PostgreSQL connection parameters
conn_params = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT')
}

# Redis connection
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Bonus thresholds
X = 100.00  # Minimum billing value in the last 10 minutes
Y = 500.00  # Minimum billing value in the last 6 hours

def generate_coupon_code(length=8):
    letters_and_digits = string.ascii_letters + string.digits
    return ''.join(random.choice(letters_and_digits) for i in range(length))

def notify_web_application(user_id, coupon_code):
    # Implement notification to the web application (example print statement)
    print(f"Notifying user {user_id} with coupon code: {coupon_code}")

def check_and_apply_bonus(user_id):
    now = datetime.now()
    ten_minutes_ago = now - timedelta(minutes=10)
    six_hours_ago = now - timedelta(hours=6)
    
    # Get total purchases in the last 10 minutes
    total_last_10_minutes = 0
    purchases_last_10_minutes = redis_client.lrange(f"purchases:{user_id}", 0, -1)
    for purchase in purchases_last_10_minutes:
        purchase_data = json.loads(purchase)
        if datetime.fromisoformat(purchase_data['time']) > ten_minutes_ago:
            total_last_10_minutes += purchase_data['amount']
    
    # Get total purchases in the last 6 hours
    total_last_6_hours = 0
    purchases_last_6_hours = redis_client.lrange(f"purchases:{user_id}", 0, -1)
    for purchase in purchases_last_6_hours:
        purchase_data = json.loads(purchase)
        if datetime.fromisoformat(purchase_data['time']) > six_hours_ago:
            total_last_6_hours += purchase_data['amount']
    
    # Check if bonus conditions are met
    if total_last_10_minutes > X and total_last_6_hours > Y:
        coupon_code = generate_coupon_code()
        now = datetime.now()
        
        # Store the coupon in PostgreSQL
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO conta_verde.coupons (user_id, code, discount, creation_date, expiration_date) 
            VALUES (%s, %s, %s, %s, %s)
        """, (user_id, coupon_code, 10.00, now, now + timedelta(days=30)))
        conn.commit()
        cursor.close()
        conn.close()
        
        # Notify the web application
        notify_web_application(user_id, coupon_code)

def register_purchase(user_id, product_id, quantity):
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    
    now = datetime.now()
    cursor.execute("""
        INSERT INTO conta_verde.purchase_orders (user_id, product_id, quantity, creation_date) 
        VALUES (%s, %s, %s, %s)
    """, (user_id, product_id, quantity, now))
    conn.commit()
    
    # Get product price
    cursor.execute("SELECT price FROM conta_verde.products WHERE id = %s", (product_id,))
    price = cursor.fetchone()[0]
    amount = price * quantity
    
    cursor.close()
    conn.close()
    
    # Store purchase data in Redis
    purchase_data = {
        'time': now.isoformat(),
        'amount': amount
    }
    redis_client.lpush(f"purchases:{user_id}", json.dumps(purchase_data))

    # Trim the list to only keep the necessary time frame data
    redis_client.ltrim(f"purchases:{user_id}", 0, 1000)  # Keeps the latest 1000 purchases, adjust as needed

    # Check and apply bonus after purchase
    check_and_apply_bonus(user_id)

if __name__ == "__main__":
    # Example purchase registration
    register_purchase(1, 1, 2)
