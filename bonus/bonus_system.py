from celery import Celery
from datetime import datetime, timedelta
import json
import os

# Get environment variables of rabbitmq broker
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS')
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_PORT = os.environ.get('RABBITMQ_PORT')

app = Celery('tasks', broker=f'amqp://{RABBITMQ_USER}:{RABBITMQ_PASS}@{RABBITMQ_HOST}:{RABBITMQ_PORT}//')

# Bonus thresholds
X = 100.00  # Minimum billing value in the last 10 minutes
Y = 500.00  # Minimum billing value in the last 6 hours

# In-memory database simulation to record purchases
purchases = []

@app.task
def save_event(message: str):
    global purchases
    event = json.loads(message)
    event['timestamp'] = datetime.now()
    purchases.append(event)
    print(f"Purchase recorded: {event}")
    
    # Check if the user should receive the coupon
    user_id = event['user_id']
    if check_criteria(user_id):
        coupon = generate_coupon(user_id)
        notify_ecommerce(user_id, coupon)
        return {"status": "coupon generated", "coupon": coupon}
    else:
        return {"status": "no coupon"}

def check_criteria(user_id):
    global purchases
    now = datetime.now()
    
    # Calculate total amount spent in the last 10 minutes
    total_10_minutes = sum(purchase['amount'] for purchase in purchases if purchase['user_id'] == user_id and purchase['timestamp'] > now - timedelta(minutes=10))
    # Calculate total amount spent in the last 6 hours
    total_6_hours = sum(purchase['amount'] for purchase in purchases if purchase['user_id'] == user_id and purchase['timestamp'] > now - timedelta(hours=6))
    
    return total_10_minutes > X and total_6_hours > Y

def generate_coupon(user_id):
    # Logic to generate a unique coupon code
    coupon = f"COUPON-{user_id}-{int(datetime.now().timestamp())}"
    return coupon

def notify_ecommerce(user_id, coupon):
    # Simulate notification to e-commerce
    print(f"Notifying e-commerce: User {user_id} received the coupon {coupon}")
