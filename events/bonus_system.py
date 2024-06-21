from celery import Celery, group, chain, chord
from datetime import datetime, timedelta
import json
import os
import redis


# Get environment variables of rabbitmq broker
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS')
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_PORT = os.environ.get('RABBITMQ_PORT')
REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PORT = os.environ.get('REDIS_PORT')


app = Celery('tasks', 
             broker=f'amqp://{RABBITMQ_USER}:{RABBITMQ_PASS}@{RABBITMQ_HOST}:{RABBITMQ_PORT}//', broker_connection_retry_on_startup=True)

# Bonus thresholds
X = 100.00  # Minimum billing value in the last 10 minutes
Y = 500.00  # Minimum billing value in the last 6 hours

# In-memory database simulation to record purchases
purchases = []

redis = redis.StrictRedis(host=os.environ.get('REDIS_HOST'), port=os.environ.get('REDIS_PORT'), db=0)

@app.task
def save_event_message(message: str):
    # Save the message in redis, append the message to the list
    redis.rpush('events', message)
    print(f"Message saved: {message}")

@app.task
def receive_batch_events(events_list_id):
    print("here")
    # for message in messages:
    #     save_event(message)
    # print(f"Received batch of {events} events")
    # get the events from the redis
    # events = app.backend.get(events_list_id)
    events = redis.lrange('events_bonus', 0, -1)
    print("ok")
    # process the events in parallel
    redis.delete(events_list_id)

    # NAO ESTA ENTRANDO AQUI, PARCE QUE A LISTA DEE EVENTOS TA VAZIA

    for event in events:
        print(f"Event: {event}")
        process_data_list(event)
    # apply the result of the group to the save_event
    # chord(group_process_data_list, save_event.s()).delay()

    
    # print(f"Received batch of {events} events")
    return {"status": "events processed"}

# organize the vnts as jsjon
# @app.task
def process_data_list(item):

    timestamp, store, user, id_user_store, action, details = item.split(';')
    # 1718969722922310367;0_96f73bce-5719-480b-b0ab-20a77d9219be;User;168011577341;SCROLLING;HOME.
    # Example processing logic:
    processed_item = {
        'timestamp': timestamp,
        'store': store,
        'user': user,
        'id': id_user_store,
        'action': action,
        'details': details.strip()  # Remove trailing newline characters
    }
        
    print(f"\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n   item: {processed_item}\n\n")
    return save_event(processed_item)

@app.task
def save_event(message):
    global purchases
    purchases.append(message)
    print(f"Purchase recorded: {message}")
    user_id = message['id']
    print(f"User ID: {user_id}")
    
    # if check_criteria(user_id):
    #     coupon = generate_coupon(user_id)
    #     notify_ecommerce(user_id, coupon)
    #     return {"status": "coupon generated", "coupon": coupon}
    # else:
    #     return {"status": "no coupon"}

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
