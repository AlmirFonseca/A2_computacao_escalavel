import requests
import random
import time

# URL of the Flask server
SERVER_URL = "http://localhost:5000/log"

def generate_log():
    # Generate a random log entry
    user = random.randint(1000, 9999)
    product = random.randint(100, 999)
    STIMUL_ZOOM = random.choice(["zoom_in", "zoom_out"])
    VIEW_PRODUCT = "viewed"
    log_entry = f";User;{user};{STIMUL_ZOOM};{VIEW_PRODUCT} {product}.\n"
    return log_entry

while True:
    log_entry = generate_log()
    try:
        response = requests.post(SERVER_URL, data=log_entry)
        if response.status_code != 200:
            print(f"Failed to send log: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error sending log: {e}")
    
    print(f"Sent log: {log_entry}")
    time.sleep(0.5)  # Simulate logging interval
