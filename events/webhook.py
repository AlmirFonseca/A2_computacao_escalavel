from flask import Flask, request, jsonify
import pika
import json
from celery_worker_node import store_user_behavior

app = Flask(__name__)


@app.route('/log', methods=['POST'])
def log():
    log_entry = request.data.decode('utf-8')
    print(f"Received log entry: {log_entry}")
    response = store_user_behavior.delay(log_entry)
    print(response)
    # result = response.get()


    print("RESULT: received", response)
    
    # try to decode the result
    
    return jsonify({"status": "received"}), 200

@app.route('/')
def index():
    return "Flask server running!"

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)