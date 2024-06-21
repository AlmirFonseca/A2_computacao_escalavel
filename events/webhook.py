from flask import Flask, request, jsonify
from bonus_system import save_event_message
import json

app = Flask(__name__)


@app.route('/log', methods=['POST'])
def log():
    #
    log_entry = request.data.decode('utf-8')
    print(f"Received log entry: {log_entry}")
    response = save_event_message.delay(log_entry)
    print(response)
    

    print("RESULT: received", response)
    
    # try to decode the result
    
    return jsonify({"status": "received"}), 200

@app.route('/')
def index():
    return "Flask server running!"

if __name__ == "__main__":
    # add th
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)