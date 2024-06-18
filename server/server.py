# server.py
from flask import Flask, request, jsonify
import threading

app = Flask(__name__)

class CadeAnalyticsServer:
    def __init__(self):
        pass

    def process_log(self, log_line):
        print(f"Processed log: {log_line}")

cade_analytics_server = CadeAnalyticsServer()

@app.route('/log', methods=['POST'])
def log_data():
    content = request.json
    log_line = content.get("log_flow")

    if log_line:
        # Process each log line in a separate thread
        thread = threading.Thread(target=cade_analytics_server.process_log, args=(log_line,))
        thread.start()
        return jsonify({"status": "log data is being processed"}), 202
    else:
        return jsonify({"status": "no log data provided"}), 400

if __name__ == '__main__':
    app.run(debug=True)
