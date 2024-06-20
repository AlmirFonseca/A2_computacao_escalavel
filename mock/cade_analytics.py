
# User Behavior Analytics

import requests

class CadeAnalytics:
    def __init__(self, config):
        self.config = config
        self.SERVER_URL = self.config["server_url"]
        self.cade_analytics_header = ["timestamp", "store_id", "type", "content", "extra_1", "extra_2"]

    def send_message_to_server(self, message):
        # sends to the server
        try:
            print("Sending log to URL!!!!!!!!!!!!!!!!!!!!!!!!!\n\n\n\n")
            # requests.post(self.SERVER_URL, data=message.encode('utf-8'), timeout=1)
            # requests.post(f"{self.SERVER_URL}", data=message.encode('utf-8'), timeout=1)
            requests.post(self.SERVER_URL, data=message.encode('utf-8'))

        except requests.exceptions.RequestException as e:
            print(f"\nError sending log to URL: {e}\n")


