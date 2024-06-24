
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
            # check if is it a buy messagee
            # msg_buy = f";{self.params.store_id};User;{user};{STIMUL_CLICK};{BUY}-{product}-{self.products_prices[i]}.\n"
            # if message.split(";")[5].split("-")[0] == "BUY":
                # print("BUY MESSAGE", message)
                # print(f"Sending log  to URL {message}!!!!!!!!!!!!!!!!!!!!!!!!!\n\n\n\n")

            # requests.post(self.SERVER_URL, data=message.encode('utf-8'), timeout=1)
            # requests.post(f"{self.SERVER_URL}", data=message.encode('utf-8'), timeout=1)
            requests.post(self.SERVER_URL, data=message.encode('utf-8'))

        except requests.exceptions.RequestException as e:
            print(f"\nError sending log to URL: {e}\n")


