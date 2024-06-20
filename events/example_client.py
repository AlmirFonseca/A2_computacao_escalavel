# creates a client that sends a message to the server (webhook)
import requests

url = 'http://webhook:5000/log'
data = 'User;login;quelo_logar'


def send_log(data):
    response = requests.post(url, data=data)
    print(response.json())


send_log(data)