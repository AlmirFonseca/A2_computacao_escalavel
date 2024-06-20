import os
from dataclasses import dataclass
from random import choice, randint, random, choices, shuffle
import models
from collections import deque
from graph_user_flow import *
import time
import time
import cade_analytics
import conta_verde
import datacat
import requests
import threading
import time
import random
import json

@dataclass
class SimulationParams:
    cycle_duration: float
    num_initial_users: int
    num_initial_products: int
    qtd_stock_initial: int
    max_simultaneus_users: int
    num_new_users_per_cycle: int
    num_new_products_per_cycle: int
    store_id: str
    logs_folder: str
    requests_folder: str

class Simulation:
    params: SimulationParams
    cycle: int
    silent: bool = True

    def __init__(self, params: SimulationParams, silent: bool = True):
        self.cycle = 0
        self.params = params
        self.silent = silent
        self.users = []
        self.new_users = []
        self.products = []
        self.new_products = []
        self.stock = {}
        self.new_stock_decreases = {}
        self.purchase_orders = []
        self.new_purchase_orders = []
        self.waiting_users = deque()
        self.logged_users = deque()
        self.depuration = []
        self.log_flow = []
        self.user_flow_report = []

        self.G = G
        
        # URL of the server to publish logs to
        # self.SERVER_URL = f"http://{os.environ.get('WEBHOOK_HOST')}:{os.environ.get('WEBHOOK_PORT')}/log"
        self.SERVER_URL = f"http://localhost:5000/log"

        self.store_folder_name = f"store_{self.params.store_id}"

        # create store folder inside requests folder and logs folder
        self.logs_folder_name = self.params.logs_folder
        self.requests_folder_name = self.params.requests_folder

        # create store folder inside requests folder and logs folder
        self.logs_folder_store = f"{self.logs_folder_name}/{self.store_folder_name}"
        self.requests_folder_store = f"{self.requests_folder_name}/{self.store_folder_name}"

        if not os.path.exists(self.logs_folder_store):
            os.makedirs(self.logs_folder_store)
        if not os.path.exists(self.requests_folder_store):
            os.makedirs(self.requests_folder_store)

        config_conta_verde = {}

        config_datacat = {
            "data_path": self.logs_folder_store,
            "log_filename": "log_simulation.txt",
        }

        config_cade_analytics = {
            "data_path": self.requests_folder_store,
            "request_filename": "request_simulation.txt",
            "server_url": f"http://{os.environ.get('WEBHOOK_HOST')}:{os.environ.get('WEBHOOK_PORT')}"
        }

          # Replace with your Flask server URL
        
        self.ContaVerde = conta_verde.ContaVerde(config_conta_verde)
        self.DataCat = datacat.DataCat(config_datacat)
        self.CadeAnalytics = cade_analytics.CadeAnalytics(config_cade_analytics)

  
        # generate users at the start of the simulation
        for _ in range(self.params.num_initial_users):
            self.__generate_user()

        # generate products at the start of the simulation
        for _ in range(self.params.num_initial_products):
            self.__generate_product()

        # generate stock for the products
        for product in self.products:
            self.__generate_stock(product, self.params.qtd_stock_initial)

        self.__report_initial_cycle()
            

    
    def __report_initial_cycle(self):
        
        self.ContaVerde.add_users_to_postgresql(self.new_users)
        self.new_users = []
      
        self.ContaVerde.add_new_products_to_postgresql(self.new_products)

        self.ContaVerde.add_new_stock_to_postgresql(self.stock, self.params.store_id)

    
    def run(self):
        while True:
            self.cycle += 1

            # CONTA VERDE
            for _ in range(self.params.num_new_users_per_cycle):
                self.__generate_user()
            for _ in range(self.params.num_new_products_per_cycle):
                self.__generate_product()
            self.__generate_stock_for_new_products()
            
            # allow users to enter and perform actions on the system, after they are loged
            # DATACAT & DataAnalytics
            self.__select_waiting_users()
            self.__select_users_to_login()    

            # CONTA VERDE
            self.__update_cycle_stock()

            # DATACAT
            self.__introduct_errors_for_log()

            self.__print_status()
            self.__report_cycle()
            time.sleep(self.params.cycle_duration)
           
    def __introduct_errors_for_log(self):
        
        # choose a random number int from 0 to 5
        component_error = randint(0, 5)
        message = f";Error;{component_error}\n"
        self.__add_message_to_log(message)
        
    def __select_waiting_users(self):

        num_users = min(self.params.max_simultaneus_users, len(self.users))
        user_copy = self.users.copy()
        shuffle(user_copy)
        for i in range(num_users):
            self.waiting_users.append(user_copy[i])

        
    def __select_users_to_login(self):

        for _ in self.waiting_users.copy():
            user = self.waiting_users.popleft()
            # apply userflow to the user, without using threads
            self.__user_flow(user)


    def __user_flow(self, user):
        # let the user perform actions on the system until it reaches the EXIT node
        current_node = LOGIN
        message = f";{self.params.store_id};Audit;{user};{LOGIN}\n"
        self.__add_message_to_log(message)

        current_product_list = []
        current_product = None
        
        while current_node != EXIT:
            # select the next as the current node's successor, based on the probability of each neighbor in the edge

            current_node_neighbors = {}
            for u, v, d in G.edges(data=True):
                current_node_neighbors.setdefault(u, []).append((v, d["prob"]))
            next_node = choices(*zip(*current_node_neighbors[current_node]))[0]

            if next_node == HOME:
                self.__home(user)

            elif next_node == VIEW_PRODUCT:
                products_in_stock = [product for product in self.products.copy() if self.stock[product] > 0]
                if not products_in_stock:
                    next_node = EXIT
                else:
                    current_product = choice(products_in_stock)
                    self.__view_product(user, current_product)

            elif next_node == CART:
                current_product_list.append(current_product)
                self.__cart(user, current_product)

            elif next_node == CHECKOUT:
                self.__checkout(user, current_product_list)

            current_node = next_node
        self.__exit(user)

    def get_timestamp_string(self):
        """Returns a high-resolution timestamp string."""
        return str(time.time_ns())  # Use nanoseconds for best resolution
    
    def __add_message_to_log(self, message):
        cur_time = self.get_timestamp_string()
        self.log_flow.append(cur_time + message)

    def __add_message_to_user_flow_report(self, message):
        cur_time = self.get_timestamp_string()
        msg = cur_time + message
        self.send_message_to_server(msg)
        self.user_flow_report.append(msg)
    
    def send_message_to_server(self, message):
        # sends to the server
        try:
            print("Sending log to URL!!!!!!!!!!!!!!!!!!!!!!!!!\n\n\n\n")
            # requests.post(self.SERVER_URL, data=message.encode('utf-8'), timeout=1)
            # requests.post(f"{self.SERVER_URL}", data=message.encode('utf-8'), timeout=1)
            requests.post(self.SERVER_URL, data=message.encode('utf-8'))

        except requests.exceptions.RequestException as e:
            print(f"\nError sending log to URL: {e}\n")


    def __home(self, user):
        msg = f";{self.params.store_id};User;{user};{STIMUL_SCROLLING};{HOME}.\n"
        self.__add_message_to_user_flow_report(msg)

    def __view_product(self, user, product):
        msg = f";{self.params.store_id};User;{user};{STIMUL_ZOOM};{VIEW_PRODUCT} {product}.\n"
        self.__add_message_to_user_flow_report(msg)

    def __cart(self, user, product):
        msg = f";{self.params.store_id};User;{user};{STIMUL_CLICK};{CART} with {product}.\n"
        self.__add_message_to_user_flow_report(msg)

    def __checkout(self, user, product_list):
        msg = f";{self.params.store_id};User;{user};{STIMUL_CLICK};{CHECKOUT} with {product_list}.\n"
        self.__add_message_to_user_flow_report(msg)

        
        def add_purchase_order():
            dictionary_products = {}
            for product in product_list:
                dictionary_products[product] = dictionary_products.get(product, 0) + 1
            for product, quantity in dictionary_products.items():
                purchase_order = self.__generate_purchase_order(user, product, quantity)
                
                for _ in range(quantity):
                    mesage = f";{self.params.store_id};Audit;{user};BUY;{purchase_order.product_id}\n"
                    self.__add_message_to_log(mesage)

                self.__decrease_stock(product, quantity)
        add_purchase_order()
            
    def __exit(self, user):
        msg = f";{self.params.store_id};User;{user};{STIMUL_CLICK};{EXIT}\n"
        self.__add_message_to_user_flow_report(msg)

        msg = f";{self.params.store_id};Audit;{user};{EXIT}\n"
        self.__add_message_to_log(msg)


    def __generate_user(self):
        user = models.generate_user(self.params.store_id)
        self.users.append(user.id)
        self.new_users.append(user)

    def __generate_stock(self, product_id, quantity):
        stock_product = models.generate_stock(product_id, quantity, self.params.store_id)
        self.stock[stock_product.id_product] = stock_product.quantity

    def __generate_product(self):
        product = models.generate_product(self.params.store_id)
        self.products.append(product.id)
        self.new_products.append(product)        

    def __generate_stock_for_new_products(self):
        for product in self.new_products:
            self.__generate_stock(product.id, randint(1, 100))

    def __decrease_stock(self, product_id, quantity):
        self.new_stock_decreases[product_id] = self.new_stock_decreases.get(product_id, 0) + quantity

    def __update_cycle_stock(self):
        for product_id, quantity in self.new_stock_decreases.items():
            self.stock[product_id] -= quantity

    def __generate_purchase_order(self, user_id, product_id, quantity):
        purchase_order = models.generate_purchase_order(user_id, product_id, quantity, self.params.store_id)
        self.purchase_orders.append(purchase_order)
        self.new_purchase_orders.append(purchase_order)
        return purchase_order
    
    def __print_status(self):
        if self.silent:
            return

    def __report_cycle(self):
        
        first_half = self.user_flow_report[len(self.user_flow_report)//2:]
        self.user_flow_report = self.user_flow_report[:len(self.user_flow_report)//2]

        self.log_flow = first_half + self.log_flow

        self.DataCat.write_log(self.cycle, self.log_flow)
        self.log_flow = []

        self.CadeAnalytics.write_log(self.cycle, self.user_flow_report)

        self.user_flow_report = []

        self.ContaVerde.add_users_to_postgresql(self.new_users)
        self.new_users = []

        self.ContaVerde.add_new_products_to_postgresql(self.new_products)
        self.new_products = []

        self.ContaVerde.update_stock_in_postgresql(self.new_stock_decreases, self.stock, self.params.store_id)
        self.new_stock_decreases = {}

        self.ContaVerde.add_new_purchase_orders_to_postgresql(self.new_purchase_orders)
        self.new_purchase_orders = []