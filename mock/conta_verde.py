import csv
import random
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql
import database_secrets

# DataCat class is a class that is responsible for managing the data of the application.
class ContaVerde:

    def __init__(self, config):
        self.config = config

        self.base_data_path = config['data_path']
        self.csv_complete_path = [self.base_data_path + "/" + config['users_filename'], 
                                    self.base_data_path + "/" + config['products_filename'],
                                    self.base_data_path + "/" + config['stock_filename'],
                                    self.base_data_path + "/" + config['purchase_orders_filename']]
        self.product_header = ["id", "name", "image", "description", "price", "store_id"]
        self.__add_product_header()
        self.stock_header = ["id_product", "quantity", "store_id"]
        self.__add_stock_header()
        self.purchase_order_header = ["user_id", "product_id", "quantity", "creation_date", "payment_date", "delivery_date", "store_id"]
        self.__add_purchase_order_header()
        

    def add_new_purchase_orders_to_csv(self, new_purchase_orders):
        if new_purchase_orders:
            content = [[purchase_order.user_id, 
                        purchase_order.product_id, 
                        purchase_order.quantity, 
                        purchase_order.creation_date, 
                        purchase_order.payment_date, 
                        purchase_order.delivery_date,
                        purchase_order.store_id
                        ] for purchase_order in new_purchase_orders]
            with open(self.csv_complete_path[3], 'a') as file:
                # acquire lock and write to the file, then release the lock
                writer = csv.writer(file, delimiter=';', lineterminator='\n')
                # if self.acquire_lock(self.csv_complete_path[3]):
                writer.writerows(content)
                # self.release_lock(self.csv_complete_path[3])
                
    def __add_purchase_order_header(self):
        with open(self.csv_complete_path[3], 'w') as file:
            writer = csv.writer(file, delimiter=';', lineterminator='\n')
            writer.writerow(self.purchase_order_header)

    def add_new_stock_to_csv(self, stock):
        if stock:
            with open(self.csv_complete_path[2], 'w') as file:
                writer = csv.writer(file, delimiter=';', lineterminator='\n')
                content = [[product_id, quantity] for product_id, quantity in stock.items()]
                writer.writerows(content)

    def __add_stock_header(self):
        with open(self.csv_complete_path[2], 'w') as file:
            writer = csv.writer(file, delimiter=';', lineterminator='\n')
            writer.writerow(self.stock_header)
    

    def rewrite_full_stock_to_csv(self, new_stock_decreases, stock, store_id):
        if new_stock_decreases:
            content = [[product_id, 
                        quantity,
                        store_id
                        ] for product_id, quantity in stock.items()]
            # add the first line to the content beggining
            content.insert(0, self.stock_header)
            with open(self.csv_complete_path[2], 'w', newline='') as file:
                writer = csv.writer(file, delimiter=';', lineterminator='\n')
                # if self.acquire_lock(self.csv_complete_path[2]):
                writer.writerows(content)
                # self.release_lock(self.csv_complete_path[2])

    def add_users_to_postgresql(self, users):
        try:
            conn = psycopg2.connect(
                port = database_secrets.POSTGREE_CREDENTIALS['port'],
                dbname=database_secrets.POSTGREE_CREDENTIALS['dbname'],
                user=database_secrets.POSTGREE_CREDENTIALS['user'],
                password=database_secrets.POSTGREE_CREDENTIALS['password'],
                host=database_secrets.POSTGREE_CREDENTIALS['host']  # Using service name as host
            )
            cur = conn.cursor()

            insert_query = sql.SQL("""
                INSERT INTO conta_verde.users (id, name, email, address, registration_date, birth_date, store_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """)

            user_data = [(user.id, user.name, user.email, user.address, user.registration_date, user.birth_date, user.store_id) for user in users]

            cur.executemany(insert_query, user_data)

            conn.commit()
            cur.close()

            print("Users added successfully!")
        except Exception as e:
            print("Error:", e)

    def add_new_products(self, new_products):
        if new_products:
            content = [[product.id, 
                        product.name, 
                        product.image, 
                        product.description, 
                        product.price, 
                        product.store_id] for product in new_products]
            with open(self.csv_complete_path[1], 'a') as file:
                writer = csv.writer(file, delimiter=';', lineterminator='\n')
                # acquire lock and write to the file, then release the lock
                # if self.acquire_lock(self.csv_complete_path[1]):
                writer.writerows(content)
                # self.release_lock(self.csv_complete_path[1])

    def __add_product_header(self):
        with open(self.csv_complete_path[1], 'w') as file:
            writer = csv.writer(file, delimiter=';', lineterminator='\n')
            writer.writerow(self.product_header)