import csv
import random
from datetime import datetime, timedelta

# DataCat class is a class that is responsible for managing the data of the application.
class ContaVerde:

    def __init__(self, config):
        self.config = config
        self.base_data_path = config['data_path']
        self.csv_complete_path = [self.base_data_path + "/" + config['users_filename'], 
                                    self.base_data_path + "/" + config['products_filename'],
                                    self.base_data_path + "/" + config['stock_filename'],
                                    self.base_data_path + "/" + config['purchase_orders_filename']]
        self.user_header = ["id", "name", "email", "address", "registration_date", "birth_date", "store_id"]
        self.__add_user_header()
        self.product_header = ["id", "name", "image", "description", "price", "store_id"]
        self.__add_product_header()
        self.stock_header = ["id_product", "quantity", "store_id"]
        self.__add_stock_header()
        self.purchase_order_header = ["user_id", "product_id", "quantity", "creation_date", "payment_date", "delivery_date", "store_id"]
        self.__add_purchase_order_header()
        self.__generate_inserts()
        

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

    def add_new_users_to_csv(self, new_users):
        if new_users:
            content = [[user.id, 
                        user.name,
                        user.email, 
                        user.address, 
                        user.registration_date, 
                        user.birth_date,
                        user.store_id] for user in new_users]
            with open(self.csv_complete_path[0], 'a') as file:
                writer = csv.writer(file, delimiter=';', lineterminator='\n')
                # acquire lock and write to the file, then release the lock
                # if self.acquire_lock(self.csv_complete_path[0]):
                writer.writerows(content)
                # self.release_lock(self.csv_complete_path[0])

    def __add_user_header(self):
        with open(self.csv_complete_path[0], 'w') as file:
            writer = csv.writer(file, delimiter=';', lineterminator='\n')
            writer.writerow(self.user_header)

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

    def __generate_inserts(self):
        self.__generate_users_inserts()
        self.__generate_products_inserts()
        self.__generate_stock_inserts()
        self.__generate_purchase_orders_inserts()
        
    def __generate_users_inserts(self, num):
        users = []
        for i in range(num):
            user = {
                'name': f'User {i+1}',
                'email': f'user{i+1}@example.com',
                'address': f'{i+1} Main St',
                'registration_date': (datetime.now() - timedelta(days=random.randint(0, 365))).date(),
                'birth_date': (datetime.now() - timedelta(days=random.randint(6570, 18250))).date(),  # between 18 and 50 years old
                'store_id': 1
            }
            users.append(user)

        with open(self.csv_complete_path[0], 'a') as file:
            writer = csv.writer(file, delimiter=';', lineterminator='\n')
            for user in users:
                writer.writerow([None, user['name'], user['email'], user['address'], user['registration_date'], user['birth_date'], user['store_id']])

    def __generate_products_inserts(self, num):
        products = []
        for i in range(num):
            product = {
                'name': f'Product {i+1}',
                'image': f'image_{i+1}.jpg',
                'description': f'Description for Product {i+1}',
                'price': round(random.uniform(5.0, 100.0), 2),
                'store_id': 1
            }
            products.append(product)

        with open(self.csv_complete_path[1], 'a') as file:
            writer = csv.writer(file, delimiter=';', lineterminator='\n')
            for product in products:
                writer.writerow([None, product['name'], product['image'], product['description'], product['price'], product['store_id']])

    def __generate_stock_inserts(self, num):
        stock = []
        for i in range(num):
            stock_item = {
                'product_id': i + 1,
                'quantity': random.randint(1, 50),
                'store_id': 1
            }
            stock.append(stock_item)

        with open(self.csv_complete_path[2], 'a') as file:
            writer = csv.writer(file, delimiter=';', lineterminator='\n')
            for item in stock:
                writer.writerow([item['product_id'], item['quantity'], item['store_id']])

    def __generate_purchase_orders_inserts(self, num):
        orders = []
        for i in range(num):
            order = {
                'user_id': random.randint(1, num),
                'product_id': random.randint(1, num),
                'quantity': random.randint(1, 5),
                'creation_date': (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d %H:%M:%S'),
                'payment_date': (datetime.now() - timedelta(days=random.randint(0, 29))).strftime('%Y-%m-%d %H:%M:%S'),
                'delivery_date': (datetime.now() - timedelta(days=random.randint(0, 28))).strftime('%Y-%m-%d %H:%M:%S'),
                'store_id': 1
            }
            orders.append(order)

        with open(self.csv_complete_path[3], 'a') as file:
            writer = csv.writer(file, delimiter=';', lineterminator='\n')
            for order in orders:
                writer.writerow([order['user_id'], order['product_id'], order['quantity'], order['creation_date'], order['payment_date'], order['delivery_date'], order['store_id']])

