import csv

# DataCat class is a class that is responsible for managing the data of the application.
class ContaVerde:

    def __init__(self, config):
        self.config = config
        self.csv_complete_path = [f"{config['data_path']}/users.csv", f"{config['data_path']}/products.csv", f"{config['data_path']}/stock.csv", f"{config['data_path']}/purchase_orders.csv"]
        self.user_header = ["id", "name", "email", "address", "registration_date", "birth_date"]
        self.__add_user_header()
        self.product_header = ["id", "name", "image", "description", "price"]
        self.__add_product_header()
        self.stock_header = ["id_product", "quantity"]
        self.__add_stock_header()
        self.purchase_order_header = ["user_id", "product_id", "quantity", "creation_date", "payment_date", "delivery_date"]
        self.__add_purchase_order_header()
        

    def add_new_purchase_orders_to_csv(self, new_purchase_orders):
        if new_purchase_orders:
            content = [[purchase_order.user_id, purchase_order.product_id, purchase_order.quantity, purchase_order.creation_date, purchase_order.payment_date, purchase_order.delivery_date] for purchase_order in new_purchase_orders]
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
    

    def rewrite_full_stock_to_csv(self, new_stock_decreases, stock):
        if new_stock_decreases:
            content = [[product_id, quantity] for product_id, quantity in stock.items()]
            # add the first line to the content beggining
            content.insert(0, self.stock_header)
            with open(self.csv_complete_path[2], 'w', newline='') as file:
                writer = csv.writer(file, delimiter=';', lineterminator='\n')
                # if self.acquire_lock(self.csv_complete_path[2]):
                writer.writerows(content)
                # self.release_lock(self.csv_complete_path[2])

    def add_new_users_to_csv(self, new_users):
        if new_users:
            content = [[user.id, user.name, user.email, user.address, user.registration_date, user.birth_date] for user in new_users]
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
            content = [[product.id, product.name, product.image, product.description, product.price] for product in new_products]
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

