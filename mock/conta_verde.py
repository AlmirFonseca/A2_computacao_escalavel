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

    def add_new_purchase_orders_to_postgresql(self, new_purchase_orders):
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
                INSERT INTO conta_verde.purchase_orders (user_id, product_id, quantity, creation_date, payment_date, delivery_date, store_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """)

            purchase_order_data = [(purchase_order.user_id, 
                                    purchase_order.product_id, 
                                    purchase_order.quantity, 
                                    purchase_order.creation_date, 
                                    purchase_order.payment_date, 
                                    purchase_order.delivery_date,
                                    purchase_order.store_id
                                    ) for purchase_order in new_purchase_orders]

            cur.executemany(insert_query, purchase_order_data)

            conn.commit()
            cur.close()

            print("Purchase orders added successfully!")
        except Exception as e:
            print("Error:", e)


    def add_new_stock_to_postgresql(self, stock, store_id):
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
                INSERT INTO conta_verde.stock (product_id, quantity, store_id)
                VALUES (%s, %s, %s)
            """)

            stock_data = [(product_id, quantity, store_id) for product_id, quantity in stock.items()]

            cur.executemany(insert_query, stock_data)

            conn.commit()
            cur.close()

            print("Stock added successfully!")
        except Exception as e:
            print

    def update_stock_in_postgresql(self, new_stock_decreases, stock, store_id):
        try:
            conn = psycopg2.connect(
                port = database_secrets.POSTGREE_CREDENTIALS['port'],
                dbname=database_secrets.POSTGREE_CREDENTIALS['dbname'],
                user=database_secrets.POSTGREE_CREDENTIALS['user'],
                password=database_secrets.POSTGREE_CREDENTIALS['password'],
                host=database_secrets.POSTGREE_CREDENTIALS['host']  # Using service name as host
            )
            cur = conn.cursor()

            for product_id, quantity in new_stock_decreases.items():
                update_query = sql.SQL("""
                    UPDATE conta_verde.stock
                    SET quantity = quantity - %s
                    WHERE product_id = %s AND store_id = %s
                """)

                cur.execute(update_query, (quantity, product_id, store_id))

            conn.commit()
            cur.close()

            print("Stock updated successfully!")
        except Exception as e:
            print("Error:", e)

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

    def add_new_products_to_postgresql(self, new_products):
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
                INSERT INTO conta_verde.products (id, name, image, description, price, store_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """)

            product_data = [(product.id, product.name, product.image, product.description, product.price, product.store_id) for product in new_products]

            cur.executemany(insert_query, product_data)

            conn.commit()
            cur.close()

            print("Products added successfully!")
        except Exception as e:
            print("Error:", e)