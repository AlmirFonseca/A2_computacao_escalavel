import database_secrets
import psycopg2
import schedule
import time
from datetime import datetime


def update_prices():
    conn = None
    try:
        print("Updating prices...")
        conn = psycopg2.connect(
            port = database_secrets.POSTGREE_CREDENTIALS['port'],
            dbname=database_secrets.POSTGREE_CREDENTIALS['dbname'],
            user=database_secrets.POSTGREE_CREDENTIALS['user'],
            password=database_secrets.POSTGREE_CREDENTIALS['password'],
            host=database_secrets.POSTGREE_CREDENTIALS['host']
        )
        print("Connected to database")
        cur = conn.cursor()
        
        # execute the commands in the folder "SQL/conta_verde.sql"
        with open('update_prices.sql') as f:
            cur.execute(f.read())

        # Add the changes to the database conta_verde.price_history
        now = datetime.now()
        cur.execute("SELECT id_product, price, store_id FROM conta_verde.products")
        new_prices = cur.fetchall()

        cur.executemany("INSERT INTO conta_verde.price_history (product_id, price, store_id, recorded_at) VALUES (%s, %s, %s, %s)", [(product_id, price, store_id, now) for product_id, price, store_id in new_prices])

        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        print("Closing connection")
        if conn is not None:
            conn.close()

def job():
    update_prices()

if __name__ == '__main__':
    print("Starting the job")
    
    try:
        # Add first product prices to the history
        conn = psycopg2.connect(
                port = database_secrets.POSTGREE_CREDENTIALS['port'],
                dbname=database_secrets.POSTGREE_CREDENTIALS['dbname'],
                user=database_secrets.POSTGREE_CREDENTIALS['user'],
                password=database_secrets.POSTGREE_CREDENTIALS['password'],
                host=database_secrets.POSTGREE_CREDENTIALS['host']
            )
        
        cur = conn.cursor()
        cur.execute("DELETE FROM conta_verde.price_history")

        now = datetime.now()
        cur.execute("SELECT id_product, price, store_id FROM conta_verde.products")
        first_prices = cur.fetchall()
        cur.executemany("INSERT INTO conta_verde.price_history (product_id, price, store_id, recorded_at) VALUES (%s, %s, %s, %s)", [(product_id, price, store_id, now) for product_id, price, store_id in first_prices])

        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    # Schedule the job every 10 seconds
    schedule.every(10).seconds.do(job)

    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(1)