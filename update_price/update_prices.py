import database_secrets
import psycopg2
import schedule
import time


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
    # Schedule the job every minute
    schedule.every().minute.do(job)

    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(1)