import psycopg2
import time

def create_connection():
    while True:
        try:
            connection = psycopg2.connect(
                dbname="postgres",
                user="myuser",
                password="mypassword",
                host="db",
                port="5432"
            )
            return connection
        except psycopg2.OperationalError:
            print("Database is not ready, retrying in 5 seconds...")
            time.sleep(5)

def create_table(connection):
    with connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processor_2_table (
                id SERIAL PRIMARY KEY,
                data TEXT
            );
        """)
        connection.commit()

def insert_data(connection):
    with connection.cursor() as cursor:
        cursor.execute("""
            INSERT INTO processor_2_table (data) VALUES (%s);
        """, ('Hello from Processor 2',))
        connection.commit()

def print_data(connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM processor_2_table;")
        rows = cursor.fetchall()
        for row in rows:
            print(row)

if __name__ == "__main__":
    conn = create_connection()
    create_table(conn)
    insert_data(conn)
    print_data(conn)
    conn.close()
