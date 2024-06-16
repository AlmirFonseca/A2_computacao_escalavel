import psycopg2
from psycopg2 import sql

def execute_script(script_path, conn):
    with open(script_path, 'r') as file:
        script = file.read()
    with conn.cursor() as cursor:
        cursor.execute(script)
    conn.commit()

def main():
    # Configurações de conexão com o banco de dados
    conn_params = {
        "dbname": "your_db_name",
        "user": "your_username",
        "password": "your_password",
        "host": "localhost",
        "port": 5432
    }

    # Conecta ao banco de dados
    conn = psycopg2.connect(**conn_params)

    try:
        # Executa os scripts SQL
        execute_script('cade_analytics.sql', conn)
        execute_script('conta_verde.sql', conn)
        execute_script('datacat.sql', conn)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
