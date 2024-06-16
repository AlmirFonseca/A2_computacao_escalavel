import psycopg2
from psycopg2 import sql
import time

import database_secrets

SQL_BASE_PATH = 'SQL/'

def create_connection():
    while True:
        try:
            connection = psycopg2.connect(**database_secrets.POSTGREE_CREDENTIALS)
            return connection
        except psycopg2.OperationalError:
            print("Database is not ready, retrying in 5 seconds...")
            time.sleep(5)

def execute_script(script_path, conn):
    # Lê o script SQL
    with open(script_path, 'r') as file:
        script = file.read()

    # Executa o script SQL
    with conn.cursor() as cursor:
        cursor.execute(script)

    # Commita as alterações
    conn.commit()

if __name__ == "__main__":
    # Conecta ao banco de dados
    conn = create_connection()

    try:
        # Executa os scripts SQL de criação de tabelas e inserção de dados
        execute_script(SQL_BASE_PATH + 'create_tables.sql', conn)
        execute_script(SQL_BASE_PATH + 'create_views.sql', conn)
        execute_script(SQL_BASE_PATH + 'insert_data.sql', conn)
    except Exception as e:
        print(e)
    finally:
        # Fecha a conexão
        conn.close()