import os
import psycopg2


def main():
    conn = None
    try:
        conn = psycopg2.connect(
            port = 5432,
            dbname="mydatabase",
            user="myuser",
            password="mypassword",
            host='postgres'  # Using service name as host
        )
        cur = conn.cursor()
        
        # execute the commands in the folder "SQL/conta_verde.sql"
        with open('SQL/conta_verde.sql') as f:
            cur.execute(f.read())


        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    main()
