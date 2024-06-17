import database_secrets
import psycopg2


def main():
    conn = None
    try:
        conn = psycopg2.connect(
            port = database_secrets.POSTGREE_CREDENTIALS['port'],
            dbname=database_secrets.POSTGREE_CREDENTIALS['dbname'],
            user=database_secrets.POSTGREE_CREDENTIALS['user'],
            password=database_secrets.POSTGREE_CREDENTIALS['password'],
            host=database_secrets.POSTGREE_CREDENTIALS['host']
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
