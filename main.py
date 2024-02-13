
import psycopg2
from config import config
from sql_queries import *
from etl import *
from create_database import main as create_table_main
from etl import main as etl_main

def connection():
    conn = None
    params = config()
    print("Sucessfully connected with the postgresql ")
    
    conn = psycopg2.connect(**params)
    
    #create the cursor
    cur = conn.cursor()
    cur.close()
    
    print("Connection status:", conn.status)
    
    
def main():
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=data_modeling user=postgres password=admin123")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()
    
    
if __name__ == "__main__":
    main
    create_table_main()
    etl_main()
    
'''from create_database import main as create_table_main
from etl import main as etl_main

if __name__ == "__main__":
    create_table_main()
    etl_main()'''