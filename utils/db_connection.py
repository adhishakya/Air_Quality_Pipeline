import os
from dotenv import load_dotenv
import psycopg2
import logging

logging.basicConfig(level=logging.INFO)

def get_db_connection():
    load_dotenv()
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')

    try:
        # Establish a connection to the PostgreSQL database
        conn = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        return conn
    
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        exit()