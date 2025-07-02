import os
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import traceback

# Load .env
load_dotenv()

class Database:
    def __init__(self):
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT"),
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD")
            )
            if self.connection_pool:
                print("✅ Connection Pool created successfully")
        except Exception as e:
            print(f"❌ Failed to initialize pool: {str(e)}")
            traceback.print_exc()
            self.connection_pool = None

    def get_connection(self):
        if not self.connection_pool:
            raise Exception("Connection pool not initialized.")
        return self.connection_pool.getconn()

    def put_connection(self, conn):
        if self.connection_pool and conn:
            self.connection_pool.putconn(conn)

    def close_all(self):
        if self.connection_pool:
            self.connection_pool.closeall()
            print("✅ Closed all DB connections")
