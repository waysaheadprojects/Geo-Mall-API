import os
import psycopg2
from dotenv import load_dotenv
import traceback
from psycopg2.extras import RealDictCursor 

# Load environment variables from .env file
load_dotenv()

class Database:
    def __init__(self):
        self.connection = None

    def connect(self):
        try:
            host = os.getenv("DB_HOST")
            port = os.getenv("DB_PORT")
            database = os.getenv("DB_NAME")
            user = os.getenv("DB_USER")
            password = os.getenv("DB_PASSWORD")

            print("🔍 Connecting to DB with:")
            print(f"Host={host}, Port={port}, DB={database}, User={user}")

            self.connection = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            print("✅ PostgreSQL database connected successfully")
        except Exception as e:
            print("❌ Failed to connect to the database:", str(e))
            traceback.print_exc()

    def get_cursor(self):
        if self.connection is None:
            self.connect()
        if self.connection:
            return self.connection.cursor(cursor_factory=RealDictCursor)
        else:
            raise Exception("🔌 Database connection not available.")

    def close(self):
        if self.connection:
            self.connection.close()
            print("🔒 PostgreSQL connection closed.")
