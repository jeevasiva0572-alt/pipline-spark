import mysql.connector
import os
from dotenv import load_dotenv

# =========================================
# LOAD ENV VARIABLES
# =========================================
load_dotenv()

# =========================================
# DB CONFIG FROM ENV
# =========================================
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "autocommit": True,
    "ssl_ca": os.getenv("SSL_CA")  # path to SSL cert
}

# =========================================
# CONNECTION FUNCTION
# =========================================
_db_connected_logged = False  # Print success message only once per process

def get_db_connection():
    global _db_connected_logged
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if not _db_connected_logged:
            print("✅ DB Connected Successfully")
            _db_connected_logged = True
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None