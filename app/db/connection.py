import mysql.connector
import os
from dotenv import load_dotenv

# =========================================
# LOAD ENV VARIABLES
# =========================================
# load_dotenv()  # Disabled - using hardcoded values

# =========================================
# STATE FLAGS
# =========================================
_db_connected_logged = False

# =========================================
# SSL CERT — resolve relative to this file
# Works on local Windows & Railway Linux
# =========================================
_APP_DIR = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_SSL = os.path.join(_APP_DIR, "..", "global-bundle.pem")

# Hardcode to look in the app/ dir where it exists
_ssl_ca = _DEFAULT_SSL if os.path.exists(_DEFAULT_SSL) else "./global-bundle.pem"

# =========================================
# DB CONFIG - HARDCODED VALUES
# =========================================
DB_CONFIG = {
    "host": "cloud360-db.czz9oknmols5.us-east-1.rds.amazonaws.com",
    "port": 3306,
    "user": "cloud360_main",
    "password": "12345678",
    "database": "file_upload_db",
    "autocommit": True,
}

# Only add ssl_ca if the cert file exists
if _ssl_ca:
    DB_CONFIG["ssl_ca"] = _ssl_ca

# =========================================
# CONNECTION FUNCTION
# =========================================
def get_db_connection():
    global _db_connected_logged
    
    # Check for missing config (Railway dashboard issue)
    if not DB_CONFIG["host"]:
        print("❌ CRITICAL: DB_HOST environment variable is missing!")
        return None
        
    try:
        # Debug: show what we are trying to connect to
        if not _db_connected_logged:
            print(f"📡 Attempting to connect to DB: {DB_CONFIG['host']}")
            
        conn = mysql.connector.connect(**DB_CONFIG)
        if not _db_connected_logged:
            print("✅ DB Connected Successfully")
            _db_connected_logged = True
        return conn
    except Exception as e:
        print(f"❌ Database connection failed at {DB_CONFIG['host']}: {e}")
        return None