import mysql.connector
import os
from dotenv import load_dotenv

# =========================================
# LOAD ENV VARIABLES
# =========================================
load_dotenv()

# =========================================
# SSL CERT — resolve relative to this file
# Works on local Windows & Railway Linux
# =========================================
_APP_DIR = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_SSL = os.path.join(_APP_DIR, "..", "global-bundle.pem")

_ssl_ca = os.getenv("SSL_CA")
if not _ssl_ca or not os.path.exists(_ssl_ca):
    _ssl_ca = _DEFAULT_SSL if os.path.exists(_DEFAULT_SSL) else None

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