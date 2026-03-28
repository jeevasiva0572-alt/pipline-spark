import os
import sys

# ✅ Add project root to sys.path (needed if this file was moved)
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

# ✅ Set JAVA_HOME for Railway (Nix-based Java path)
if not os.environ.get("JAVA_HOME"):
    import shutil
    java_path = shutil.which("java")
    if java_path:
        real = os.path.realpath(java_path)
        os.environ["JAVA_HOME"] = os.path.dirname(os.path.dirname(real))

from flask import Flask, jsonify
from app.pipeline.runner import run_pipeline_for_api, run_pipeline_for_file
from app.db.connection import get_db_connection
from datetime import datetime

app = Flask(__name__)

# ✅ Lazy Spark init — only created when a route actually needs it
_spark = None

def get_spark():
    global _spark
    if _spark is None:
        from pyspark.sql import SparkSession
        _spark = SparkSession.builder \
            .appName("PipelineAPI") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .getOrCreate()
        _spark.sparkContext.setLogLevel("WARN")
    return _spark

# -------------------------------
# Health check route
# -------------------------------
@app.route("/")
def home():
    return "🚀 Flask + Spark Pipeline Running"

# -------------------------------
# API trigger route
# -------------------------------
@app.route("/run-file")
def run_file():
    try:
        db = get_db_connection()
        cursor = db.cursor(dictionary=True)

        cursor.execute("""
            SELECT id, file_name, file_path, table_name, file_content
            FROM files
            WHERE status='NEW'
            ORDER BY id ASC
            LIMIT 1
        """)

        file_record = cursor.fetchone()

        if not file_record:
            return jsonify({"message": "No NEW files"})

        file_id = file_record["id"]

        cursor.execute("UPDATE files SET status='PROCESSING' WHERE id=%s", (file_id,))
        db.commit()

        run_pipeline_for_file(get_spark(), file_record)

        cursor.execute("UPDATE files SET status='DONE' WHERE id=%s", (file_id,))
        db.commit()

        cursor.close()
        db.close()

        return jsonify({"status": "File processed successfully"})

    except Exception as e:
        return jsonify({"error": str(e)})

# -------------------------------
# API trigger for API data
# -------------------------------
@app.route("/run-api")
def run_api():
    try:
        db = get_db_connection()
        cursor = db.cursor(dictionary=True)

        cursor.execute("""
            SELECT id, api_url, response, file_name, file_path
            FROM api_data
            WHERE status='NEW'
            ORDER BY id ASC
            LIMIT 1
        """)

        record = cursor.fetchone()

        if not record:
            return jsonify({"message": "No NEW API data"})

        api_id = record["id"]

        cursor.execute("UPDATE api_data SET status='PROCESSING' WHERE id=%s", (api_id,))
        db.commit()

        run_pipeline_for_api(get_spark(), record)

        cursor.execute("UPDATE api_data SET status='DONE' WHERE id=%s", (api_id,))
        db.commit()

        cursor.close()
        db.close()

        return jsonify({"status": "API data processed successfully"})

    except Exception as e:
        return jsonify({"error": str(e)})

# -------------------------------
# Run server
# -------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
