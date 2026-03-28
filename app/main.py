import os
import sys

# ✅ Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from flask import Flask, jsonify
from app.pipeline.runner import run_pipeline_for_api, run_pipeline_for_file
from app.db.connection import get_db_connection
from pyspark.sql import SparkSession
from datetime import datetime

app = Flask(__name__)

# ✅ Initialize Spark (light config)
def initialize_spark():
    spark = SparkSession.builder \
        .appName("PipelineAPI") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark

spark = initialize_spark()

# -------------------------------
# Home route
# -------------------------------
@app.route("/")
def home():
    return "🚀 Flask + Spark API Running"

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

        run_pipeline_for_file(spark, file_record)

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

        run_pipeline_for_api(spark, record)

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
    app.run(host="0.0.0.0", port=5000)