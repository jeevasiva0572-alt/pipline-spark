import time
import os
import sys
from pathlib import Path

# ✅ Fix import path
project_root = Path(__file__).resolve().parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# ✅ Set JAVA_HOME for Railway / Local
if not os.environ.get("JAVA_HOME"):
    import shutil
    java_path = shutil.which("java")
    if java_path:
        real = os.path.realpath(java_path)
        java_home = os.path.dirname(os.path.dirname(real))
        os.environ["JAVA_HOME"] = java_home
        print(f"☕ JAVA_HOME set to: {java_home}", flush=True)

# 🤫 Suppress Java 17+ incubator warnings
os.environ["_JAVA_OPTIONS"] = os.environ.get("_JAVA_OPTIONS", "") + " --add-opens=java.base/java.lang=ALL-UNNAMED"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from datetime import datetime
from pyspark.sql import SparkSession
from app.db.connection import get_db_connection
from app.pipeline.runner import run_pipeline_for_api, run_pipeline_for_file, CHECK_INTERVAL

# ⏰ FILES batch time
FILES_BATCH_HOUR = 16
FILES_BATCH_MINUTE = 25


def initialize_spark():
    # 🤫 Shhh... suppress Hadoop & Incubator warnings
    spark = SparkSession.builder \
        .appName("UniversalDataCleaningPipeline") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=log4j2.properties -Dspark.ui.showConsoleProgress=false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    print("🚀 WORKER STARTED", flush=True)
    print("⚡ API → realtime | 📦 FILE → scheduled\n", flush=True)

    spark = initialize_spark()

    # ✅ Spark test
    try:
        print("🧪 Testing Spark...", flush=True)
        print("✅ Spark working:", spark.range(5).collect(), flush=True)
    except Exception as e:
        print("❌ Spark failed:", e, flush=True)
        return

    last_file_batch_date = None

    while True:
        try:
            db = get_db_connection()
            if not db:
                print("⚠ DB connection failed, retrying...")
                time.sleep(CHECK_INTERVAL)
                continue

            cursor = db.cursor(dictionary=True)
            now = datetime.now()

            # ==================================
            # ⚡ API DATA (REALTIME LOOP)
            # ==================================
            try:
                cursor.execute("""
                    SELECT id, api_url, response, file_name, file_path
                    FROM api_data
                    WHERE status='NEW'
                    ORDER BY id ASC
                """)

                api_records = cursor.fetchall()

                for record in api_records:
                    api_id = record["id"]

                    try:
                        print(f"⚙ Processing API id={api_id}")

                        cursor.execute(
                            "UPDATE api_data SET status='PROCESSING' WHERE id=%s",
                            (api_id,)
                        )
                        db.commit()

                        run_pipeline_for_api(spark, record)

                        cursor.execute(
                            "UPDATE api_data SET status='DONE' WHERE id=%s",
                            (api_id,)
                        )
                        db.commit()

                        print(f"✅ API id={api_id} done")

                    except Exception as e:
                        cursor.execute(
                            "UPDATE api_data SET status='ERROR' WHERE id=%s",
                            (api_id,)
                        )
                        db.commit()
                        print(f"❌ API id={api_id} failed:", e)

            except Exception as e:
                print("❌ API processing error:", e)

            # ==================================
            # 📦 FILES (SCHEDULED)
            # ==================================
            # Target scheduled time today
            sched_time = now.replace(hour=FILES_BATCH_HOUR, minute=FILES_BATCH_MINUTE, second=0, microsecond=0)

            if (
                now >= sched_time and
                last_file_batch_date != now.date()
            ):
                print(f"🕛 FILE BATCH STARTED (Target: {FILES_BATCH_HOUR}:{FILES_BATCH_MINUTE:02})", flush=True)

                try:
                    cursor.execute("""
                        SELECT id, file_name, file_path, table_name, file_content
                        FROM files
                        WHERE status='NEW'
                        ORDER BY id ASC
                    """)

                    files = cursor.fetchall()
                    print(f"📦 Found {len(files)} new files to process", flush=True)

                    for file_record in files:
                        file_id = file_record["id"]

                        try:
                            print(f"⚙ Processing file id={file_id}")

                            cursor.execute("""
                                UPDATE files 
                                SET status='PROCESSING'
                                WHERE id=%s
                            """, (file_id,))
                            db.commit()

                            run_pipeline_for_file(spark, file_record)

                            cursor.execute("""
                                UPDATE files 
                                SET status='DONE'
                                WHERE id=%s
                            """, (file_id,))
                            db.commit()

                            print(f"✅ File id={file_id} done")

                        except Exception as e:
                            cursor.execute("""
                                UPDATE files 
                                SET status='ERROR'
                                WHERE id=%s
                            """, (file_id,))
                            db.commit()
                            print(f"❌ File id={file_id} failed:", e)

                    last_file_batch_date = now.date()

                except Exception as e:
                    print("❌ File batch error:", e)

            cursor.close()
            db.close()

        except Exception as e:
            print("⚠ Runtime error:", e)

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()