import time
import os
import sys
from pathlib import Path

# ✅ Fix import path
project_root = Path(__file__).resolve().parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from datetime import datetime
from pyspark.sql import SparkSession
from app.db.connection import get_db_connection
from app.pipeline.runner import run_pipeline_for_api, run_pipeline_for_file, CHECK_INTERVAL

# ⏰ FILES batch time
FILES_BATCH_HOUR = 14
FILES_BATCH_MINUTE = 57


# ✅ Spark init (LOW MEMORY for Railway)
def initialize_spark():
    spark = SparkSession.builder \
        .appName("UniversalDataCleaningPipeline") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    print("🚀 WORKER STARTED")
    print("⚡ API → realtime | 📦 FILE → scheduled\n")

    spark = initialize_spark()

    # ✅ Spark test
    try:
        print("🧪 Testing Spark...")
        print("✅ Spark working:", spark.range(5).collect())
    except Exception as e:
        print("❌ Spark failed:", e)
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
            if (
                now.hour == FILES_BATCH_HOUR and
                now.minute == FILES_BATCH_MINUTE and
                last_file_batch_date != now.date()
            ):
                print("🕛 FILE BATCH STARTED")

                try:
                    cursor.execute("""
                        SELECT id, file_name, file_path, table_name, file_content
                        FROM files
                        WHERE status='NEW'
                        ORDER BY id ASC
                    """)

                    files = cursor.fetchall()

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