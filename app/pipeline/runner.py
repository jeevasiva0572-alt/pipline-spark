import os
import re
import json
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, 
    DoubleType, FloatType, TimestampType, DateType
)
from pyspark.sql.functions import countDistinct, col, to_date, coalesce, to_timestamp, when, monotonically_increasing_id
import pandas as pd
from dotenv import load_dotenv

# =========================================
# CONFIG
# =========================================
load_dotenv()

# On Railway: no local disk — use /tmp for any file output
# On local: uses BASE_UPLOAD_DIR from .env
_DEFAULT_UPLOAD_DIR = "/tmp/pipeline_uploads"
BASE_UPLOAD_DIR = os.getenv("BASE_UPLOAD_DIR") or _DEFAULT_UPLOAD_DIR
UPLOADS_DIR = os.path.join(BASE_UPLOAD_DIR, "uploads")
PROCESSED_DIR = os.path.join(BASE_UPLOAD_DIR, "uploads", "processed")
CHECK_INTERVAL = 1

# -----------------------------------------
# Helpers
# -----------------------------------------
def _clean_column_name(col: str) -> str:
    col = str(col).strip()
    col = re.sub(r"\s+", "_", col)
    col = re.sub(r"[^0-9A-Za-z_]+", "", col)
    col = re.sub(r"_+", "_", col)
    return col.lower() or "col"

def is_id_column(col_name: str):
    return any(
        k in col_name.lower()
        for k in ["id", "order", "invoice", "txn", "match", "ref", "no"]
    )

def drop_technical_cols(df: DataFrame):
    tech_cols = ["_raw_row_seq"]
    for c in tech_cols:
        if c in df.columns:
            df = df.drop(c)
    return df

def safe_makedirs(path):
    os.makedirs(path, exist_ok=True)

def normalize_date_or_datetime(df: DataFrame, col_name: str):
    return df.withColumn(
        col_name,
        to_date(
            coalesce(
                to_timestamp(col(col_name), "yyyy-MM-dd"),
                to_timestamp(col(col_name), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(col(col_name), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                to_timestamp(col(col_name), "MM/dd/yyyy HH:mm"),
                to_timestamp(col(col_name), "dd/MM/yyyy"),
                to_timestamp(col(col_name), "MM/dd/yyyy"),
                to_timestamp(col(col_name), "yyyy/MM/dd"),
                to_timestamp(col(col_name), "dd-MM-yyyy HH:mm:ss"),
                to_timestamp(col(col_name), "dd-MM-yyyy"),
                to_timestamp(col(col_name), "MM-dd-yyyy"),
                to_timestamp(col(col_name), "dd-MM, yyyy"),
                to_timestamp(col(col_name), "dd/MM, yyyy"),
                to_timestamp(col(col_name), "dd MMM yyyy"),
                to_timestamp(col(col_name), "MMM dd yyyy"),
                to_timestamp(col(col_name), "MMM d, yyyy hh:mm a"),
                to_timestamp(col(col_name), "MMMM dd, yyyy"),
                to_timestamp(col(col_name), "MMM d, yyyy"),
                to_timestamp(col(col_name), "MMMM d, yyyy")
            )
        )
    )

def flatten_json_record(record, parent_key="", sep="_"):
    items = {}
    for k, v in record.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_json_record(v, new_key, sep))
        elif isinstance(v, list):
            items[new_key] = v
        else:
            items[new_key] = v
    return items

def recursive_explode(df):
    while True:
        array_cols = [c for c in df.columns if df[c].apply(lambda x: isinstance(x, list)).any()]
        if not array_cols:
            break
        col = array_cols[0]
        df = df.explode(col, ignore_index=True)
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            expanded = pd.json_normalize(df[col])
            expanded.columns = [f"{col}_{c}" for c in expanded.columns]
            df = pd.concat([df.drop(columns=[col]), expanded], axis=1)
    return df

def find_records(json_data):
    if isinstance(json_data, list):
        return json_data
    if isinstance(json_data, dict):
        for key in ["data", "results", "items", "rows", "records", "response"]:
            if key in json_data and isinstance(json_data[key], list):
                return json_data[key]
        return [json_data]
    return [{"value": json_data}]

def auto_flatten_json_to_pandas(json_data):
    if isinstance(json_data, (str, bytes)):
        json_data = json.loads(json_data)
    records = find_records(json_data)
    flattened_rows = []
    for r in records:
        if isinstance(r, dict):
            flattened_rows.append(flatten_json_record(r))
        else:
            flattened_rows.append({"value": r})
    df = pd.DataFrame(flattened_rows)
    df = recursive_explode(df)
    df.columns = [_clean_column_name(c) for c in df.columns]
    return df

class PySparkAdvancedCleaner:
    def __init__(self, numeric_impute_strategy='median', date_threshold=0.6,
                 numeric_threshold=0.8, outlier_iqr_factor=1.5,
                 drop_empty_columns=True, id_name_tokens=None):
        self.numeric_impute_strategy = numeric_impute_strategy
        self.date_threshold = date_threshold
        self.numeric_threshold = numeric_threshold
        self.outlier_iqr_factor = outlier_iqr_factor
        self.drop_empty_columns = drop_empty_columns
        self.id_name_tokens = id_name_tokens or ['id', 'code', 'no', 'num', 'ref', 'key', 'uid', 'match']

    def normalize_columns(self, df: DataFrame):
        new_cols = [_clean_column_name(c) for c in df.columns]
        seen = {}
        final_cols = []
        for c in new_cols:
            if c in seen:
                seen[c] += 1
                final_cols.append(f"{c}_{seen[c]}")
            else:
                seen[c] = 0
                final_cols.append(c)
        for old_name, new_name in zip(df.columns, final_cols):
            if old_name != new_name:
                df = df.withColumnRenamed(old_name, new_name)
        return df

    def remove_noise_columns(self, df: DataFrame):
        drop_cols = []
        for c in df.columns:
            if c.startswith('unnamed') or c.startswith('index'):
                drop_cols.append(c)
                continue
            if self.drop_empty_columns:
                null_count = df.filter(F.col(c).isNull()).count()
                if null_count == df.count():
                    drop_cols.append(c)
        if drop_cols:
            df = df.drop(*drop_cols)
        return df

    def clean_text_columns(self, df: DataFrame):
        for c in df.columns:
            col_type = df.schema[c].dataType
            if isinstance(col_type, StringType):
                df = df.withColumn(c, F.regexp_replace(F.col(c), r"[\x00-\x1f\x7f-\x9f]", ""))
                df = df.withColumn(c, F.trim(F.col(c)))
                df = df.withColumn(c, F.regexp_replace(F.col(c), r'^"(.*)"$', r'$1'))
                df = df.withColumn(c, F.when(F.col(c).isin(['nan', 'None', '']), None).otherwise(F.col(c)))
                df = df.withColumn(c, F.regexp_replace(F.col(c), r"\s+", " "))
                if "date" in c.lower():
                    df = normalize_date_or_datetime(df, c)
                if not any(k in c.lower() for k in ["id", "code", "no", "ref"]):
                    df = df.withColumn(c, F.initcap(F.col(c)))
        return df

    def _infer_column_type(self, df: DataFrame, col_name: str):
        sample = df.select(col_name).limit(1000).collect()
        values = [row[0] for row in sample if row[0] is not None]
        if not values:
            return 'string'
        numeric_count = 0
        for val in values:
            try:
                str_val = str(val).strip().replace(',', '').replace('₹', '').replace('$', '').replace('€', '').replace('£', '')
                float(str_val)
                numeric_count += 1
            except:
                pass
        numeric_ratio = numeric_count / len(values)
        date_count = 0
        for val in values:
            try:
                str_val = str(val).strip()
                from dateutil import parser
                parser.parse(str_val)
                date_count += 1
            except:
                pass
        date_ratio = date_count / len(values)
        if numeric_ratio >= self.numeric_threshold:
            all_int = all(float(str(v).replace(',', '')).is_integer()
                          for v in values if str(v).replace(',', '').replace('.', '').isdigit())
            return 'int' if all_int else 'float'
        elif date_ratio >= self.date_threshold:
            return 'datetime'
        else:
            return 'string'

    def convert_column_types(self, df: DataFrame):
        converted = {}
        for c in df.columns:
            col_type = self._infer_column_type(df, c)
            if col_type == 'int':
                df = df.withColumn(c, F.regexp_replace(F.col(c).cast(StringType()), r"[,₹$€£]", ""))
                df = df.withColumn(c, F.col(c).cast(LongType()))
                converted[c] = 'int'
            elif col_type == 'float':
                df = df.withColumn(c, F.regexp_replace(F.col(c).cast(StringType()), r"[,₹$€£]", ""))
                df = df.withColumn(c, F.col(c).cast(DoubleType()))
                converted[c] = 'float'
            elif col_type == 'datetime':
                converted[c] = 'date'
            else:
                df = df.withColumn(c, F.col(c).cast(StringType()))
                converted[c] = 'string'
        return df, converted

    def handle_missing_values(self, df: DataFrame):
        for c in df.columns:
            col_type = df.schema[c].dataType
            if is_id_column(c):
                continue
            if isinstance(col_type, (IntegerType, LongType, DoubleType, FloatType)):
                fill_value = df.approxQuantile(c, [0.5], 0.01)[0]
                if fill_value is not None:
                    df = df.withColumn(c, F.coalesce(F.col(c), F.lit(fill_value)))
            elif isinstance(col_type, TimestampType):
                mode_val = df.groupBy(c).count().orderBy(F.desc("count")).first()
                if mode_val and mode_val[0] is not None:
                    df = df.withColumn(c, F.coalesce(F.col(c), F.lit(mode_val[0])))
            else:
                non_null_ratio = df.filter(F.col(c).isNotNull()).count() / df.count()
                if non_null_ratio > 0.5:
                    mode_val = df.groupBy(c).count().orderBy(F.desc("count")).first()
                    if mode_val and mode_val[0] is not None:
                        df = df.withColumn(c, F.coalesce(F.col(c), F.lit(mode_val[0])))
                else:
                    df = df.withColumn(c, F.coalesce(F.col(c), F.lit("NaN")))
        return df

    def remove_outliers_iqr(self, df: DataFrame, columns=None):
        if columns is None:
            columns = [c for c in df.columns
                       if isinstance(df.schema[c].dataType, (IntegerType, LongType, DoubleType, FloatType))]
        for c in columns:
            quantiles = df.approxQuantile(c, [0.25, 0.75], 0.01)
            if len(quantiles) == 2:
                q1, q3 = quantiles
                iqr = q3 - q1
                lower = q1 - (self.outlier_iqr_factor * iqr)
                upper = q3 + (self.outlier_iqr_factor * iqr)
                df = df.withColumn(c,
                                   F.when(F.col(c) < lower, lower)
                                   .when(F.col(c) > upper, upper)
                                   .otherwise(F.col(c)))
        return df

    def deduplicate(self, df: DataFrame, subset=None):
        original_count = df.count()
        technical_cols = {"_raw_row_seq", "auto_id"}
        usable_cols = [c for c in df.columns if c not in technical_cols]
        if subset:
            use_subset = [c for c in subset if c in usable_cols]
        else:
            use_subset = usable_cols
        df_dedup = df.dropDuplicates(use_subset)
        dup_removed = original_count - df_dedup.count()
        return df_dedup, dup_removed

    def clean(self, df: DataFrame, file_name: str = None, dedupe_subset=None, remove_outliers=False):
        df = self.normalize_columns(df)
        df = self.remove_noise_columns(df)
        df = self.clean_text_columns(df)
        df, converted_map = self.convert_column_types(df)
        df = self.handle_missing_values(df)
        df, full_dup_removed = self.deduplicate(df, subset=None)
        print(f"🗑 Full row duplicates removed: {full_dup_removed}")
        if dedupe_subset:
            df, dup_removed = self.deduplicate(df, subset=dedupe_subset)
        else:
            dup_removed = 0
        if remove_outliers:
            df = self.remove_outliers_iqr(df)
        return df, {
            "converted_map": converted_map,
            "duplicates_removed": dup_removed,
            "rows_out": df.count()
        }

def detect_primary_key_strict(df: DataFrame):
    n = df.count()
    id_keywords = ["id", "order", "invoice", "txn", "match", "ref", "no"]
    for c in df.columns:
        if c == "_raw_row_seq":
            continue
        cname = c.lower()
        if any(k in cname for k in id_keywords):
            non_null = df.filter(F.col(c).isNotNull()).count()
            distinct = df.select(c).distinct().count()
            if non_null == n and distinct == n:
                return c
    if "_raw_row_seq" in df.columns:
        return "_raw_row_seq"
    return None

def sort_by_primary_key_if_exists(df: DataFrame, id_col: str):
    if id_col and id_col in df.columns:
        return df.orderBy(F.col(id_col).asc())
    return df

def split_entity_metric_dimension_strict(df: DataFrame):
    id_keywords = ["id", "order", "invoice", "txn", "match", "ref", "no"]
    id_col = None
    total_rows = df.count()
    for c in df.columns:
        cname = c.lower()
        if any(k in cname for k in id_keywords):
            non_null = df.filter(F.col(c).isNotNull()).count()
            distinct = df.select(c).distinct().count()
            if non_null >= total_rows * 0.95 and distinct >= total_rows * 0.9:
                id_col = c
                break
    if not id_col:
        id_col = "auto_id"
        df = df.withColumn(id_col, monotonically_increasing_id() + 1)

    metric_cols = [id_col]
    for c in df.columns:
        dtype = df.schema[c].dataType
        cname = c.lower()
        if isinstance(dtype, (IntegerType, LongType, DoubleType, FloatType, DateType, TimestampType)):
            metric_cols.append(c)
        elif "date" in cname or "time" in cname:
            metric_cols.append(c)
    metric_cols = list(dict.fromkeys(metric_cols))
    metrics_df = df.select(*metric_cols)

    candidate_entity_cols = [
        c for c in df.columns
        if c != id_col
        and not isinstance(df.schema[c].dataType,(IntegerType, LongType, DoubleType, FloatType, DateType, TimestampType))
    ]
    cardinality = {c: df.select(countDistinct(c)).collect()[0][0] for c in candidate_entity_cols}
    ordered_entities = sorted(cardinality, key=cardinality.get)
    entity_cols = [id_col] + ordered_entities[:3]
    entity_df = df.select(*entity_cols)

    used_cols = set(metric_cols + entity_cols)
    dimension_cols = [id_col] + [
        c for c in df.columns
        if c != id_col
        and c not in used_cols
        and isinstance(df.schema[c].dataType, StringType)
    ]
    dimension_df = df.select(*dimension_cols)

    return {
        "entity_table": entity_df,
        "metrics_table": metrics_df,
        "dimension_table": dimension_df,
        "id_column": id_col
    }, df

def spark_to_mysql_dtype(spark_type):
    if isinstance(spark_type, (IntegerType, LongType)):
        return "BIGINT"
    elif isinstance(spark_type, (DoubleType, FloatType)):
        return "DOUBLE"
    elif isinstance(spark_type, TimestampType):
        return "DATETIME"
    elif isinstance(spark_type, DateType):
        return "DATE"
    else:
        return "TEXT"

def create_mysql_table(cursor, table_name, df: DataFrame):
    seen = set()
    cols_sql = []
    for field in df.schema.fields:
        if field.name in seen: continue
        seen.add(field.name)
        cols_sql.append(f"`{field.name}` {spark_to_mysql_dtype(field.dataType)}")
    sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({','.join(cols_sql)})"
    cursor.execute(sql)

def insert_spark_dataframe_mysql(cursor, table_name, df: DataFrame, batch_size=1000):
    cols = [f"`{c}`" for c in df.columns]
    placeholders = ",".join(["%s"] * len(cols))
    sql = f"INSERT INTO `{table_name}` ({','.join(cols)}) VALUES ({placeholders})"
    def row_generator():
        for row in df.toLocalIterator():
            yield tuple(None if v is None else v for v in row)
    batch = []
    for r in row_generator():
        batch.append(r)
        if len(batch) >= batch_size:
            cursor.executemany(sql, batch)
            batch.clear()
    if batch:
        cursor.executemany(sql, batch)

def force_date_to_string_yyyy_mm_dd(df: DataFrame):
    for field in df.schema.fields:
        if isinstance(field.dataType, (DateType, TimestampType)):
            df = df.withColumn(field.name, F.date_format(F.col(field.name), "yyyy-MM-dd"))
    return df

def save_tables_and_register(*, df_clean: DataFrame, tables, file_base, save_folder):
    try:
        # FULL TABLE
        full_path = os.path.join(save_folder, f"{file_base}_fulltable.csv")
        df_clean.toPandas().to_csv(full_path, index=False)

        # ENTITY TABLE
        if "entity_table" in tables and isinstance(tables["entity_table"], DataFrame):
            entity_path = os.path.join(save_folder, f"{file_base}_entity.csv")
            tables["entity_table"].toPandas().to_csv(entity_path, index=False)

        # METRICS TABLE
        if "metrics_table" in tables and isinstance(tables["metrics_table"], DataFrame):
            if tables["metrics_table"].count() > 0:
                metrics_path = os.path.join(save_folder, f"{file_base}_metrics.csv")
                tables["metrics_table"].toPandas().to_csv(metrics_path, index=False)

        # DIMENSION TABLE
        if "dimension_table" in tables and isinstance(tables["dimension_table"], DataFrame):
            if tables["dimension_table"].count() > 0:
                dim_path = os.path.join(save_folder, f"{file_base}_dimension.csv")
                tables["dimension_table"].toPandas().to_csv(dim_path, index=False)

        print(f"✅ CSV files saved in {save_folder}")
    except Exception as e:
        print(f"❌ Error saving tables to {save_folder}: {e}")

def validate_processed_files_spark(file_base):
    folder = os.path.join(PROCESSED_DIR, file_base)
    required_files = [
        f"{file_base}_fulltable.csv",
        f"{file_base}_entity.csv",
        f"{file_base}_metrics.csv",
        f"{file_base}_dimension.csv"
    ]
    if not os.path.exists(folder):
        return False, "Processed folder missing"
    for f in required_files:
        if not os.path.exists(os.path.join(folder, f)):
            return False, f"Missing file: {f}"
    return True, None

def validate_mysql_tables_spark(cursor, file_base):
    tables = [
        f"{file_base}_fulltable",
        f"{file_base}_entity",
        f"{file_base}_metrics",
        f"{file_base}_dimension"
    ]
    for t in tables:
        cursor.execute("SHOW TABLES LIKE %s", (t,))
        if not cursor.fetchone():
            return False, f"Missing table: {t}"
    return True, None

def insert_file_run_stats(cursor, file_name, rows_count, processed_at, status="DONE"):
    try:
        cursor.execute("""
            INSERT INTO file_run_stats
            (file_name, company_name, uploaded_by,
             processed_at, rows_count, status)
            SELECT
              f.file_name,
              f.company_name,
              f.uploaded_by,
              %s,
              %s,
              %s
            FROM files f
            WHERE f.file_name = %s
            ORDER BY f.id DESC
            LIMIT 1
        """, (
            processed_at,
            rows_count,
            status,
            file_name
        ))
    except Exception as e:
        print("❌ file_run_stats insert failed:", e)

def process_dataframe_and_save(spark, df: DataFrame, file_base, raw_columns=None):
    if df is None:
        raise ValueError("Input must be a Spark DataFrame")

    cleaner = PySparkAdvancedCleaner()

    print(f"🔄 Cleaning DataFrame with {df.count()} rows...")
    df_clean, stats = cleaner.clean(df, file_name=file_base)
    df_full = df_clean  
    print("✅ Cleaning stats:", stats)

    pk = detect_primary_key_strict(df_clean)
    if pk:
        df_clean = sort_by_primary_key_if_exists(df_clean, pk)

    if raw_columns:
        df_clean = df_clean.select(*[c for c in raw_columns if c in df_clean.columns])

    print("🔄 Splitting into entity/metrics/dimension tables...")
    tables, df_clean = split_entity_metric_dimension_strict(df_clean)
    
    tables["entity_table"] = drop_technical_cols(tables["entity_table"])
    tables["metrics_table"] = drop_technical_cols(tables["metrics_table"])
    tables["dimension_table"] = drop_technical_cols(tables["dimension_table"])

    for k, v in tables.items():
        if isinstance(v, DataFrame):
            tables[k] = force_date_to_string_yyyy_mm_dd(v)

    df_full = force_date_to_string_yyyy_mm_dd(df_full)
    id_col = tables["id_column"]

    df_full = df_clean
    df_full = drop_technical_cols(df_full)

    if id_col == "auto_id" and "auto_id" in df_full.columns:
        df_full = df_full.drop("auto_id")

    save_folder = os.path.join(PROCESSED_DIR, file_base)
    safe_makedirs(save_folder)

    print("💾 Saving CSV files...")
    save_tables_and_register(
        df_clean=df_full,
        tables=tables,
        file_base=file_base,
        save_folder=save_folder
    )
    if "auto_id" in df_full.columns:
        df_full = df_full.drop("auto_id")

    print("💾 Saving to MySQL...")
    from app.db.connection import get_db_connection
    db = get_db_connection()
    if not db: return
    cursor = db.cursor()

    cursor.execute(f"DROP TABLE IF EXISTS `{file_base}_fulltable`")
    cursor.execute(f"DROP TABLE IF EXISTS `{file_base}_entity`")
    cursor.execute(f"DROP TABLE IF EXISTS `{file_base}_metrics`")
    cursor.execute(f"DROP TABLE IF EXISTS `{file_base}_dimension`")

    create_mysql_table(cursor, f"{file_base}_fulltable", df_full)
    insert_spark_dataframe_mysql(cursor, f"{file_base}_fulltable", df_full)

    create_mysql_table(cursor, f"{file_base}_entity", tables["entity_table"])
    insert_spark_dataframe_mysql(cursor, f"{file_base}_entity", tables["entity_table"])

    metrics_df = tables.get("metrics_table")
    if metrics_df is not None and metrics_df.count() > 0:
        create_mysql_table(cursor, f"{file_base}_metrics", metrics_df)
        insert_spark_dataframe_mysql(cursor, f"{file_base}_metrics", metrics_df)

    dimension_df = tables.get("dimension_table")
    if dimension_df is not None and dimension_df.count() > 0:
        create_mysql_table(cursor, f"{file_base}_dimension", dimension_df)
        insert_spark_dataframe_mysql(cursor, f"{file_base}_dimension", dimension_df)

    db.commit()
    cursor.close()
    db.close()

    folder_valid, folder_msg = validate_processed_files_spark(file_base)
    db = get_db_connection()
    if db:
        cursor = db.cursor()
        table_valid, table_msg = validate_mysql_tables_spark(cursor, file_base)
        cursor.close()
        db.close()

        if folder_valid and table_valid:
            print(f"✅ Spark processing + validation completed for {file_base}")
        else:
            print(f"❌ Validation failed for {file_base}")
            if not folder_valid: print("CSV issue:", folder_msg)
            if not table_valid: print("MySQL issue:", table_msg)

    print(f"✅ Processing completed for {file_base}")

def run_pipeline_for_api(spark, file_record):
    api_id = file_record.get("id")
    response_raw = file_record.get("response")
    file_name = file_record.get("file_name") or f"api_{api_id}"
    file_base = os.path.splitext(file_name)[0].strip()

    if response_raw is None:
        print("API record has no response. Skipping.")
        return

    try:
        parsed_json = response_raw
        if isinstance(response_raw, (str, bytes)):
            parsed_json = json.loads(response_raw)

        pandas_df = auto_flatten_json_to_pandas(parsed_json)
        spark_df = spark.createDataFrame(pandas_df)
    except Exception as e:
        print("⚠ Could not parse response JSON:", e)
        return

    process_dataframe_and_save(spark, spark_df, file_base)

def run_pipeline_for_file(spark, file_record):
    file_name = file_record.get("file_name")
    file_path = file_record.get("file_path")
    table_name = file_record.get("table_name")

    if file_path == "MULTI_UPLOAD":
        if not table_name:
            print("❌ MULTI_UPLOAD found but table_name missing")
            return
        print(f"📊 Processing MySQL table directly: {table_name}")
        try:
            from app.db.connection import DB_CONFIG
            import mysql.connector
            conn = mysql.connector.connect(**DB_CONFIG)
            pandas_df = pd.read_sql(f"SELECT * FROM `{table_name}`", conn)
            conn.close()

            if pandas_df.empty:
                print("⚠ Table is empty. Skipping.")
                return

            spark_df = spark.createDataFrame(pandas_df)
            spark_df = spark_df.withColumn("_raw_row_seq", monotonically_increasing_id())
            raw_columns = spark_df.columns
            process_dataframe_and_save(spark, spark_df, table_name, raw_columns)
        except Exception as e:
            print("❌ Error reading MySQL table:", e)
        return

    if not file_name:
        print("❌ No file_name in record. Skipping.")
        return

    file_base = os.path.splitext(file_name)[0].strip()
    ext = os.path.splitext(file_name)[1].lower()
    
    # Fallback: if file_name has no extension, try getting it from file_path
    if not ext and file_path:
        ext = os.path.splitext(file_path)[1].lower()

    try:
        if file_record.get("file_content"):
            import io
            print(f"✅ Processing from DB BLOB: {file_name}")
            content = file_record.get("file_content")
            if isinstance(content, memoryview):
                content = content.tobytes()
            buffer = io.BytesIO(content)

            if ext == ".csv": pandas_df = pd.read_csv(buffer)
            elif ext in [".xlsx", ".xls"]: pandas_df = pd.read_excel(buffer, dtype=str)
            elif ext == ".json":
                parsed = json.loads(content.decode("utf-8"))
                pandas_df = auto_flatten_json_to_pandas(parsed)
            else:
                print("⚠ Unknown format, trying CSV fallback")
                pandas_df = pd.read_csv(buffer)

            spark_df = spark.createDataFrame(pandas_df)
            spark_df = spark_df.withColumn("_raw_row_seq", monotonically_increasing_id())

        elif file_path:
            print(f"📁 Processing from file path: {file_path}")
            
            potential_paths = []
            normalized_path = file_path.replace("/", os.sep).lstrip(os.sep)
            
            # 1. Try with BASE_UPLOAD_DIR
            potential_paths.append(os.path.join(BASE_UPLOAD_DIR, normalized_path))
            
            # 2. Try with project root (where the pipeline runs)
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            potential_paths.append(os.path.join(project_root, normalized_path))
            
            # 3. Try the original path directly
            potential_paths.append(file_path)

            effective_path = None
            for p in potential_paths:
                if os.path.exists(p):
                    effective_path = p
                    break
            
            if not effective_path:
                print("❌ File not found. Checked the following locations:")
                for p in potential_paths:
                    print(f"   - {p}")
                return
            
            file_path = effective_path

            if ext == ".csv":
                spark_df = spark.read.csv(file_path, header=True, inferSchema=True, multiLine=True, escape='"')
            elif ext in [".xlsx", ".xls"]:
                pandas_df = pd.read_excel(file_path, dtype=str)
                spark_df = spark.createDataFrame(pandas_df)
            elif ext == ".json":
                with open(file_path, "r", encoding="utf-8") as f:
                    parsed = json.load(f)
                pandas_df = auto_flatten_json_to_pandas(parsed)
                spark_df = spark.createDataFrame(pandas_df)
            else:
                print("❌ Unsupported format:", ext)
                return
            spark_df = spark_df.withColumn("_raw_row_seq", monotonically_increasing_id())
        else:
            print("❌ No BLOB or file_path found")
            return
    except Exception as e:
        print("❌ Error loading file:", e)
        return

    print(f"📁 Processing File: {file_name} (rows: {spark_df.count()})")
    process_dataframe_and_save(spark, spark_df, file_base)

    processed_at = datetime.now()
    rows_count = spark_df.count()
    from app.db.connection import get_db_connection
    db = get_db_connection()
    if db:
        cursor = db.cursor()
        insert_file_run_stats(cursor, file_name, rows_count, processed_at, status="DONE")
        db.commit()
        cursor.close()
        db.close()
