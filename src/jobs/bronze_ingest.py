import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, col

# --- Configuration ---
WAREHOUSE_PATH = os.path.abspath("data/warehouse")
RAW_DATA_PATH = "data/landing/fraud_data.csv" # Ensure this matches your uploaded file name

# --- Initialize Spark with Iceberg ---
# In src/jobs/bronze_ingest.py

# In src/jobs/bronze_ingest.py

# In src/jobs/bronze_ingest.py

spark = SparkSession.builder \
    .appName("Argus-Bronze-Ingestion") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH) \
    .config("spark.driver.extraJavaOptions", 
            "--add-opens=java.base/java.lang=ALL-UNNAMED " +
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED " +
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED " +
            "--add-opens=java.base/java.io=ALL-UNNAMED " +
            "--add-opens=java.base/java.net=ALL-UNNAMED " +
            "--add-opens=java.base/java.nio=ALL-UNNAMED " +
            "--add-opens=java.base/java.util=ALL-UNNAMED " +
            "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED " +
            "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED " +
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
            "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED " +
            "--add-opens=java.base/sun.security.action=ALL-UNNAMED " +
            "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED " +
            "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED") \
    .getOrCreate()

print(f"🚀 Spark Session Started. Warehouse: {WAREHOUSE_PATH}")

# --- Step 1: Read Raw CSV ---
try:
    print(f"reading from {RAW_DATA_PATH}...")
    df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_DATA_PATH)
    
    # --- Step 2: Add Metadata Columns (Audit Trail) ---
    df_bronze = df_raw \
        .withColumn("ingestion_ts", current_timestamp()) \
        .withColumn("source_file", input_file_name()) \
        .withColumnRenamed("trans_date_trans_time", "txn_ts") \
        .withColumnRenamed("trans_num", "txn_id")

    # --- Step 3: Write to Iceberg (Bronze Layer) ---
    # We use 'append' for logs, but 'overwrite' here for the experiment to keep it clean
    print("Writing to Iceberg Bronze Layer...")
    df_bronze.writeTo("local.db.bronze_transactions") \
        .createOrReplace()

    print("✅ Bronze Layer Ingestion Complete: local.db.bronze_transactions")
    
    # Validation Count
    count = spark.table("local.db.bronze_transactions").count()
    print(f"📊 Total Records in Bronze: {count}")

except Exception as e:
    print(f"❌ Error during ingestion: {e}")

finally:
    spark.stop()