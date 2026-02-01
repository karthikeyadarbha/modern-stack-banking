from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, MapType
from src.utils.spark_session_factory import create_hybrid_spark_session
import os

def run_ingestion():
    # 1. Initialize the Hybrid Spark Engine (Delta + Iceberg)
    spark = create_hybrid_spark_session("Argus_Ingest_Real_Data")
    
    # 2. Define Strict Schema (Optimization)
    # We map the 'pca_features' as a Map (Dictionary) to keep the Bronze table clean.
    # We will explode/flatten it in the Silver layer later if needed.
    schema = StructType([
        StructField("txn_id", StringType(), True),
        StructField("account_id", StringType(), True), # <--- We will mask this later
        StructField("amount", DoubleType(), True),
        StructField("txn_ts", StringType(), True),
        StructField("is_fraud_label", IntegerType(), True),
        StructField("pca_features", MapType(StringType(), DoubleType()), True) 
    ])
    
    input_path = os.path.abspath("data/landing/transactions.json")
    
    # 3. Read JSON (Bronze Ingestion)
    print(f"📥 Reading Raw Stream from {input_path}...")
    df_raw = spark.read.schema(schema).json(input_path)
    
    # 4. Write to Bronze (Delta Lake)
    # Delta is best for Bronze because it handles appends/upserts quickly.
    bronze_table = "raw_transactions_delta"
    print(f"💾 Writing {df_raw.count()} rows to Bronze Table: {bronze_table}...")
    
    df_raw.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"default.{bronze_table}")
        
    # ... inside src/jobs/etl_ingest.py ...

    # 1. Read from the Bronze (Delta) table we just wrote
    df_bronze = spark.read.format("delta").table("raw_transactions_delta")
    
    print("❄️  Promoting to Silver (Iceberg) for Snowflake usage...")
    
    # 2. Define df_silver (The Transformation Step)
    # We'll just pass it through for now, but this is where you'd add clean-up logic
    df_silver = df_bronze

    print("❄️  Promoting to Silver (Iceberg) for Snowflake usage...")
    
    # --- ARCHITECT FIX: Explicitly Drop before Create ---
    # This forces the Hadoop Catalog to reset its state, preventing the version-hint mismatch
    spark.sql("DROP TABLE IF EXISTS local.db.silver_transactions")

    # Now write cleanly as a new table
    df_silver.writeTo("local.db.silver_transactions") \
        .create()  # Changed from createOrReplace() to create() since we just dropped it

    print("✅ Pipeline Complete.")

if __name__ == "__main__":
    run_ingestion()