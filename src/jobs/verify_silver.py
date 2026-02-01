import os
from pyspark.sql import SparkSession

# --- Setup Paths ---
WAREHOUSE_PATH = os.path.abspath("data/warehouse")

# --- Build Spark Session with Iceberg Packages ---
spark = SparkSession.builder \
    .appName("Verify-Silver-Final") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH) \
    .getOrCreate()

print("\n📊 --- Silver Layer: Fraud Distribution by Category ---")

try:
    df = spark.table("local.db.silver_transactions")
    
    print("\n📊 --- Silver Layer Analysis: Fraud by Account ---")
    df.groupBy("account_id") \
      .agg(
          {"*": "count", 
           "is_fraud_label": "sum", 
           "amount": "avg"}
      ) \
      .withColumnRenamed("count(1)", "txn_volume") \
      .withColumnRenamed("sum(is_fraud_label)", "fraud_events") \
      .withColumnRenamed("avg(amount)", "avg_ticket_size") \
      .orderBy("fraud_events", ascending=False) \
      .limit(10) \
      .show()

    # Bonus: Check the PCA Feature vector to ensure it's populated
    print("🧬 Sample PCA Feature Vector:")
    df.select("pca_features").limit(1).show(truncate=False)

except Exception as e:
    print(f"❌ Verification failed: {e}")
    
finally:
    spark.stop()