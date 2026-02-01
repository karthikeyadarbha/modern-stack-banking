import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

# --- Setup Paths ---
WAREHOUSE_PATH = os.path.abspath("data/warehouse")

# --- Initialize Spark ---
spark = SparkSession.builder \
    .appName("Gold-Correlation-Analysis") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH) \
    .getOrCreate()

print("📊 Loading Silver Layer for Feature Analysis...")

try:
    # 1. Load the Silver Table
    df_silver = spark.table("local.db.silver_transactions")

    # 2. Explode the Map into Columns
    # We create a list of all V1...V28 columns from the pca_features map
    feature_cols = [f"V{i}" for i in range(1, 29)]
    for f in feature_cols:
        df_silver = df_silver.withColumn(f, col("pca_features").getItem(f))

    # 3. Assemble for Correlation
    # Spark's Correlation tool requires a single vector column
    assembler = VectorAssembler(inputCols=feature_cols + ["is_fraud_label"], outputCol="features_and_label")
    assembled_df = assembler.transform(df_silver).select("features_and_label")

    # 4. Calculate Pearson Correlation Matrix
    # Formula: $r = \frac{\sum(x_i - \bar{x})(y_i - \bar{y})}{\sqrt{\sum(x_i - \bar{x})^2 \sum(y_i - \bar{y})^2}}$
    matrix = Correlation.corr(assembled_df, "features_and_label").head()[0]
    
    # 5. Extract Fraud Correlation (The last row/column of the matrix)
    # We want the correlation of V1-V28 with the 'is_fraud_label'
    fraud_correlations = matrix.toArray()[-1][:-1]
    
    # 6. Result Sorting
    results = sorted(zip(feature_cols, fraud_correlations), key=lambda x: abs(x[1]), reverse=True)

    print("\n🔍 --- Top PCA Features Driving Fraud ---")
    print(f"{'Feature':<10} | {'Correlation with Fraud':<25}")
    print("-" * 40)
    for feature, corr in results[:5]: # Show top 5
        print(f"{feature:<10} | {corr:>25.4f}")

except Exception as e:
    print(f"❌ Analysis failed: {e}")

finally:
    spark.stop()