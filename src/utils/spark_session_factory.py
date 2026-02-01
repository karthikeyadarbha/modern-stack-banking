import os
from pyspark.sql import SparkSession

def create_hybrid_spark_session(app_name: str = "Argus-Lakehouse") -> SparkSession:
    """
    Initializes a SparkSession configured for BOTH Delta Lake and Iceberg.
    """
    
    # 1. Define the Local Warehouse Path
    warehouse_path = os.path.abspath("data/warehouse")
    
    # 2. Define Dependency Versions
    iceberg_version = "1.4.3"
    delta_version = "3.0.0"
    scala_version = "2.12"
    
    packages = [
        f"org.apache.iceberg:iceberg-spark-runtime-3.5_{scala_version}:{iceberg_version}",
        f"io.delta:delta-spark_{scala_version}:{delta_version}"
    ]
    
    # 3. Build the Session
    print(f"⚡ Starting Spark {app_name} with Warehouse at: {warehouse_path}")
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", warehouse_path) \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
        
    return spark