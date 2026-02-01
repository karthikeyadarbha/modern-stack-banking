
  
    
    

    create  table
      "argus_vault"."main"."sat_txn_features"
  
    as (
      

WITH silver_data AS (
    -- Reading from our Iceberg Silver layer
    SELECT * FROM iceberg_scan('/workspaces/modern-stack-banking/data/warehouse/db/silver_transactions/metadata/v1.metadata.json')
)

SELECT
    -- 1. Link to the Hub (Primary Key)
    MD5(CAST(txn_id AS VARCHAR)) as txn_hash_key,

    -- 2. The Features (Context)
    pca_features,
    is_fraud_label,
    amount,
    
    -- 3. Hash Diff (Standard DV 2.0 for detecting state changes)
    MD5(
        CAST(amount AS VARCHAR) || 
        CAST(is_fraud_label AS VARCHAR) || 
        CAST(pca_features AS VARCHAR)
    ) as hash_diff,

    -- 4. Metadata
    txn_ts as load_date,
    'SPARK_ICEBERG_SILVER' as record_source

FROM silver_data


    );
  
  
  