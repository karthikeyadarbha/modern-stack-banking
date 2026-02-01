

WITH silver_src AS (
    -- Reusing the same Iceberg source we just validated
    SELECT 
        txn_id,
        txn_ts,
        'SPARK_ICEBERG_SILVER' as record_source
    FROM iceberg_scan('/workspaces/modern-stack-banking/data/warehouse/db/silver_transactions/metadata/v1.metadata.json')
)

SELECT
    -- MD5 Hashing the business key (txn_id) to create the unique anchor
    MD5(CAST(txn_id AS VARCHAR)) as txn_hash_key,
    txn_id,
    txn_ts as load_date,
    record_source
FROM silver_src

