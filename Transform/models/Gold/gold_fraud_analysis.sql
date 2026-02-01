{{ config(materialized='view') }}

WITH transactions AS (
    -- Get the unique IDs from the Hub
    SELECT 
        txn_hash_key,
        txn_id,
        record_source
    FROM {{ ref('hubs_transactions') }}
),

fraud_context AS (
    -- Get the latest state (features/label) from the Satellite
    SELECT 
        txn_hash_key,
        amount,
        is_fraud_label,
        pca_features,
        load_date
    FROM {{ ref('sat_txn_features') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_hash_key ORDER BY load_date DESC) = 1
)
SELECT
    t.txn_id,
    c.amount,
    c.is_fraud_label,
    c.pca_features,
    c.load_date as analysis_timestamp,
    t.record_source
FROM transactions t
JOIN fraud_context c ON t.txn_hash_key = c.txn_hash_key