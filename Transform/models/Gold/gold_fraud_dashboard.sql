{{ config(materialized='view') }}

WITH hubs AS (
    -- Updated to use the full path
    SELECT 
        txn_hash_key, 
        txn_id, 
        load_date as established_date 
    FROM {{ ref('hubs_transactions') }} 
),

features AS (
    -- Updated to use the full path
    SELECT 
        txn_hash_key, 
        amount, 
        is_fraud_label, 
        pca_features,
        load_date as observation_date
    FROM {{ ref('sat_txn_features') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_hash_key ORDER BY load_date DESC) = 1
),

ai_insights AS (
    -- THIS IS THE CRITICAL FIX: Match the folder-based naming
    SELECT 
        txn_hash_key, 
        ai_reason_for_flag,
        load_date as insight_date
    FROM {{ ref('sat_txn_ai_insights') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_hash_key ORDER BY load_date DESC) = 1
)

SELECT 
    h.txn_id,
    f.amount,
    f.is_fraud_label,
    CASE 
        WHEN f.is_fraud_label = 1 THEN '🚩 FRAUD'
        ELSE '✅ NORMAL'
    END as fraud_status,
    a.ai_reason_for_flag as forensic_explanation,
    f.pca_features,
    h.established_date
FROM hubs h
LEFT JOIN features f ON h.txn_hash_key = f.txn_hash_key
LEFT JOIN ai_insights a ON h.txn_hash_key = a.txn_hash_key