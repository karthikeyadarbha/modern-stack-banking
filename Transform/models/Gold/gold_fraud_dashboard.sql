{{ config(materialized='view') }}

WITH hubs AS (
    SELECT txn_hash_key, txn_id, load_date as established_date 
    FROM {{ ref('hubs_transactions') }}
),
features AS (
    SELECT txn_hash_key, amount, is_fraud_label, pca_features, load_date
    FROM {{ ref('sat_txn_features') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_hash_key ORDER BY load_date DESC) = 1
),
ai_insights AS (
    SELECT txn_hash_key, ai_reason_for_flag, load_date
    FROM {{ ref('sat_txn_ai_insights') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_hash_key ORDER BY load_date DESC) = 1
)

SELECT 
    h.txn_id,
    f.amount,
    f.is_fraud_label,
    CASE WHEN f.is_fraud_label = 1 THEN '🚩 FRAUD' ELSE '✅ NORMAL' END as fraud_status,
    a.ai_reason_for_flag as forensic_explanation,
    -- ADD THESE COLUMNS FOR THE GOVERNANCE SCRIPT
    h.established_date,
    a.load_date as insight_date, 
    f.pca_features
FROM hubs h
LEFT JOIN features f ON h.txn_hash_key = f.txn_hash_key
LEFT JOIN ai_insights a ON h.txn_hash_key = a.txn_hash_key