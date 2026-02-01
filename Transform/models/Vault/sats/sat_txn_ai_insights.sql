{{ config(materialized='incremental') }}

-- This model acts as the landing zone for our Python AI script
-- It grabs the specific transaction from the Hub and attaches the explanation passed via variables
SELECT
    h.txn_hash_key,
    '{{ var("ai_explanation", "Pending AI Review") }}' as ai_reason_for_flag,
    CURRENT_TIMESTAMP as load_date,
    'LLAMA3_OLLAMA_FORENSIC' as record_source
FROM {{ ref('hubs_transactions') }} h
WHERE h.txn_id = '{{ var("txn_id", "0000") }}'