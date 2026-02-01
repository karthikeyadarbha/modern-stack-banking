

-- This model acts as the landing zone for our Python AI script
-- It grabs the specific transaction from the Hub and attaches the explanation passed via variables
SELECT
    h.txn_hash_key,
    'Highly anomalous PCA scores across multiple factors indicate fraudulent transaction.' as ai_reason_for_flag,
    CURRENT_TIMESTAMP as load_date,
    'LLAMA3_OLLAMA_FORENSIC' as record_source
FROM "argus_vault"."main"."hubs_transactions" h
WHERE h.txn_id = '393d0456-b718-4cbb-9475-8203b721cd1b'