

-- This model acts as the landing zone for our Python AI script
-- It grabs the specific transaction from the Hub and attaches the explanation passed via variables
SELECT
    h.txn_hash_key,
    'The transaction is fraudulent due to significantly high PCA V4 and V26 scores.' as ai_reason_for_flag,
    CURRENT_TIMESTAMP as load_date,
    'LLAMA3_OLLAMA_FORENSIC' as record_source
FROM "argus_vault"."main"."hubs_transactions" h
WHERE h.txn_id = '253dcfb5-1ca7-40c0-b2d8-abf694437713'