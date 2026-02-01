

-- This model acts as the landing zone for our Python AI script
-- It grabs the specific transaction from the Hub and attaches the explanation passed via variables
SELECT
    h.txn_hash_key,
    'The transaction is fraud due to a significant anomaly score (-2.3122265423263) indicating suspicious behavior in multiple PCA factors.' as ai_reason_for_flag,
    CURRENT_TIMESTAMP as load_date,
    'LLAMA3_OLLAMA_FORENSIC' as record_source
FROM "argus_vault"."main"."hubs_transactions" h
WHERE h.txn_id = '5142e1f7-4873-4cfd-ab71-c1731ad3861e'