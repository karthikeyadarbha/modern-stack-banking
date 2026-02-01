

-- This model acts as the landing zone for our Python AI script
-- It grabs the specific transaction from the Hub and attaches the explanation passed via variables
SELECT
    h.txn_hash_key,
    'Fraud detected: Anomalously low PCA scores (V1-V20) indicate suspicious transaction.' as ai_reason_for_flag,
    CURRENT_TIMESTAMP as load_date,
    'LLAMA3_OLLAMA_FORENSIC' as record_source
FROM "argus_vault"."main"."hubs_transactions" h
WHERE h.txn_id = '5a4787c2-94fd-44c9-94a3-616729639197'