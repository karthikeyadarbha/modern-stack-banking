#!/usr/bin/env python3
import duckdb
import requests
import subprocess
import json

def process_unexplained_fraud(limit=5):
    """Finds fraud cases without explanations and processes them in a batch."""
    con = duckdb.connect('data/warehouse/argus_vault.db')
    
    # ADVANCED QUERY: Identifies the 'Explanation Gap' using a LEFT JOIN
    query = """
        SELECT 
            h.txn_id, 
            s.amount, 
            s.pca_features 
        FROM sat_txn_features s
        JOIN hubs_transactions h ON s.txn_hash_key = h.txn_hash_key
        LEFT JOIN sat_txn_ai_insights a ON s.txn_hash_key = a.txn_hash_key
        WHERE s.is_fraud_label = 1 
          AND a.ai_reason_for_flag IS NULL
        LIMIT ?
    """
    
    try:
        unexplained_cases = con.execute(query, [limit]).fetchall()
    except Exception as e:
        print(f"❌ Database Error: {e}")
        return
    finally:
        con.close()

    if not unexplained_cases: 
        print("✅ Success: All fraud cases are currently explained. Coverage is 100%.")
        return

    print(f"🚀 Found {len(unexplained_cases)} unexplained cases. Starting batch inference...")

    for txn_id, amount, pca_features in unexplained_cases:
        print(f"\n🔍 Analyzing: {txn_id}")
        
        prompt = (f"System: Financial Forensic Analyst. Explain why this txn is fraud based "
                  f"on these PCA factors: {pca_features}. Keep it under 15 words.")
        
        try:
            # Call Ollama (Llama3)
            response = requests.post('http://localhost:11434/api/generate', 
                                    json={"model": "llama3", "prompt": prompt, "stream": False})
            
            ai_text = response.json().get('response', 'No response').replace("'", "").replace("\n", " ")
            print(f"🤖 AI Insight: {ai_text}")

            # WRITE BACK to Data Vault via dbt
            dbt_cmd = [
                "../.msb-poc/bin/dbt", "run",
                "--select", "sat_txn_ai_insights",
                "--vars", f'{{"txn_id": "{txn_id}", "ai_explanation": "{ai_text}"}}',
                "--profiles-dir", "."
            ]
            
            subprocess.run(dbt_cmd, cwd="Transform", check=True, capture_output=True)
            print(f"💾 Insight stored for {txn_id}")

        except Exception as e:
            print(f"❌ Error processing {txn_id}: {e}")

if __name__ == "__main__":
    # You can increase this limit once you verify the first few work!
    process_unexplained_fraud(limit=3)