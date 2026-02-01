#!/usr/bin/env python3
import duckdb
import requests
import subprocess
import json

def explain_and_store_fraud():
    # 1. Fetch a target fraud case by JOINING Hub and Sat
    con = duckdb.connect('data/warehouse/argus_vault.db')
    
    # CORRECTED QUERY: Joins Hub (for ID) and Sat (for Features)
    query = """
        SELECT 
            h.txn_id, 
            s.amount, 
            s.pca_features 
        FROM sat_txn_features s
        JOIN hubs_transactions h ON s.txn_hash_key = h.txn_hash_key
        WHERE s.is_fraud_label = 1 
        LIMIT 1
    """
    
    try:
        case = con.execute(query).fetchone()
    except Exception as e:
        print(f"❌ Database Error: {e}")
        return
    finally:
        con.close()

    if not case: 
        print("No fraud cases found.")
        return

    txn_id, amount, pca_features = case
    print(f"🔍 Analyzing Transaction: {txn_id} (${amount})")

    # 2. Get AI Explanation
    prompt = f"System: Financial Forensic Analyst. Explain why this txn is fraud based on these PCA factors: {pca_features}. Keep it under 20 words."
    
    try:
        # Call Ollama
        response = requests.post('http://localhost:11434/api/generate', 
                                json={"model": "llama3", "prompt": prompt, "stream": False})
        
        # Clean the response text to prevent SQL injection issues
        ai_text = response.json().get('response', 'No response').replace("'", "").replace('"', '').replace("\n", " ")
        
        print(f"🤖 AI Insight: {ai_text}")

        # 3. WRITE BACK: Trigger dbt to insert this specific record
        print("💾 Saving insight to Data Vault...")
        dbt_cmd = [
            "../.msb-poc/bin/dbt", "run",
            "--select", "sat_txn_ai_insights",
            "--vars", f'{{"txn_id": "{txn_id}", "ai_explanation": "{ai_text}"}}',
            "--profiles-dir", "."
        ]
        
        # We run this from the 'Transform' directory context
        subprocess.run(dbt_cmd, cwd="Transform", check=True)
        print("✅ Insight successfully stored.")

    except Exception as e:
        print(f"❌ Error during AI/dbt execution: {e}")

if __name__ == "__main__":
    explain_and_store_fraud()