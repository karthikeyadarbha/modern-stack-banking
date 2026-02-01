import duckdb
import pandas as pd

# 1. Connect to your Data Vault
con = duckdb.connect('data/warehouse/argus_vault.db')

# 2. Extract the Validated Gold Data
# We specifically want to compare Fraud (1) vs Normal (0) behaviors
query = """
SELECT 
    is_fraud_label,
    amount,
    pca_features,
    txn_id
FROM gold_fraud_analysis
WHERE is_fraud_label IS NOT NULL
ORDER BY is_fraud_label DESC
LIMIT 10
"""

df = con.execute(query).df()

# 3. Format for AI Analysis
# We'll display the PCA vector as a summary for the model to interpret
print("\n🤖 --- Data Ready for AI Interpretation ---")
for index, row in df.iterrows():
    status = "🚩 FRAUD" if row['is_fraud_label'] == 1 else "✅ NORMAL"
    print(f"\nTransaction ID: {row['txn_id']}")
    print(f"Status: {status} | Amount: ${row['amount']}")
    print(f"Top PCA Signal (V17): {row['pca_features'].get('V17', 'N/A')}")

con.close()