import opendatasets as od
import pandas as pd
import os
import json
import uuid
from datetime import datetime, timedelta

def process_kaggle_data():
    # 1. Download (Will ask for Username/Key)
    dataset_url = 'https://www.kaggle.com/mlg-ulb/creditcardfraud'
    data_dir = 'data/temp'
    
    print("⬇️  Requesting data from Kaggle API...")
    od.download(dataset_url, data_dir=data_dir)
    
    # 2. Paths
    csv_path = f'{data_dir}/creditcardfraud/creditcard.csv'
    output_path = "data/landing/transactions.json"
    os.makedirs("data/landing", exist_ok=True)
    
    print(f"🔄  Transmuting {csv_path} to Simulated Stream Logs...")
    
    # 3. Read & Stream
    # We read the CSV (284k rows) and write it line-by-line as JSON
    df = pd.read_csv(csv_path)
    
    with open(output_path, 'w') as f:
        for index, row in df.iterrows():
            # Create a synthetic transaction ID
            txn_id = str(uuid.uuid4())
            
            # The Kaggle dataset 'Time' is "seconds since first txn".
            # We shift this to be "Days since today" to make it look recent.
            # Real Fraud Detection needs dates, not just offsets.
            seconds_offset = int(row['Time'])
            fake_txn_time = datetime.now() - timedelta(seconds=(172800 - seconds_offset))
            
            # Construct the Record
            record = {
                "txn_id": txn_id,
                "account_id": str(uuid.uuid4()), # Simulating unique accounts
                "amount": float(row['Amount']),
                "txn_ts": fake_txn_time.isoformat(),
                "is_fraud_label": int(row['Class']), # 0 = Legitimate, 1 = Fraud
                # We pack the 28 anonymized PCA features into a nested object
                # This is a common pattern: unstructured payload in structured table
                "pca_features": {
                    f"V{i}": row[f'V{i}'] for i in range(1, 29)
                }
            }
            f.write(json.dumps(record) + "\n")
            
            if index % 50000 == 0:
                print(f"   Processed {index} rows...")

    print(f"✅  Success! Data landing at: {output_path}")
    print(f"   (You can now delete the 'data/temp' folder to save space)")

if __name__ == "__main__":
    process_kaggle_data()