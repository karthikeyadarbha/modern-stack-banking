import os
import json
import random
import uuid
from datetime import datetime, timedelta

def generate_dummy_data(row_count=100000):
    """
    Generates a raw JSON file simulating banking transactions.
    """
    print(f"🏭 Generating {row_count} transactions...")
    
    # Ensure directory exists
    os.makedirs("data/landing", exist_ok=True)
    file_path = "data/landing/transactions.json"
    
    accounts = [str(uuid.uuid4()) for _ in range(500)] # 500 unique accounts
    merchants = ["Uber", "Amazon", "Netflix", "Starbucks", "Apple", "Spotify"]
    
    with open(file_path, "w") as f:
        for _ in range(row_count):
            txn = {
                "txn_id": str(uuid.uuid4()),
                "account_id": random.choice(accounts),
                "cc_num": f"{random.randint(4000,4999)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}",
                "merchant": random.choice(merchants),
                "amount": round(random.uniform(5.0, 5000.0), 2),
                "txn_ts": (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat()
            }
            f.write(json.dumps(txn) + "\n")
            
    print(f"✅ Data generated at: {file_path}")
    print(f"📊 Size: {os.path.getsize(file_path) / (1024*1024):.2f} MB")

if __name__ == "__main__":
    generate_dummy_data(row_count=100000) # Generates ~25MB of data. Increase to 1M for ~250MB.