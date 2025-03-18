import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import uuid

def generate_transaction_data(num_records: int = 1000):
    records = []
    now = datetime.now()
    for _ in range(num_records):
        record = {
            "transaction_id": str(uuid.uuid4()),
            "customer_id": "CUST" + str(np.random.randint(1000, 10000)),
            "timestamp": (now - timedelta(days=np.random.randint(0,30))).strftime("%Y-%m-%d %H:%M:%S"),
            "amount": round(np.random.uniform(10, 500), 2),
            "transaction_type": np.random.choice(["purchase", "refund", "payment"]),
            "merchant": np.random.choice(["StoreA", "StoreB", "StoreC"]),
            "category": np.random.choice(["food", "entertainment", "utilities"]),
            "status": np.random.choice(["completed", "pending", "failed"])
        }
        records.append(record)
    return records

if __name__ == "__main__":
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    output_dir = os.path.join(BASE_DIR, "data", "sample")
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "transactions.csv")
    df = pd.DataFrame(generate_transaction_data(1000))
    df.to_csv(output_file, index=False)
    print(f"Generated CSV file at: {output_file}")
