# capture_generate_data.py
import numpy as np
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
from random import randint, random, choice

N_TX = 5000           # number of transactions
N_PRODUCTS = 20
N_CUSTOMERS = 50
START = datetime.now() - timedelta(days=60)

fake = Faker()
rng = np.random.default_rng(42)

def generate_data(n=N_TX):
    product_ids = np.arange(1, N_PRODUCTS + 1)
    customer_ids = np.arange(1, N_CUSTOMERS + 1)

    rows = []
    for i in range(n):
        pid = int(choice(product_ids))
        cid = int(choice(customer_ids))
        qty = int(rng.integers(1, 6))                     # 1–5
        price = float(np.round(rng.uniform(10, 200), 2))  # 10–200
        ts = START + timedelta(minutes=int(rng.integers(0, 60*60)))

        # Introduce occasional missing values (to clean later)
        if random() < 0.01:
            qty = None
        if random() < 0.01:
            price = None

        rows.append((pid, cid, qty, price, ts))

    df = pd.DataFrame(rows, columns=["product_id","customer_id","quantity","price","ts"])
    return df

if __name__ == "__main__":
    df = generate_data()
    df.to_csv("transactions_raw.csv", index=False)
    print("Saved transactions_raw.csv with", len(df), "rows")
