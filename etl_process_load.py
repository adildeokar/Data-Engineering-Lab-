# etl_process_load.py
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from config import CONN_URL

ENGINE = create_engine(CONN_URL, future=True)

def clean_transform(df: pd.DataFrame) -> pd.DataFrame:
    # Drop rows with missing critical fields
    df = df.dropna(subset=["product_id","customer_id","quantity","price","ts"])

    # Enforce types and bounds
    df["product_id"] = df["product_id"].astype(int)
    df["customer_id"] = df["customer_id"].astype(int)
    df["quantity"] = df["quantity"].astype(int)
    df = df[df["quantity"] > 0]
    df["price"] = df["price"].astype(float)
    df["price"] = df["price"].clip(lower=0.0)

    # Compute total_amount if not using GENERATED column
    if "total_amount" not in df.columns:
        df["total_amount"] = (df["quantity"] * df["price"]).round(2)

    # Optional: add a stable hash key to prevent dup loads
    df["natural_key"] = (
        df["product_id"].astype(str) + "-" +
        df["customer_id"].astype(str) + "-" +
        df["quantity"].astype(str) + "-" +
        df["price"].astype(str) + "-" +
        pd.to_datetime(df["ts"]).astype("int64").astype(str)
    )

    return df

def ensure_table():
    with ENGINE.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS transactions (
          transaction_id BIGSERIAL PRIMARY KEY,
          product_id INTEGER NOT NULL,
          customer_id INTEGER NOT NULL,
          quantity INTEGER NOT NULL CHECK (quantity > 0),
          price NUMERIC(10,2) NOT NULL CHECK (price >= 0),
          ts TIMESTAMP NOT NULL,
          total_amount NUMERIC(12,2),
          natural_key TEXT UNIQUE
        );
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_transactions_product ON transactions(product_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_transactions_customer ON transactions(customer_id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_transactions_ts ON transactions(ts);"))

def upsert_transactions(df: pd.DataFrame):
    # Simple upsert by natural_key to avoid duplicates
    tmp = "_tmp_transactions"
    with ENGINE.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {tmp};"))
        conn.execute(text(f"""
            CREATE TEMP TABLE {tmp} (
              product_id INTEGER,
              customer_id INTEGER,
              quantity INTEGER,
              price NUMERIC(10,2),
              ts TIMESTAMP,
              total_amount NUMERIC(12,2),
              natural_key TEXT
            ) ON COMMIT DROP;
        """))
        df[["product_id","customer_id","quantity","price","ts","total_amount","natural_key"]].to_sql(
            tmp, conn, if_exists="append", index=False
        )
        conn.execute(text(f"""
            INSERT INTO transactions (product_id, customer_id, quantity, price, ts, total_amount, natural_key)
            SELECT product_id, customer_id, quantity, price, ts, total_amount, natural_key
            FROM {tmp}
            ON CONFLICT (natural_key) DO NOTHING;
        """))

if __name__ == "__main__":
    ensure_table()
    raw = pd.read_csv("transactions_raw.csv", parse_dates=["ts"])
    df = clean_transform(raw)
    upsert_transactions(df)
    print("Loaded", len(df), "clean rows into transactions")
