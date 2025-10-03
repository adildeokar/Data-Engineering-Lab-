# analyze_visualize.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from config import CONN_URL

ENGINE = create_engine(CONN_URL, future=True)

def fetch_sales_per_product():
    query = """
    SELECT product_id, SUM(quantity*price) AS total_sales
    FROM transactions
    GROUP BY product_id
    ORDER BY product_id;
    """
    return pd.read_sql(query, ENGINE)

def fetch_top_customers(limit=5):
    query = f"""
    SELECT customer_id, SUM(quantity*price) AS total_spent
    FROM transactions
    GROUP BY customer_id
    ORDER BY total_spent DESC
    LIMIT {limit};
    """
    return pd.read_sql(query, ENGINE)

if __name__ == "__main__":
    sales_df = fetch_sales_per_product()
    print(sales_df)

    sns.set(style="whitegrid")
    plt.figure(figsize=(10,6))
    plt.bar(sales_df["product_id"].astype(str), sales_df["total_sales"])
    plt.xlabel("Product ID")
    plt.ylabel("Total Sales")
    plt.title("Sales per Product")
    plt.tight_layout()
    plt.savefig("sales_per_product.png", dpi=160)
    plt.show()

    top5 = fetch_top_customers(5)
    print(top5)
