import pandas as pd
import psycopg2
import logging

# ---------------- Logging ----------------
logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------------- DB Config ----------------
DB_CONFIG = {
    "host": "localhost",
    "database": "ecommerce_db",
    "user": "postgres",
    "password": "102011",
    "port": 5433
}

# ---------------- Extract ----------------
def extract_data():
    logging.info("Extracting data from CSV")
    return pd.read_csv("data/sampledata.csv")

# ---------------- Transform ----------------
def transform_data(df):
    logging.info("Transforming data")
    df["total_amount"] = df["quantity"] * df["price"]
    return df

# ---------------- Load ----------------
def load_data(df):
    logging.info("Loading data into PostgreSQL")

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO orders (
        order_id, customer_name, product,
        quantity, price, order_date, total_amount
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (order_id) DO NOTHING;
    """

    for _, row in df.iterrows():
        cursor.execute(insert_query, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()

    logging.info("Load completed")

# ---------------- Runner ----------------
def run_pipeline():
    df = extract_data()
    df = transform_data(df)
    load_data(df)

if __name__ == "__main__":
    run_pipeline()