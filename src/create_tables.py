import os
import psycopg2

# Read DB connection info from environment variables
conn = psycopg2.connect(
    host=os.getenv("RDS_HOST"),
    port=os.getenv("RDS_PORT"),
    dbname=os.getenv("RDS_DB"),
    user=os.getenv("RDS_USER"),
    password=os.getenv("RDS_PASS"),
)
cur = conn.cursor()

# Create the fact table if it doesn't already exist
cur.execute(
    """
CREATE TABLE IF NOT EXISTS sales_daily (
    order_id TEXT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    quantity INTEGER,
    total_sales NUMERIC,
    order_date DATE
);
"""
)
conn.commit()
cur.close()
conn.close()
print("Table sales_daily ensured in RDS")
