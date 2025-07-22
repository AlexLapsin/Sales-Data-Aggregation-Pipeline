# src/scripts/create_tables.py
import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()
conn = psycopg2.connect(
    host=os.getenv("RDS_HOST"),
    port=os.getenv("RDS_PORT"),
    dbname=os.getenv("RDS_DB"),
    user=os.getenv("RDS_USER"),
    password=os.getenv("RDS_PASS"),
)
cur = conn.cursor()

# DROP old tables so we can recreate with new schema
cur.execute("DROP TABLE IF EXISTS fact_sales CASCADE;")
cur.execute("DROP TABLE IF EXISTS dim_product CASCADE;")
cur.execute("DROP TABLE IF EXISTS dim_date CASCADE;")
conn.commit()

# dim_date
cur.execute(
    """
CREATE TABLE IF NOT EXISTS dim_date (
    date_id    SERIAL PRIMARY KEY,
    order_date DATE     UNIQUE,
    day        INT,
    month      INT,
    quarter    INT,
    year       INT
);
"""
)

# dim_product
cur.execute(
    """
CREATE TABLE IF NOT EXISTS dim_product (
    product_sk  SERIAL PRIMARY KEY,
    product_id  TEXT UNIQUE,
    category    TEXT
);
"""
)

# fact_sales
cur.execute(
    """
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id        SERIAL PRIMARY KEY,
    date_id        INT NOT NULL REFERENCES dim_date(date_id),
    product_sk     INT NOT NULL REFERENCES dim_product(product_sk),
    quantity       INT,
    total_sales    NUMERIC,
    profit         NUMERIC,
    unit_price     NUMERIC,
    profit_margin  NUMERIC
);
"""
)

conn.commit()
cur.close()
conn.close()
print("[create_tables] Schema ensured in RDS")
