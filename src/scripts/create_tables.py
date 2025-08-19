#!/usr/bin/env python3
from dotenv import load_dotenv
import os
import psycopg2
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DDL_DIM_DATE = """
CREATE TABLE IF NOT EXISTS dim_date (
    date_id    SERIAL PRIMARY KEY,
    order_date DATE UNIQUE,
    day        INT,
    month      INT,
    quarter    INT,
    year       INT
);
"""

DDL_DIM_PRODUCT = """
CREATE TABLE IF NOT EXISTS dim_product (
    product_sk  SERIAL PRIMARY KEY,
    product_id  TEXT UNIQUE,
    category    TEXT
);
"""

DDL_FACT_SALES = """
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


def main():
    load_dotenv()

    conn = psycopg2.connect(
        host=os.getenv("RDS_HOST"),
        port=os.getenv("RDS_PORT", "5432"),
        dbname=os.getenv("RDS_DB"),
        user=os.getenv("RDS_USER"),
        password=os.getenv("RDS_PASS"),
    )
    try:
        with conn, conn.cursor() as cur:
            logger.info("Ensuring target schema exists (idempotent).")
            cur.execute(DDL_DIM_DATE)
            cur.execute(DDL_DIM_PRODUCT)
            cur.execute(DDL_FACT_SALES)
        logger.info("Schema is ready.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
