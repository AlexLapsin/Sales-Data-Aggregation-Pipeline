# src/etl/load_funcs.py
import os
import io
import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()


def s3_parquet_to_df(bucket: str, key: str) -> pd.DataFrame:
    """Read a Parquet object from S3 into a DataFrame (in-memory). Requires pyarrow."""
    obj = boto3.client("s3").get_object(Bucket=bucket, Key=key)
    bio = io.BytesIO(obj["Body"].read())
    return pd.read_parquet(bio)


def get_rds_conn():
    """Return a psycopg2 connection to the RDS instance."""
    return psycopg2.connect(
        host=os.getenv("RDS_HOST"),
        port=os.getenv("RDS_PORT"),
        dbname=os.getenv("RDS_DB"),
        user=os.getenv("RDS_USER"),
        password=os.getenv("RDS_PASS"),
    )


def truncate_tables(conn):
    """Truncate date, product, and fact tables, resetting their sequences."""
    with conn.cursor() as cur:
        for tbl in ("fact_sales", "dim_product", "dim_date"):
            cur.execute(f"TRUNCATE {tbl} RESTART IDENTITY CASCADE;")
        conn.commit()


def load_dim_date(df: pd.DataFrame, conn):
    """Bulk insert date dimension rows."""
    sql = "INSERT INTO dim_date(order_date,day,month,quarter,year) VALUES (%s,%s,%s,%s,%s)"
    with conn.cursor() as cur:
        execute_batch(cur, sql, df.values.tolist())
    conn.commit()


def load_dim_product(df: pd.DataFrame, conn):
    """Bulk insert product dimension rows."""
    sql = "INSERT INTO dim_product(product_id,category) VALUES (%s,%s)"
    with conn.cursor() as cur:
        execute_batch(cur, sql, df.values.tolist())
    conn.commit()


def load_fact_sales(df: pd.DataFrame, conn):
    """Map natural keys to surrogate and bulk insert fact rows."""
    # Ensure order_date is datetime on both sides
    # Read and parse dates from dimension as datetime64
    dates = pd.read_sql(
        "SELECT date_id, order_date FROM dim_date", conn, parse_dates=["order_date"]
    )
    # Read products (product_id can stay as text)
    prods = pd.read_sql("SELECT product_sk, product_id FROM dim_product", conn)
    # Ensure dataframe's order_date is datetime64
    df["order_date"] = pd.to_datetime(df["order_date"])
    # Merge to get surrogate keys
    df_m = df.merge(dates, on="order_date").merge(prods, on="product_id")
    # Define columns in fact
    cols = [
        "date_id",
        "product_sk",
        "quantity",
        "total_sales",
        "profit",
        "unit_price",
        "profit_margin",
    ]
    sql = (
        f"INSERT INTO fact_sales({','.join(cols)}) "
        f"VALUES ({','.join(['%s']*len(cols))})"
    )
    with conn.cursor() as cur:
        execute_batch(cur, sql, df_m[cols].values.tolist())
    conn.commit()


def create_tables(conn) -> None:
    """Create all necessary tables for the star schema."""
    with conn.cursor() as cur:
        # Create dim_date table
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dim_date (
                date DATE PRIMARY KEY,
                year INTEGER,
                month INTEGER,
                quarter INTEGER,
                day_of_week INTEGER
            )
        """
        )

        # Create dim_product table
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dim_product (
                product_id VARCHAR(255) PRIMARY KEY,
                category VARCHAR(255)
            )
        """
        )

        # Create fact_sales table
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS fact_sales (
                id SERIAL PRIMARY KEY,
                order_date DATE,
                ship_date DATE,
                sales DECIMAL(10,2),
                profit DECIMAL(10,2),
                quantity INTEGER,
                unit_price DECIMAL(10,2),
                profit_margin DECIMAL(5,4),
                product_id VARCHAR(255) REFERENCES dim_product(product_id)
            )
        """
        )

    conn.commit()


def load_to_postgres(
    fact_sales: pd.DataFrame, dim_date: pd.DataFrame, dim_product: pd.DataFrame
) -> None:
    """Load star schema data to PostgreSQL."""
    conn = get_rds_conn()

    try:
        # Create tables if they don't exist
        create_tables(conn)

        # Truncate existing data
        truncate_tables(conn)

        # Load data
        load_dim_date(dim_date, conn)
        load_dim_product(dim_product, conn)
        load_fact_sales(fact_sales, conn)

    finally:
        conn.close()
