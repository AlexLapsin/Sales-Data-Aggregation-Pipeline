# src/etl/transform_funcs.py
import pandas as pd
import os
import io
import boto3
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

DEFAULT_THRESHOLD = float(os.getenv("SALES_THRESHOLD", "10000"))


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse 'Order Date' & 'Ship Date' to datetime, coercing errors.
    """
    df = df.copy()
    df["Order Date"] = pd.to_datetime(df["Order Date"], dayfirst=True, errors="coerce")
    df["Ship Date"] = pd.to_datetime(df["Ship Date"], dayfirst=True, errors="coerce")
    return df


def clean_basic(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only rows with positive quantity & sales and non-null dates.
    """
    return df.query(
        "Quantity > 0 and Sales > 0 and `Order Date`.notnull() and `Ship Date`.notnull()"
    ).copy()


def cap_extremes(
    df: pd.DataFrame, threshold: float = DEFAULT_THRESHOLD
) -> pd.DataFrame:
    """
    Filter out rows where Sales exceed the threshold.
    """
    # Only keep rows with Sales <= threshold
    return df[df["Sales"] <= threshold].copy()


def derive_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add unit_price and profit_margin.
    """
    df = df.copy()
    df["unit_price"] = (df["Sales"] / df["Quantity"]).round(2)
    df["profit_margin"] = (df["Profit"] / df["Sales"]).round(2)
    return df


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename Camel/space names to snake_case matching SQL schema.
    """
    return df.rename(
        columns={
            "Region": "region",
            "Country": "country",
            "Order ID": "order_id",
            "Order Date": "order_date",
            "Ship Date": "ship_date",
            "Customer ID": "customer_id",
            "Product ID": "product_id",
            "Category": "category",
            "Quantity": "quantity",
            "Sales": "total_sales",
            "Profit": "profit",
        }
    )


def df_to_s3_parquet(df: pd.DataFrame, bucket: str, key: str) -> None:
    """Write a DataFrame to S3 as Parquet (in-memory). Requires pyarrow."""
    bio = io.BytesIO()
    df.to_parquet(bio, index=False)
    bio.seek(0)
    boto3.client("s3").upload_fileobj(bio, bucket, key)
    logger.info("Uploaded parquet -> s3://%s/%s (%d rows)", bucket, key, len(df))


def process_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """Complete transformation pipeline for sales data."""
    # Apply transformations in sequence
    df = parse_dates(df)
    df = clean_basic(df)
    df = cap_extremes(df)
    df = derive_fields(df)
    df = rename_columns(df)
    return df


def create_star_schema(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Create star schema tables from processed sales data."""
    # This is a simplified star schema - in practice you'd have more sophisticated logic

    # Fact table: sales transactions
    fact_sales = df[
        [
            "order_date",
            "ship_date",
            "sales",
            "profit",
            "quantity",
            "product_id",
            "category",
        ]
    ].copy()

    # Dimension table: dates
    all_dates = (
        pd.concat([df["order_date"].dropna(), df["ship_date"].dropna()])
        .drop_duplicates()
        .sort_values()
    )

    dim_date = pd.DataFrame(
        {
            "date": all_dates,
            "year": all_dates.dt.year,
            "month": all_dates.dt.month,
            "quarter": all_dates.dt.quarter,
            "day_of_week": all_dates.dt.dayofweek,
        }
    )

    # Dimension table: products
    dim_product = (
        df[["product_id", "category"]].drop_duplicates().reset_index(drop=True)
    )

    return fact_sales, dim_date, dim_product
