# src/etl/transform_funcs.py
import pandas as pd
import os, io, boto3, logging
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
