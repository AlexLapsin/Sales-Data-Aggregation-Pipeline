# src/etl/extract_funcs.py
import os
import io
import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()  # ← load .env immediately

# Initialize S3 client;
s3_client = boto3.client("s3")
BUCKET = os.environ["S3_BUCKET"]
PREFIX = os.environ.get("S3_PREFIX", "")


def get_data_files():
    """
    List all CSV keys under S3 prefix ending with '_orders.csv'.
    """
    resp = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    keys = [
        o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith("_orders.csv")
    ]
    return sorted(keys)


def load_region_csv(key: str) -> pd.DataFrame:
    """
    Download one CSV from S3 and return as DataFrame.
    """
    obj = s3_client.get_object(Bucket=BUCKET, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def extract_all() -> pd.DataFrame:
    """
    Fetch all regional CSVs and concatenate into one DataFrame.
    """
    keys = get_data_files()
    dfs = [load_region_csv(k) for k in keys]
    return pd.concat(dfs, ignore_index=True)
