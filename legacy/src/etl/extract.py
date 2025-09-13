# src/etl/extract_funcs.py
import os
import io
import boto3
import pandas as pd
from dotenv import load_dotenv
from typing import Optional

# Global variables for lazy initialization
_s3_client: Optional[boto3.client] = None
_bucket: Optional[str] = None
_prefix: Optional[str] = None


def _get_s3_client():
    """Lazy initialization of S3 client and configuration."""
    global _s3_client, _bucket, _prefix

    if _s3_client is None:
        load_dotenv()  # Load .env when first needed
        _s3_client = boto3.client("s3")
        _bucket = os.environ["S3_BUCKET"]
        _prefix = os.environ.get("S3_PREFIX", "")

    return _s3_client, _bucket, _prefix


def get_data_files():
    """
    List all CSV keys under S3 prefix ending with '.csv'.
    """
    s3_client, bucket, prefix = _get_s3_client()
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys = [
        o["Key"] for o in resp.get("Contents", []) if o["Key"].lower().endswith(".csv")
    ]
    return sorted(keys)


def load_region_csv(key: str) -> pd.DataFrame:
    """
    Download one CSV from S3 and return as DataFrame.
    """
    s3_client, bucket, _ = _get_s3_client()
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def extract_all() -> pd.DataFrame:
    """
    Fetch all regional CSVs and concatenate into one DataFrame.
    """
    keys = get_data_files()
    dfs = [load_region_csv(k) for k in keys]
    return pd.concat(dfs, ignore_index=True)
