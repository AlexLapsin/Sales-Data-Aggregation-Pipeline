import os
import io
import boto3
import pandas as pd

# Initialize S3 client and read bucket/prefix from .env
S3 = boto3.client("s3")
BUCKET = os.getenv("S3_BUCKET")
PREFIX = os.getenv("S3_PREFIX", "")  # e.g. "data/"

# List all CSV files under the given prefix
def get_data_files():
    resp = S3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    keys = [
        o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith("_orders.csv")
    ]
    return sorted(keys)


# Download and parse a single CSV file from S3
def load_region_csv(key: str) -> pd.DataFrame:
    obj = S3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


# Main execution: read and combine all regional CSVs
def main():
    keys = get_data_files()
    df_list = [load_region_csv(key) for key in keys]
    df_raw = pd.concat(df_list, ignore_index=True)
    print(f"Read {len(keys)} files, total {len(df_raw)} rows")
    # Optional: write locally for inspection
    # df_raw.to_csv("raw_combined.csv", index=False)


if __name__ == "__main__":
    main()
