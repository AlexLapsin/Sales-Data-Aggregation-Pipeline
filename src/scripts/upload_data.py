#!/usr/bin/env python3
from dotenv import load_dotenv
import os
import logging
from pathlib import Path
import boto3
from botocore.exceptions import BotoCoreError, ClientError

# Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    load_dotenv()

    # Always read from the container mount point
    base_dir = Path(os.getenv("LOCAL_DATA_DIR", "/app/data"))
    raw_dir = base_dir / "raw"

    bucket = os.getenv("S3_BUCKET")
    prefix = os.getenv("S3_PREFIX", "") or ""
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    logger.info("Resolved local data dir: %s", base_dir)
    logger.info("Looking for raw CSVs in: %s", raw_dir)

    if not bucket:
        raise RuntimeError("S3_BUCKET not set. Cannot upload files to S3.")
    if not raw_dir.exists():
        raise FileNotFoundError(f"Local raw data directory does not exist: {raw_dir}")

    files = sorted(raw_dir.glob("*.csv"))
    if not files:
        logger.warning("No CSV files found in %s; nothing to upload.", raw_dir)
        return

    s3 = boto3.client("s3")
    logger.info("Uploading %d CSVs to s3://%s/%s", len(files), bucket, prefix)

    for fp in files:
        key = f"{prefix}{fp.name}" if prefix else fp.name
        try:
            s3.upload_file(str(fp), bucket, key, ExtraArgs={"ContentType": "text/csv"})
            logger.info("Uploaded %s -> s3://%s/%s", fp.name, bucket, key)
        except (BotoCoreError, ClientError) as e:
            logger.error("Failed to upload %s: %s", fp.name, e)
            raise

    logger.info("Successfully uploaded %d file(s).", len(files))


if __name__ == "__main__":
    main()
