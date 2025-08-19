#!/usr/bin/env python3
from dotenv import load_dotenv
import os
import boto3
import psycopg2
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    # Load environment variables from .env
    load_dotenv()

    # S3 connectivity check
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        logger.error("S3_BUCKET is not set. Cannot perform S3 connectivity check.")
        raise RuntimeError("Missing S3_BUCKET env variable")
    try:
        s3_client = boto3.client("s3")
        # Attempt to list objects to verify bucket access (limit to 1 result)
        s3_client.list_objects_v2(Bucket=bucket, MaxKeys=1)
        logger.info(f"Successfully accessed S3 bucket '{bucket}'.")
    except Exception as e:
        logger.error(f"Failed to access S3 bucket '{bucket}': {e}")
        raise

    # RDS connectivity check
    try:
        conn = psycopg2.connect(
            host=os.getenv("RDS_HOST"),
            port=os.getenv("RDS_PORT", "5432"),
            dbname=os.getenv("RDS_DB"),
            user=os.getenv("RDS_USER"),
            password=os.getenv("RDS_PASS"),
        )
        conn.close()
        logger.info("Successfully connected to the RDS database.")
    except Exception as e:
        logger.error(f"Failed to connect to the RDS database: {e}")
        raise

    logger.info("Preflight check passed: S3 bucket and RDS database are reachable.")


if __name__ == "__main__":
    main()
