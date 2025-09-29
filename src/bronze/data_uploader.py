#!/usr/bin/env python3
"""
Bronze Layer Data Uploader
Handles uploading local CSV files to S3 Bronze layer with proper partitioning

This module implements the critical data movement from local data/raw files
to the Bronze layer S3 bucket with Medallion architecture partitioning.
"""

import os
import json
import boto3
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from botocore.exceptions import BotoCoreError, ClientError

# Configure logging
logger = logging.getLogger(__name__)


class BronzeDataUploader:
    """
    Handles uploading local CSV files to S3 Bronze layer with proper partitioning

    Key Functions:
    - Upload local CSV files to partitioned Bronze structure
    - Create proper year/month/day directory structure
    - Generate upload metadata and audit trail
    - Handle upload errors and retries
    """

    def __init__(self, bucket_name: str, aws_region: str = "us-east-1"):
        """Initialize Bronze data uploader"""
        self.bucket_name = bucket_name
        self.aws_region = aws_region
        self.s3_client = boto3.client("s3", region_name=aws_region)

        logger.info(f"Bronze uploader initialized for bucket: {bucket_name}")

    def upload_csv_files_to_bronze(
        self, local_data_dir: str, batch_date: str, file_pattern: str = "*.csv"
    ) -> Dict:
        """
        Upload all CSV files from local directory to Bronze layer

        Args:
            local_data_dir: Path to local data directory (e.g., /app/data/raw)
            batch_date: Target batch date (YYYY-MM-DD format)
            file_pattern: File pattern to match (default: *.csv)

        Returns:
            Dict with upload results and metadata
        """
        logger.info(f"Starting Bronze upload for batch: {batch_date}")
        logger.info(f"Source directory: {local_data_dir}")
        logger.info(f"Target bucket: {self.bucket_name}")

        # Validate batch date format
        try:
            datetime.strptime(batch_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError(
                f"Invalid batch_date format: {batch_date}. Expected YYYY-MM-DD"
            )

        # Create Bronze path structure
        year = batch_date[:4]
        month = batch_date[5:7]
        day = batch_date[8:10]
        bronze_prefix = f"sales_data/year={year}/month={month}/day={day}/"

        logger.info(f"Bronze path: s3://{self.bucket_name}/{bronze_prefix}")

        # Find CSV files in local directory
        local_path = Path(local_data_dir)
        if not local_path.exists():
            raise FileNotFoundError(f"Local data directory not found: {local_data_dir}")

        csv_files = list(local_path.glob(file_pattern))
        if not csv_files:
            logger.warning(
                f"No CSV files found in {local_data_dir} with pattern {file_pattern}"
            )
            return {
                "status": "no_files",
                "files_found": 0,
                "files_uploaded": 0,
                "bronze_prefix": bronze_prefix,
            }

        logger.info(f"Found {len(csv_files)} CSV files to upload")

        # Upload results tracking
        upload_results = {
            "batch_date": batch_date,
            "upload_timestamp": datetime.utcnow().isoformat() + "Z",
            "source_directory": str(local_data_dir),
            "bronze_prefix": bronze_prefix,
            "files_found": len(csv_files),
            "files_uploaded": 0,
            "files_failed": 0,
            "upload_details": [],
            "total_size_bytes": 0,
            "status": "in_progress",
        }

        # Upload each CSV file
        for csv_file in csv_files:
            try:
                # Create S3 key with Bronze structure
                s3_key = f"{bronze_prefix}{csv_file.name}"

                # Get file size
                file_size = csv_file.stat().st_size
                upload_results["total_size_bytes"] += file_size

                logger.info(
                    f"Uploading {csv_file.name} ({file_size:,} bytes) to {s3_key}"
                )

                # Upload file to S3
                self.s3_client.upload_file(
                    str(csv_file),
                    self.bucket_name,
                    s3_key,
                    ExtraArgs={
                        "ServerSideEncryption": "AES256",
                        "Metadata": {
                            "batch_date": batch_date,
                            "source_file": csv_file.name,
                            "upload_timestamp": datetime.utcnow().isoformat(),
                            "medallion_layer": "bronze",
                            "file_size_bytes": str(file_size),
                        },
                    },
                )

                # Track successful upload
                upload_results["files_uploaded"] += 1
                upload_results["upload_details"].append(
                    {
                        "file_name": csv_file.name,
                        "s3_key": s3_key,
                        "file_size_bytes": file_size,
                        "status": "success",
                    }
                )

                logger.info(f"Successfully uploaded {csv_file.name}")

            except Exception as e:
                logger.error(f"Failed to upload {csv_file.name}: {e}")
                upload_results["files_failed"] += 1
                upload_results["upload_details"].append(
                    {
                        "file_name": csv_file.name,
                        "s3_key": f"{bronze_prefix}{csv_file.name}",
                        "status": "failed",
                        "error": str(e),
                    }
                )

        # Determine final status
        if upload_results["files_uploaded"] == upload_results["files_found"]:
            upload_results["status"] = "success"
        elif upload_results["files_uploaded"] > 0:
            upload_results["status"] = "partial_success"
        else:
            upload_results["status"] = "failed"

        # Upload metadata file
        try:
            metadata_key = f"{bronze_prefix}_metadata/upload_log.json"
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=metadata_key,
                Body=json.dumps(upload_results, indent=2),
                ContentType="application/json",
                ServerSideEncryption="AES256",
            )
            logger.info(
                f"Upload metadata saved to: s3://{self.bucket_name}/{metadata_key}"
            )
        except Exception as e:
            logger.error(f"Failed to save upload metadata: {e}")

        logger.info(f"Bronze upload completed: {upload_results['status']}")
        logger.info(
            f"Files uploaded: {upload_results['files_uploaded']}/{upload_results['files_found']}"
        )
        logger.info(f"Total size: {upload_results['total_size_bytes']:,} bytes")

        return upload_results

    def verify_bronze_structure(self, batch_date: str) -> Dict:
        """
        Verify that Bronze layer structure exists and contains expected files

        Args:
            batch_date: Batch date to verify (YYYY-MM-DD format)

        Returns:
            Dict with verification results
        """
        year = batch_date[:4]
        month = batch_date[5:7]
        day = batch_date[8:10]
        bronze_prefix = f"sales_data/year={year}/month={month}/day={day}/"

        logger.info(
            f"Verifying Bronze structure: s3://{self.bucket_name}/{bronze_prefix}"
        )

        try:
            # List objects in Bronze path
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=bronze_prefix
            )

            if "Contents" not in response:
                return {
                    "status": "empty",
                    "bronze_prefix": bronze_prefix,
                    "files_found": 0,
                    "message": "No files found in Bronze layer for this batch",
                }

            # Count CSV files
            csv_files = [
                obj for obj in response["Contents"] if obj["Key"].endswith(".csv")
            ]
            total_size = sum(obj["Size"] for obj in csv_files)

            verification_result = {
                "status": "exists",
                "batch_date": batch_date,
                "bronze_prefix": bronze_prefix,
                "files_found": len(csv_files),
                "total_size_bytes": total_size,
                "csv_files": [obj["Key"] for obj in csv_files],
                "verification_timestamp": datetime.utcnow().isoformat() + "Z",
            }

            logger.info(
                f"Bronze verification complete: {len(csv_files)} CSV files found"
            )
            return verification_result

        except Exception as e:
            logger.error(f"Bronze verification failed: {e}")
            return {"status": "error", "bronze_prefix": bronze_prefix, "error": str(e)}

    def create_bronze_sample_data(self, batch_date: str) -> Dict:
        """
        Create sample data in Bronze layer for testing (when no local data available)

        Args:
            batch_date: Target batch date (YYYY-MM-DD format)

        Returns:
            Dict with sample data creation results
        """
        year = batch_date[:4]
        month = batch_date[5:7]
        day = batch_date[8:10]
        bronze_prefix = f"sales_data/year={year}/month={month}/day={day}/"

        logger.info(f"Creating sample Bronze data for testing: {bronze_prefix}")

        # Sample CSV content (24-field schema)
        sample_csv_content = """row_id,order_id,order_date,ship_date,ship_mode,customer_id,customer_name,segment,city,state,country,postal_code,market,region,product_id,category,sub_category,product_name,sales,quantity,discount,profit,shipping_cost,order_priority
1,ORD-123456,15-01-2024,17-01-2024,Standard Class,CUST-12345,John Doe,Consumer,New York,NY,United States,10001,US,East,PROD-1001,Technology,Phones,Sample Phone,299.99,1,0.05,89.99,9.99,Medium
2,ORD-123457,15-01-2024,18-01-2024,First Class,CUST-12346,Jane Smith,Corporate,Los Angeles,CA,United States,90210,US,West,PROD-1002,Furniture,Chairs,Office Chair,149.99,2,0.10,44.99,15.99,High"""

        try:
            # Upload sample CSV file
            sample_key = f"{bronze_prefix}sample_orders.csv"
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=sample_key,
                Body=sample_csv_content,
                ContentType="text/csv",
                ServerSideEncryption="AES256",
                Metadata={
                    "batch_date": batch_date,
                    "data_type": "sample_test_data",
                    "medallion_layer": "bronze",
                    "created_timestamp": datetime.utcnow().isoformat(),
                },
            )

            # Create sample metadata
            sample_metadata = {
                "batch_date": batch_date,
                "bronze_prefix": bronze_prefix,
                "sample_file": sample_key,
                "data_type": "test_sample",
                "record_count": 2,
                "created_timestamp": datetime.utcnow().isoformat() + "Z",
                "purpose": "Bronze layer testing and validation",
            }

            metadata_key = f"{bronze_prefix}_metadata/sample_data_log.json"
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=metadata_key,
                Body=json.dumps(sample_metadata, indent=2),
                ContentType="application/json",
                ServerSideEncryption="AES256",
            )

            logger.info(
                f"Sample Bronze data created: s3://{self.bucket_name}/{sample_key}"
            )
            return {
                "status": "success",
                "sample_key": sample_key,
                "metadata_key": metadata_key,
                "bronze_prefix": bronze_prefix,
            }

        except Exception as e:
            logger.error(f"Failed to create sample Bronze data: {e}")
            return {"status": "failed", "error": str(e)}


# Standalone functions for Airflow integration
def upload_local_data_to_bronze(
    bucket_name: str, local_data_dir: str, batch_date: str
) -> Dict:
    """
    Upload local CSV data to Bronze layer - for Airflow task integration

    Args:
        bucket_name: S3 bucket name for Bronze layer
        local_data_dir: Local directory containing CSV files
        batch_date: Target batch date (YYYY-MM-DD)

    Returns:
        Dict with upload results
    """
    uploader = BronzeDataUploader(bucket_name)

    # Try to upload local data, fallback to sample data if needed
    try:
        result = uploader.upload_csv_files_to_bronze(local_data_dir, batch_date)

        if result["files_uploaded"] == 0:
            logger.warning("No local files uploaded, creating sample data for testing")
            sample_result = uploader.create_bronze_sample_data(batch_date)
            result["sample_data_created"] = sample_result

        return result

    except FileNotFoundError:
        logger.warning(f"Local directory not found: {local_data_dir}")
        logger.info("Creating sample Bronze data for testing")
        return uploader.create_bronze_sample_data(batch_date)


if __name__ == "__main__":
    # Example usage
    import sys

    if len(sys.argv) < 4:
        print(
            "Usage: python data_uploader.py <bucket_name> <local_data_dir> <batch_date>"
        )
        print(
            "Example: python data_uploader.py raw-sales-pipeline-976404003846 /app/data/raw 2024-01-15"
        )
        sys.exit(1)

    bucket_name = sys.argv[1]
    local_data_dir = sys.argv[2]
    batch_date = sys.argv[3]

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    uploader = BronzeDataUploader(bucket_name)
    result = uploader.upload_csv_files_to_bronze(local_data_dir, batch_date)

    print(json.dumps(result, indent=2))
