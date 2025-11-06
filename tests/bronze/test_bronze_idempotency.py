#!/usr/bin/env python3
"""
Test script to verify enhanced Bronze layer idempotent behavior
"""

import os
import sys
import logging

sys.path.append("src")

from bronze.data_uploader import BronzeDataUploader

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def test_idempotent_behavior():
    """Test Bronze layer idempotent behavior with sample data"""

    bucket_name = "raw-sales-pipeline-976404003846"
    test_batch_date = "2024-01-15"

    # Initialize Bronze uploader with enhanced capabilities
    uploader = BronzeDataUploader(bucket_name)

    print("\n=== Testing Enhanced Bronze Layer Idempotent Behavior ===")

    # Test 1: Create sample data for testing
    print("\n1. Creating sample Bronze data for testing...")
    sample_result = uploader.create_bronze_sample_data(test_batch_date)
    print(f"Sample data creation: {sample_result['status']}")
    if sample_result["status"] == "success":
        print(f"Sample file created: {sample_result['sample_key']}")

    # Test 2: Verify Bronze structure
    print("\n2. Verifying Bronze structure...")
    verify_result = uploader.verify_bronze_structure(test_batch_date)
    print(f"Bronze verification: {verify_result['status']}")
    if verify_result["status"] == "exists":
        print(f"Files found: {verify_result['files_found']}")
        print(f"Total size: {verify_result['total_size_bytes']:,} bytes")

    # Test 3: Try to upload same data again (should be idempotent)
    print("\n3. Testing idempotent behavior by re-running sample creation...")
    duplicate_result = uploader.create_bronze_sample_data(test_batch_date)
    print(f"Duplicate upload attempt: {duplicate_result['status']}")

    print("\n=== Bronze Layer Test Complete ===")
    return True


if __name__ == "__main__":
    test_idempotent_behavior()
