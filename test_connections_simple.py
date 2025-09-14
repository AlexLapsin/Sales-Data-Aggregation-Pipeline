#!/usr/bin/env python3
"""
Simple connection tests for cloud services without Unicode characters

This script provides basic connectivity testing for:
- Snowflake (key-pair authentication)
- Databricks (token authentication)
- AWS (boto3 connectivity)
"""

import os
import sys
import json
import time
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Load environment variables
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# Test results
results = {
    "snowflake": {"status": "unknown", "details": {}, "errors": []},
    "databricks": {"status": "unknown", "details": {}, "errors": []},
    "aws": {"status": "unknown", "details": {}, "errors": []},
}


def test_snowflake():
    """Test Snowflake connection"""
    print("Testing Snowflake connection...")

    try:
        import snowflake.connector
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
        from cryptography.hazmat.backends import default_backend

        # Load configuration
        config = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "private_key_path": os.getenv(
                "SNOWFLAKE_PRIVATE_KEY_PATH", "./config/keys/snowflake_private_key.pem"
            ),
            "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "database": os.getenv("SNOWFLAKE_DATABASE", "SALES_DW"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
        }

        # Validate required fields
        if not config["account"] or not config["user"]:
            results["snowflake"]["status"] = "error"
            results["snowflake"]["errors"].append(
                "Missing required Snowflake configuration"
            )
            return

        # Load private key
        key_path = Path(config["private_key_path"])
        if not key_path.exists():
            results["snowflake"]["status"] = "error"
            results["snowflake"]["errors"].append(
                f"Private key file not found: {key_path}"
            )
            return

        with open(key_path, "rb") as key_file:
            private_key = load_pem_private_key(
                key_file.read(), password=None, backend=default_backend()
            )

        private_key_der = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        # Test connection
        start_time = time.time()
        conn = snowflake.connector.connect(
            user=config["user"],
            account=config["account"],
            private_key=private_key_der,
            role=config["role"],
            warehouse=config["warehouse"],
            database=config["database"],
            schema=config["schema"],
            login_timeout=30,
        )

        # Test basic query
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE()")
        user_info = cursor.fetchone()

        connection_time = round(time.time() - start_time, 2)

        results["snowflake"]["status"] = "success"
        results["snowflake"]["details"] = {
            "current_user": user_info[0] if user_info else None,
            "current_role": user_info[1] if user_info else None,
            "current_database": user_info[2] if user_info else None,
            "connection_time": connection_time,
            "account": config["account"],
        }

        cursor.close()
        conn.close()

        print(f"  SUCCESS - Connected as {user_info[0]} in {connection_time}s")

    except ImportError as e:
        results["snowflake"]["status"] = "error"
        results["snowflake"]["errors"].append(
            f"Snowflake dependencies missing: {str(e)}"
        )
        print(f"  ERROR - Missing dependencies: {str(e)}")

    except Exception as e:
        results["snowflake"]["status"] = "error"
        results["snowflake"]["errors"].append(str(e))
        print(f"  ERROR - {str(e)}")


def test_databricks():
    """Test Databricks connection"""
    print("Testing Databricks connection...")

    try:
        import requests

        # Load configuration
        config = {
            "host": os.getenv("DATABRICKS_HOST"),
            "token": os.getenv("DATABRICKS_TOKEN"),
        }

        # Validate required fields
        if not config["host"] or not config["token"]:
            results["databricks"]["status"] = "error"
            results["databricks"]["errors"].append(
                "Missing required Databricks configuration"
            )
            return

        # Normalize host URL
        if not config["host"].startswith(("http://", "https://")):
            config["host"] = f"https://{config['host']}"

        # Test authentication
        headers = {
            "Authorization": f"Bearer {config['token']}",
            "Content-Type": "application/json",
        }

        start_time = time.time()
        response = requests.get(
            f"{config['host']}/api/2.0/preview/scim/v2/Me", headers=headers, timeout=30
        )
        response_time = round(time.time() - start_time, 2)

        if response.status_code == 200:
            user_info = response.json()
            results["databricks"]["status"] = "success"
            results["databricks"]["details"] = {
                "user_name": user_info.get("userName", "unknown"),
                "display_name": user_info.get("displayName", "unknown"),
                "active": user_info.get("active", False),
                "response_time": response_time,
                "host": config["host"],
            }
            print(
                f"  SUCCESS - Connected as {user_info.get('userName')} in {response_time}s"
            )
        else:
            results["databricks"]["status"] = "error"
            results["databricks"]["errors"].append(
                f"Authentication failed: {response.status_code} - {response.text}"
            )
            print(f"  ERROR - Authentication failed: {response.status_code}")

    except ImportError as e:
        results["databricks"]["status"] = "error"
        results["databricks"]["errors"].append(
            f"Required dependencies missing: {str(e)}"
        )
        print(f"  ERROR - Missing dependencies: {str(e)}")

    except Exception as e:
        results["databricks"]["status"] = "error"
        results["databricks"]["errors"].append(str(e))
        print(f"  ERROR - {str(e)}")


def test_aws():
    """Test AWS connection"""
    print("Testing AWS connection...")

    try:
        import boto3
        from botocore.exceptions import ClientError, NoCredentialsError

        # Load configuration
        config = {
            "access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            "secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "region": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            "s3_bucket": os.getenv("S3_BUCKET"),
            "processed_bucket": os.getenv("PROCESSED_BUCKET"),
        }

        # Validate required fields
        if not config["access_key_id"] or not config["secret_access_key"]:
            results["aws"]["status"] = "error"
            results["aws"]["errors"].append("Missing required AWS credentials")
            return

        # Test STS authentication
        start_time = time.time()
        sts_client = boto3.client(
            "sts",
            aws_access_key_id=config["access_key_id"],
            aws_secret_access_key=config["secret_access_key"],
            region_name=config["region"],
        )

        response = sts_client.get_caller_identity()
        auth_time = round(time.time() - start_time, 2)

        # Test S3 access
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=config["access_key_id"],
            aws_secret_access_key=config["secret_access_key"],
            region_name=config["region"],
        )

        start_time = time.time()
        buckets = s3_client.list_buckets()
        s3_time = round(time.time() - start_time, 2)

        # Test specific bucket access
        bucket_access = {}
        for bucket_name in [config["s3_bucket"], config["processed_bucket"]]:
            if bucket_name:
                try:
                    s3_client.head_bucket(Bucket=bucket_name)
                    bucket_access[bucket_name] = "accessible"
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code")
                    if error_code in ["NoSuchBucket", "404"]:
                        bucket_access[bucket_name] = "not_exists"
                    else:
                        bucket_access[bucket_name] = f"error: {error_code}"

        results["aws"]["status"] = "success"
        results["aws"]["details"] = {
            "account_id": response.get("Account"),
            "user_id": response.get("UserId"),
            "arn": response.get("Arn"),
            "region": config["region"],
            "auth_time": auth_time,
            "s3_time": s3_time,
            "bucket_count": len(buckets.get("Buckets", [])),
            "bucket_access": bucket_access,
        }

        print(f"  SUCCESS - Account {response.get('Account')} in {auth_time}s")
        print(f"    S3 buckets found: {len(buckets.get('Buckets', []))}")
        for bucket_name, status in bucket_access.items():
            print(f"    {bucket_name}: {status}")

    except NoCredentialsError:
        results["aws"]["status"] = "error"
        results["aws"]["errors"].append("No AWS credentials found")
        print("  ERROR - No AWS credentials found")

    except ImportError as e:
        results["aws"]["status"] = "error"
        results["aws"]["errors"].append(f"AWS dependencies missing: {str(e)}")
        print(f"  ERROR - Missing dependencies: {str(e)}")

    except Exception as e:
        results["aws"]["status"] = "error"
        results["aws"]["errors"].append(str(e))
        print(f"  ERROR - {str(e)}")


def main():
    """Main function"""
    print("=" * 70)
    print("CLOUD SERVICES CONNECTION TEST")
    print("=" * 70)

    # Run tests
    test_snowflake()
    print()
    test_databricks()
    print()
    test_aws()

    # Summary
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)

    success_count = 0
    total_count = 3

    for service, result in results.items():
        status = result["status"]
        if status == "success":
            status_symbol = "OK"
            success_count += 1
        else:
            status_symbol = "FAIL"

        print(f"{service.upper():12} - {status_symbol}")

        if result["errors"]:
            for error in result["errors"]:
                print(f"    ERROR: {error}")

    print()
    print(f"Overall: {success_count}/{total_count} services connected successfully")

    # Save detailed results
    results_file = Path("connection_test_results.json")
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"Detailed results saved to: {results_file}")

    return success_count == total_count


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
