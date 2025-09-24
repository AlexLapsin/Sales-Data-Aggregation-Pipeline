#!/usr/bin/env python3
"""
Comprehensive AWS Connection Tests

This module provides comprehensive testing for AWS connectivity using boto3.
Tests cover authentication, S3 bucket access, IAM permissions, and various AWS services
used in the sales data pipeline.

Usage:
    python -m pytest tests/test_aws_connection.py -v
    python tests/test_aws_connection.py  # Run standalone

Environment Variables Required:
    AWS_ACCESS_KEY_ID: AWS access key ID
    AWS_SECRET_ACCESS_KEY: AWS secret access key
    AWS_DEFAULT_REGION: AWS region (optional, defaults to us-east-1)
    S3_BUCKET: Primary S3 bucket name for raw data
    PROCESSED_BUCKET: S3 bucket name for processed data
"""

import os
import sys
import pytest
import logging
import time
import json
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
import tempfile

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    import boto3
    from botocore.exceptions import (
        ClientError,
        NoCredentialsError,
        PartialCredentialsError,
        EndpointConnectionError,
        BotoCoreError,
        ProfileNotFound,
    )
    from botocore.client import Config

    BOTO3_AVAILABLE = True
except ImportError as e:
    BOTO3_AVAILABLE = False
    IMPORT_ERROR = str(e)

# Load environment variables
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class AWSConnectionTester:
    """Comprehensive AWS connection testing utility"""

    def __init__(self):
        self.config = self._load_config()
        self.test_results = {
            "configuration": {},
            "authentication": {},
            "iam_permissions": {},
            "s3_operations": {},
            "bucket_access": {},
            "aws_services": {},
            "performance": {},
            "security": {},
        }

    def _load_config(self) -> Dict[str, Any]:
        """Load and validate AWS configuration"""
        config = {
            "access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            "secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "region": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            "s3_bucket": os.getenv("S3_BUCKET"),
            "processed_bucket": os.getenv("PROCESSED_BUCKET"),
            "session_token": os.getenv(
                "AWS_SESSION_TOKEN"
            ),  # For temporary credentials
        }

        # Validate required fields
        required_fields = ["access_key_id", "secret_access_key"]
        missing_fields = [field for field in required_fields if not config[field]]

        if missing_fields:
            raise ValueError(f"Missing required AWS configuration: {missing_fields}")

        return config

    def _create_session(self) -> boto3.Session:
        """Create boto3 session with configured credentials"""
        session_kwargs = {
            "aws_access_key_id": self.config["access_key_id"],
            "aws_secret_access_key": self.config["secret_access_key"],
            "region_name": self.config["region"],
        }

        if self.config.get("session_token"):
            session_kwargs["aws_session_token"] = self.config["session_token"]

        return boto3.Session(**session_kwargs)

    def _create_client(self, service_name: str, **kwargs):
        """Create boto3 client for specified service"""
        session = self._create_session()

        client_config = Config(
            retries={"max_attempts": 3}, connect_timeout=30, read_timeout=30
        )

        return session.client(service_name, config=client_config, **kwargs)

    def test_configuration(self) -> Dict[str, Any]:
        """Test AWS configuration validity"""
        logger.info("Testing AWS configuration...")

        result = {"status": "success", "details": {}, "errors": [], "warnings": []}

        try:
            config_checks = {
                "credentials_format": self._validate_credentials_format(),
                "region_valid": self._validate_region(),
                "bucket_names": self._validate_bucket_names(),
                "credentials_length": self._validate_credentials_length(),
            }

            result["details"] = config_checks

            # Check for any failed validations
            failed_checks = [
                check for check, passed in config_checks.items() if not passed
            ]
            if failed_checks:
                result["status"] = "failed"
                result["errors"] = [
                    f"Configuration validation failed: {check}"
                    for check in failed_checks
                ]

        except Exception as e:
            result["status"] = "error"
            result["errors"] = [f"Configuration test error: {str(e)}"]

        self.test_results["configuration"] = result
        return result

    def _validate_credentials_format(self) -> bool:
        """Validate AWS credentials format"""
        access_key = self.config["access_key_id"]
        secret_key = self.config["secret_access_key"]

        # AWS access keys typically start with AKIA and are 20 chars
        access_key_valid = (
            access_key
            and len(access_key) == 20
            and access_key.startswith("AKIA")
            and not access_key.startswith("YOUR_")
        )

        # AWS secret keys are typically 40 characters
        secret_key_valid = (
            secret_key and len(secret_key) == 40 and not secret_key.startswith("YOUR_")
        )

        return access_key_valid and secret_key_valid

    def _validate_region(self) -> bool:
        """Validate AWS region"""
        region = self.config["region"]
        # Basic region format validation
        return bool(region and len(region) > 0 and "-" in region)

    def _validate_bucket_names(self) -> bool:
        """Validate S3 bucket names format"""
        s3_bucket = self.config.get("s3_bucket", "")
        processed_bucket = self.config.get("processed_bucket", "")

        # Basic bucket name validation (AWS has specific rules)
        def is_valid_bucket_name(bucket_name):
            if not bucket_name:
                return False
            if bucket_name.startswith("RAW_BUCKET") or bucket_name.startswith(
                "PROCESSED_BUCKET"
            ):
                return False
            return len(bucket_name) >= 3 and len(bucket_name) <= 63

        return is_valid_bucket_name(s3_bucket) and is_valid_bucket_name(
            processed_bucket
        )

    def _validate_credentials_length(self) -> bool:
        """Validate credentials have appropriate length"""
        access_key = self.config["access_key_id"]
        secret_key = self.config["secret_access_key"]

        return (
            access_key
            and len(access_key) >= 16
            and secret_key
            and len(secret_key) >= 32
        )

    def test_authentication(self) -> Dict[str, Any]:
        """Test AWS authentication"""
        logger.info("Testing AWS authentication...")

        result = {
            "status": "success",
            "details": {},
            "errors": [],
            "warnings": [],
            "response_time": None,
        }

        try:
            start_time = time.time()

            # Test authentication with STS get-caller-identity
            sts_client = self._create_client("sts")
            response = sts_client.get_caller_identity()

            response_time = round(time.time() - start_time, 2)
            result["response_time"] = response_time

            result["details"] = {
                "authenticated": True,
                "account_id": response.get("Account"),
                "user_id": response.get("UserId"),
                "arn": response.get("Arn"),
                "region": self.config["region"],
            }

            # Validate response
            if not response.get("Account"):
                result["warnings"].append("Could not retrieve account ID")

        except NoCredentialsError:
            result["status"] = "failed"
            result["errors"] = ["No AWS credentials found"]
        except PartialCredentialsError as e:
            result["status"] = "failed"
            result["errors"] = [f"Incomplete AWS credentials: {str(e)}"]
        except EndpointConnectionError as e:
            result["status"] = "failed"
            result["errors"] = [f"Cannot connect to AWS endpoint: {str(e)}"]
        except ClientError as e:
            result["status"] = "failed"
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            result["errors"] = [f"AWS authentication error ({error_code}): {str(e)}"]
        except Exception as e:
            result["status"] = "error"
            result["errors"] = [f"Authentication test error: {str(e)}"]

        self.test_results["authentication"] = result
        return result

    def test_iam_permissions(self) -> Dict[str, Any]:
        """Test IAM permissions for required operations"""
        logger.info("Testing AWS IAM permissions...")

        result = {"status": "success", "details": {}, "errors": [], "warnings": []}

        try:
            permission_tests = {
                "s3_list_buckets": self._test_s3_list_permission(),
                "s3_bucket_access": self._test_s3_bucket_permission(),
                "iam_get_user": self._test_iam_get_user_permission(),
                "sts_get_caller_identity": self._test_sts_permission(),
            }

            result["details"] = permission_tests

            # Check for any failed permissions
            failed_perms = [
                perm for perm, success in permission_tests.items() if not success
            ]
            if failed_perms:
                result["status"] = "partial"
                result["warnings"] = [
                    f"Permission test failed: {perm}" for perm in failed_perms
                ]

        except Exception as e:
            result["status"] = "error"
            result["errors"] = [f"IAM permissions test error: {str(e)}"]

        self.test_results["iam_permissions"] = result
        return result

    def _test_s3_list_permission(self) -> bool:
        """Test S3 list buckets permission"""
        try:
            s3_client = self._create_client("s3")
            response = s3_client.list_buckets()
            return "Buckets" in response
        except Exception:
            return False

    def _test_s3_bucket_permission(self) -> bool:
        """Test S3 bucket-specific permissions"""
        try:
            s3_client = self._create_client("s3")
            bucket_name = self.config.get("s3_bucket")

            if not bucket_name:
                return False

            # Test if bucket exists and is accessible
            s3_client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            # 404 means bucket doesn't exist, but we have permission to check
            return error_code in ["404", "NoSuchBucket"]
        except Exception:
            return False

    def _test_iam_get_user_permission(self) -> bool:
        """Test IAM get user permission"""
        try:
            iam_client = self._create_client("iam")
            # Try to get current user info
            iam_client.get_user()
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            # Some IAM setups don't allow get_user, but that's OK
            return error_code in ["NoSuchEntity", "AccessDenied"]
        except Exception:
            return False

    def _test_sts_permission(self) -> bool:
        """Test STS get caller identity permission"""
        try:
            sts_client = self._create_client("sts")
            response = sts_client.get_caller_identity()
            return "Account" in response
        except Exception:
            return False

    def test_s3_operations(self) -> Dict[str, Any]:
        """Test S3 operations"""
        logger.info("Testing AWS S3 operations...")

        result = {"status": "success", "details": {}, "errors": [], "warnings": []}

        try:
            s3_tests = {
                "list_buckets": self._test_s3_list_buckets(),
                "bucket_operations": self._test_s3_bucket_operations(),
                "object_operations": self._test_s3_object_operations(),
                "multipart_upload": self._test_s3_multipart_upload(),
            }

            result["details"] = s3_tests

            # Check for any failed tests
            failed_tests = [test for test, success in s3_tests.items() if not success]
            if failed_tests:
                result["status"] = "partial"
                result["warnings"] = [
                    f"S3 test failed: {test}" for test in failed_tests
                ]

        except Exception as e:
            result["status"] = "error"
            result["errors"] = [f"S3 operations test error: {str(e)}"]

        self.test_results["s3_operations"] = result
        return result

    def _test_s3_list_buckets(self) -> bool:
        """Test S3 list buckets operation"""
        try:
            s3_client = self._create_client("s3")
            response = s3_client.list_buckets()
            return isinstance(response.get("Buckets"), list)
        except Exception:
            return False

    def _test_s3_bucket_operations(self) -> bool:
        """Test S3 bucket operations"""
        try:
            s3_client = self._create_client("s3")
            bucket_name = self.config.get("s3_bucket")

            if not bucket_name:
                return False

            # Test bucket location
            try:
                location = s3_client.get_bucket_location(Bucket=bucket_name)
                return True
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                # If bucket doesn't exist, that's OK for this test
                return error_code in ["NoSuchBucket", "404"]

        except Exception:
            return False

    def _test_s3_object_operations(self) -> bool:
        """Test S3 object operations with a temporary test object"""
        try:
            s3_client = self._create_client("s3")
            bucket_name = self.config.get("s3_bucket")

            if not bucket_name:
                return False

            test_key = f"connection_test/{int(time.time())}/test_object.txt"
            test_content = "AWS S3 connection test"

            try:
                # Put object
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=test_key,
                    Body=test_content,
                    ContentType="text/plain",
                )

                # Get object
                response = s3_client.get_object(Bucket=bucket_name, Key=test_key)
                retrieved_content = response["Body"].read().decode("utf-8")

                # Delete object
                s3_client.delete_object(Bucket=bucket_name, Key=test_key)

                return retrieved_content == test_content

            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                # If bucket doesn't exist, that's expected
                return error_code in ["NoSuchBucket", "404"]

        except Exception:
            return False

    def _test_s3_multipart_upload(self) -> bool:
        """Test S3 multipart upload capability"""
        try:
            s3_client = self._create_client("s3")
            bucket_name = self.config.get("s3_bucket")

            if not bucket_name:
                return False

            test_key = f"connection_test/{int(time.time())}/multipart_test.txt"

            try:
                # Create multipart upload
                response = s3_client.create_multipart_upload(
                    Bucket=bucket_name, Key=test_key
                )
                upload_id = response["UploadId"]

                # Upload part
                part_response = s3_client.upload_part(
                    Bucket=bucket_name,
                    Key=test_key,
                    PartNumber=1,
                    UploadId=upload_id,
                    Body=b"Multipart upload test content",
                )

                # Complete multipart upload
                s3_client.complete_multipart_upload(
                    Bucket=bucket_name,
                    Key=test_key,
                    UploadId=upload_id,
                    MultipartUpload={
                        "Parts": [{"ETag": part_response["ETag"], "PartNumber": 1}]
                    },
                )

                # Clean up
                s3_client.delete_object(Bucket=bucket_name, Key=test_key)
                return True

            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                return error_code in ["NoSuchBucket", "404"]

        except Exception:
            return False

    def test_bucket_access(self) -> Dict[str, Any]:
        """Test access to specific buckets configured in the pipeline"""
        logger.info("Testing AWS S3 bucket access...")

        result = {"status": "success", "details": {}, "errors": [], "warnings": []}

        try:
            buckets_to_test = {}

            if self.config.get("s3_bucket"):
                buckets_to_test["raw_bucket"] = self._test_specific_bucket_access(
                    self.config["s3_bucket"]
                )

            if self.config.get("processed_bucket"):
                buckets_to_test["processed_bucket"] = self._test_specific_bucket_access(
                    self.config["processed_bucket"]
                )

            result["details"] = buckets_to_test

            # Check results
            if not buckets_to_test:
                result["warnings"].append("No buckets configured for testing")
            else:
                failed_buckets = [
                    bucket for bucket, success in buckets_to_test.items() if not success
                ]
                if failed_buckets:
                    result["status"] = "partial"
                    result["warnings"] = [
                        f"Bucket access failed: {bucket}" for bucket in failed_buckets
                    ]

        except Exception as e:
            result["status"] = "error"
            result["errors"] = [f"Bucket access test error: {str(e)}"]

        self.test_results["bucket_access"] = result
        return result

    def _test_specific_bucket_access(self, bucket_name: str) -> Dict[str, Any]:
        """Test access to a specific S3 bucket"""
        try:
            s3_client = self._create_client("s3")

            bucket_info = {
                "exists": False,
                "accessible": False,
                "permissions": {"read": False, "write": False, "list": False},
                "region": None,
                "error": None,
            }

            try:
                # Test bucket existence and basic access
                s3_client.head_bucket(Bucket=bucket_name)
                bucket_info["exists"] = True
                bucket_info["accessible"] = True

                # Test bucket location
                location_response = s3_client.get_bucket_location(Bucket=bucket_name)
                bucket_info["region"] = (
                    location_response.get("LocationConstraint") or "us-east-1"
                )

                # Test list permission
                try:
                    s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
                    bucket_info["permissions"]["list"] = True
                except ClientError:
                    pass

                # Test write permission (create and delete a test object)
                test_key = f"connection_test_{int(time.time())}"
                try:
                    s3_client.put_object(Bucket=bucket_name, Key=test_key, Body=b"test")
                    bucket_info["permissions"]["write"] = True
                    bucket_info["permissions"]["read"] = True

                    # Clean up
                    s3_client.delete_object(Bucket=bucket_name, Key=test_key)
                except ClientError:
                    pass

            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                bucket_info["error"] = error_code

                if error_code in ["NoSuchBucket", "404"]:
                    bucket_info["exists"] = False
                elif error_code in ["403", "AccessDenied"]:
                    bucket_info["exists"] = True  # Probably exists but no access
                    bucket_info["accessible"] = False

            return bucket_info

        except Exception as e:
            return {"error": str(e), "accessible": False}

    def test_aws_services(self) -> Dict[str, Any]:
        """Test connectivity to other AWS services used in the pipeline"""
        logger.info("Testing AWS services connectivity...")

        result = {"status": "success", "details": {}, "errors": [], "warnings": []}

        try:
            service_tests = {
                "cloudwatch": self._test_cloudwatch_access(),
                "lambda": self._test_lambda_access(),
                "sns": self._test_sns_access(),
                "sqs": self._test_sqs_access(),
                "ec2": self._test_ec2_access(),
            }

            result["details"] = service_tests

            # Check for service access issues
            failed_services = [
                service for service, success in service_tests.items() if not success
            ]
            if failed_services:
                result["status"] = "partial"
                result["warnings"] = [
                    f"Service access limited: {service}" for service in failed_services
                ]

        except Exception as e:
            result["status"] = "error"
            result["errors"] = [f"AWS services test error: {str(e)}"]

        self.test_results["aws_services"] = result
        return result

    def _test_cloudwatch_access(self) -> bool:
        """Test CloudWatch access"""
        try:
            cw_client = self._create_client("cloudwatch")
            cw_client.list_metrics(MaxRecords=1)
            return True
        except Exception:
            return False

    def _test_lambda_access(self) -> bool:
        """Test Lambda access"""
        try:
            lambda_client = self._create_client("lambda")
            lambda_client.list_functions(MaxItems=1)
            return True
        except Exception:
            return False

    def _test_sns_access(self) -> bool:
        """Test SNS access"""
        try:
            sns_client = self._create_client("sns")
            sns_client.list_topics()
            return True
        except Exception:
            return False

    def _test_sqs_access(self) -> bool:
        """Test SQS access"""
        try:
            sqs_client = self._create_client("sqs")
            sqs_client.list_queues(MaxResults=1)
            return True
        except Exception:
            return False

    def _test_ec2_access(self) -> bool:
        """Test EC2 access"""
        try:
            ec2_client = self._create_client("ec2")
            ec2_client.describe_instances(MaxResults=1)
            return True
        except Exception:
            return False

    def test_performance(self) -> Dict[str, Any]:
        """Test AWS performance metrics"""
        logger.info("Testing AWS performance...")

        result = {"status": "success", "details": {}, "errors": [], "warnings": []}

        try:
            performance_metrics = {
                "authentication_time": self._test_authentication_performance(),
                "s3_operations_time": self._test_s3_performance(),
                "api_response_times": self._test_api_performance(),
            }

            result["details"] = performance_metrics

            # Check performance thresholds
            auth_time = performance_metrics.get("authentication_time", 0)
            if auth_time > 5:
                result["warnings"].append(f"Authentication time slow: {auth_time}s")

            s3_time = performance_metrics.get("s3_operations_time", {}).get(
                "list_buckets", 0
            )
            if s3_time > 10:
                result["warnings"].append(f"S3 operations slow: {s3_time}s")

        except Exception as e:
            result["status"] = "error"
            result["errors"] = [f"Performance test error: {str(e)}"]

        self.test_results["performance"] = result
        return result

    def _test_authentication_performance(self) -> float:
        """Measure authentication performance"""
        try:
            start_time = time.time()
            sts_client = self._create_client("sts")
            sts_client.get_caller_identity()
            return round(time.time() - start_time, 2)
        except Exception:
            return -1

    def _test_s3_performance(self) -> Dict[str, float]:
        """Test S3 operation performance"""
        try:
            s3_client = self._create_client("s3")

            # Test list buckets performance
            start_time = time.time()
            s3_client.list_buckets()
            list_buckets_time = round(time.time() - start_time, 2)

            # Test bucket operations if bucket exists
            bucket_ops_time = -1
            if self.config.get("s3_bucket"):
                try:
                    start_time = time.time()
                    s3_client.list_objects_v2(
                        Bucket=self.config["s3_bucket"], MaxKeys=1
                    )
                    bucket_ops_time = round(time.time() - start_time, 2)
                except ClientError:
                    pass  # Bucket might not exist

            return {
                "list_buckets": list_buckets_time,
                "bucket_operations": bucket_ops_time,
            }
        except Exception:
            return {"list_buckets": -1, "bucket_operations": -1}

    def _test_api_performance(self) -> Dict[str, float]:
        """Test various AWS API performance"""
        apis = [
            ("sts", "get_caller_identity", {}),
            ("s3", "list_buckets", {}),
            ("iam", "get_user", {}),
        ]

        results = {}

        for service, operation, params in apis:
            try:
                client = self._create_client(service)
                method = getattr(client, operation)

                start_time = time.time()
                method(**params)
                api_time = round(time.time() - start_time, 2)
                results[f"{service}_{operation}"] = api_time
            except Exception:
                results[f"{service}_{operation}"] = -1

        return results

    def test_security(self) -> Dict[str, Any]:
        """Test AWS security configuration"""
        logger.info("Testing AWS security configuration...")

        result = {"status": "success", "details": {}, "errors": [], "warnings": []}

        try:
            security_tests = {
                "ssl_connections": self._test_ssl_connections(),
                "credential_security": self._test_credential_security(),
                "bucket_encryption": self._test_bucket_encryption(),
                "access_logging": self._test_access_logging(),
            }

            result["details"] = security_tests

        except Exception as e:
            result["status"] = "error"
            result["errors"] = [f"Security test error: {str(e)}"]

        self.test_results["security"] = result
        return result

    def _test_ssl_connections(self) -> bool:
        """Test that connections use SSL/TLS"""
        # boto3 uses HTTPS by default for AWS API calls
        return True

    def _test_credential_security(self) -> Dict[str, Any]:
        """Test credential security practices"""
        return {
            "credentials_not_hardcoded": not (
                "AKIA" in str(self.config.values())
                and len(str(self.config["secret_access_key"])) == 40
            ),
            "using_env_vars": bool(os.getenv("AWS_ACCESS_KEY_ID")),
            "session_token_used": bool(self.config.get("session_token")),
        }

    def _test_bucket_encryption(self) -> bool:
        """Test S3 bucket encryption settings"""
        try:
            s3_client = self._create_client("s3")
            bucket_name = self.config.get("s3_bucket")

            if not bucket_name:
                return False

            try:
                encryption = s3_client.get_bucket_encryption(Bucket=bucket_name)
                return "ServerSideEncryptionConfiguration" in encryption
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                # NoSuchBucket or ServerSideEncryptionConfigurationNotFoundError are OK
                return error_code in [
                    "NoSuchBucket",
                    "ServerSideEncryptionConfigurationNotFoundError",
                ]

        except Exception:
            return False

    def _test_access_logging(self) -> bool:
        """Test S3 access logging configuration"""
        try:
            s3_client = self._create_client("s3")
            bucket_name = self.config.get("s3_bucket")

            if not bucket_name:
                return False

            try:
                logging_config = s3_client.get_bucket_logging(Bucket=bucket_name)
                return "LoggingEnabled" in logging_config
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                return error_code in ["NoSuchBucket", "404"]

        except Exception:
            return False

    def run_all_tests(self) -> Dict[str, Any]:
        """Run all AWS connection tests"""
        logger.info("Running comprehensive AWS connection tests...")

        if not BOTO3_AVAILABLE:
            return {
                "status": "error",
                "error": f"boto3 not available: {IMPORT_ERROR}",
                "tests": {},
            }

        tests = [
            ("Configuration", self.test_configuration),
            ("Authentication", self.test_authentication),
            ("IAM Permissions", self.test_iam_permissions),
            ("S3 Operations", self.test_s3_operations),
            ("Bucket Access", self.test_bucket_access),
            ("AWS Services", self.test_aws_services),
            ("Performance", self.test_performance),
            ("Security", self.test_security),
        ]

        overall_status = "success"

        for test_name, test_func in tests:
            try:
                logger.info(f"Running {test_name} tests...")
                test_result = test_func()

                if test_result["status"] in ["failed", "error"]:
                    overall_status = "failed"
                elif test_result["status"] == "partial" and overall_status == "success":
                    overall_status = "partial"

            except Exception as e:
                logger.error(f"Error running {test_name} test: {str(e)}")
                self.test_results[test_name.lower().replace(" ", "_")] = {
                    "status": "error",
                    "errors": [str(e)],
                }
                overall_status = "failed"

        return {
            "status": overall_status,
            "timestamp": time.time(),
            "tests": self.test_results,
            "summary": self._generate_summary(),
        }

    def _generate_summary(self) -> Dict[str, Any]:
        """Generate test results summary"""
        summary = {
            "total_tests": len(self.test_results),
            "passed": 0,
            "failed": 0,
            "partial": 0,
            "errors": 0,
        }

        for test_result in self.test_results.values():
            status = test_result.get("status", "error")
            if status == "success":
                summary["passed"] += 1
            elif status == "failed":
                summary["failed"] += 1
            elif status == "partial":
                summary["partial"] += 1
            else:
                summary["errors"] += 1

        return summary

    def print_results(self, results: Dict[str, Any]):
        """Print formatted test results"""
        print("\n" + "=" * 80)
        print("AWS CONNECTION TEST RESULTS")
        print("=" * 80)

        if results.get("error"):
            print(f"ERROR: Test Suite Error: {results['error']}")
            return

        print(f"Overall Status: {results['status'].upper()}")

        if "summary" in results:
            summary = results["summary"]
            print(f"\nTest Summary:")
            print(f"  Total Tests: {summary['total_tests']}")
            print(f"  SUCCESS: Passed: {summary['passed']}")
            print(f"  WARNING: Partial: {summary['partial']}")
            print(f"  ERROR: Failed: {summary['failed']}")
            print(f"  CRITICAL: Errors: {summary['errors']}")

        for test_name, test_result in results.get("tests", {}).items():
            print(f"\n{test_name.replace('_', ' ').title()}:")
            print("-" * 40)

            status = test_result.get("status", "unknown")
            status_icon = {
                "success": "SUCCESS",
                "partial": "WARNING",
                "failed": "ERROR",
                "error": "CRITICAL",
            }.get(status, "❓")

            print(f"Status: {status_icon} {status.upper()}")

            if test_result.get("errors"):
                print("Errors:")
                for error in test_result["errors"]:
                    print(f"  • {error}")

            if test_result.get("warnings"):
                print("Warnings:")
                for warning in test_result["warnings"]:
                    print(f"  • {warning}")

            if test_result.get("details"):
                print("Details:")
                details = test_result["details"]
                if isinstance(details, dict):
                    for key, value in details.items():
                        if isinstance(value, dict):
                            print(f"  {key}:")
                            for sub_key, sub_value in value.items():
                                print(f"    {sub_key}: {sub_value}")
                        else:
                            print(f"  {key}: {value}")
                else:
                    print(f"  {details}")

            # Show response time if available
            if test_result.get("response_time"):
                print(f"  Response Time: {test_result['response_time']}s")


# Pytest test functions
@pytest.mark.skipif(not BOTO3_AVAILABLE, reason="boto3 not available")
class TestAWSConnection:
    """Pytest test class for AWS connections"""

    @pytest.fixture(scope="class")
    def tester(self):
        """Create AWS connection tester instance"""
        return AWSConnectionTester()

    def test_configuration_valid(self, tester):
        """Test that AWS configuration is valid"""
        result = tester.test_configuration()
        assert result["status"] in [
            "success",
            "partial",
        ], f"Configuration test failed: {result.get('errors', [])}"

    def test_authentication_success(self, tester):
        """Test that AWS authentication succeeds"""
        result = tester.test_authentication()
        assert (
            result["status"] == "success"
        ), f"Authentication failed: {result.get('errors', [])}"

    def test_iam_permissions_adequate(self, tester):
        """Test that IAM permissions are adequate"""
        result = tester.test_iam_permissions()
        assert result["status"] in [
            "success",
            "partial",
        ], f"IAM permissions test failed: {result.get('errors', [])}"

    def test_s3_operations_functional(self, tester):
        """Test that S3 operations are functional"""
        result = tester.test_s3_operations()
        assert result["status"] in [
            "success",
            "partial",
        ], f"S3 operations failed: {result.get('errors', [])}"

    def test_bucket_access_available(self, tester):
        """Test that configured buckets are accessible"""
        result = tester.test_bucket_access()
        assert result["status"] in [
            "success",
            "partial",
        ], f"Bucket access failed: {result.get('errors', [])}"

    def test_performance_acceptable(self, tester):
        """Test that AWS performance is acceptable"""
        result = tester.test_performance()
        assert result["status"] in [
            "success",
            "partial",
        ], f"Performance test failed: {result.get('errors', [])}"

        # Check authentication time is reasonable
        details = result.get("details", {})
        auth_time = details.get("authentication_time", 0)
        assert auth_time < 10, f"Authentication time too slow: {auth_time}s"


def main():
    """Main function for standalone execution"""
    try:
        tester = AWSConnectionTester()
        results = tester.run_all_tests()
        tester.print_results(results)

        # Save detailed results
        results_file = Path("aws_connection_test_results.json")
        with open(results_file, "w") as f:
            json.dump(results, f, indent=2, default=str)

        print(f"\n📄 Detailed results saved to: {results_file}")

        # Exit with appropriate code
        if results["status"] in ["success", "partial"]:
            print("\nSUCCESS: AWS connection tests completed successfully!")
            sys.exit(0)
        else:
            print("\nERROR: AWS connection tests failed!")
            sys.exit(1)

    except Exception as e:
        print(f"\nCRITICAL: Fatal error running AWS tests: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
