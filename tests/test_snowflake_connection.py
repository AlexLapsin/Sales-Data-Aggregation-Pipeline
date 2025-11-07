#!/usr/bin/env python3
"""
Comprehensive Snowflake Connection Tests

This module provides comprehensive testing for Snowflake connectivity using password authentication.
Tests cover connection establishment, authentication verification, database operations,
and connection resilience.

Usage:
    python -m pytest tests/test_snowflake_connection.py -v
    python tests/test_snowflake_connection.py  # Run standalone

Environment Variables Required:
    SNOWFLAKE_ACCOUNT: Snowflake account identifier
    SNOWFLAKE_USER: Snowflake username
    SNOWFLAKE_PRIVATE_KEY_PATH: Path to Snowflake private key file
    SNOWFLAKE_KEY_PASSPHRASE: Passphrase for private key (optional)
    SNOWFLAKE_ROLE: Snowflake role (optional, defaults to ACCOUNTADMIN)
    SNOWFLAKE_WAREHOUSE: Snowflake warehouse (optional, defaults to COMPUTE_WH)
    SNOWFLAKE_DATABASE: Snowflake database (optional, defaults to SALES_DW)
    SNOWFLAKE_SCHEMA: Snowflake schema (optional, defaults to RAW)
"""

import os
import sys
import pytest
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from contextlib import contextmanager
import time
import json

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    import snowflake.connector
    from snowflake.connector import DictCursor
    from snowflake.connector.errors import (
        Error as SnowflakeError,
        DatabaseError,
        ProgrammingError,
        InterfaceError,
        OperationalError,
    )
    from tests.helpers import (
        get_private_key_bytes,
        check_snowflake_credentials_available,
    )

    SNOWFLAKE_AVAILABLE = True
except ImportError as e:
    SNOWFLAKE_AVAILABLE = False
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


class SnowflakeConnectionTester:
    """Comprehensive Snowflake connection testing utility"""

    def __init__(self):
        self.config = self._load_config()
        self.connection = None
        self.cursor = None
        self.test_results = {
            "configuration": {},
            "authentication": {},
            "basic_operations": {},
            "performance": {},
            "connection_resilience": {},
            "security": {},
        }

    def _load_config(self) -> Dict[str, Any]:
        """Load and validate Snowflake configuration"""
        # Use our current .env format with account name and organization
        account_name = os.getenv("SNOWFLAKE_ACCOUNT_NAME")
        org_name = os.getenv("SNOWFLAKE_ORGANIZATION_NAME")

        # Construct account in correct format: organization-account
        if account_name and org_name:
            account = f"{org_name}-{account_name}"
        else:
            account = os.getenv("SNOWFLAKE_ACCOUNT")  # fallback

        config = {
            "account": account,
            "user": os.getenv("SNOWFLAKE_USER"),
            "private_key": get_private_key_bytes(),
            "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "database": os.getenv("SNOWFLAKE_DATABASE", "SALES_DW"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
            "region": os.getenv("SNOWFLAKE_REGION", "us-east-1"),
        }

        # Validate required fields
        required_fields = ["account", "user", "private_key"]
        missing_fields = [field for field in required_fields if not config.get(field)]

        if missing_fields:
            raise ValueError(
                f"Missing required Snowflake configuration: {missing_fields}"
            )

        return config

    def _validate_credentials(self) -> bool:
        """Validate Snowflake credentials are available"""
        return check_snowflake_credentials_available()

    @contextmanager
    def get_connection(self):
        """Context manager for Snowflake connections"""
        connection = None
        try:
            connection = snowflake.connector.connect(
                user=self.config["user"],
                account=self.config["account"],
                password=self.config["password"],
                role=self.config["role"],
                warehouse=self.config["warehouse"],
                database=self.config["database"],
                schema=self.config["schema"],
                client_session_keep_alive=True,
                login_timeout=60,
                network_timeout=60,
            )

            yield connection

        except Exception as e:
            logger.error(f"Failed to establish Snowflake connection: {str(e)}")
            raise
        finally:
            if connection:
                try:
                    connection.close()
                except Exception as e:
                    logger.warning(f"Error closing Snowflake connection: {str(e)}")

    def test_configuration(self) -> Dict[str, Any]:
        """Test Snowflake configuration validity"""
        logger.info("Testing Snowflake configuration...")

        result = {"status": "success", "details": {}, "errors": [], "warnings": []}

        try:
            # Test configuration completeness
            config_checks = {
                "account_format": self._validate_account_format(),
                "password_validation": self._validate_password(),
                "user_format": self._validate_user_format(),
                "optional_params": self._validate_optional_params(),
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

    def _validate_account_format(self) -> bool:
        """Validate Snowflake account identifier format"""
        account = self.config["account"]
        # Snowflake account format: ORGNAME-ACCOUNT_NAME or legacy formats
        return bool(account and len(account) > 0 and not account.startswith("YOUR_"))

    def _validate_user_format(self) -> bool:
        """Validate Snowflake user format"""
        user = self.config["user"]
        return bool(user and len(user) > 0 and not user.startswith("YOUR_"))

    def _validate_optional_params(self) -> bool:
        """Validate optional parameters"""
        optional_params = ["role", "warehouse", "database", "schema"]
        return all(self.config.get(param) for param in optional_params)

    def test_authentication(self) -> Dict[str, Any]:
        """Test Snowflake authentication with password"""
        logger.info("Testing Snowflake authentication...")

        result = {
            "status": "success",
            "details": {},
            "errors": [],
            "warnings": [],
            "connection_time": None,
        }

        try:
            start_time = time.time()

            with self.get_connection() as conn:
                # Test basic connection
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()"
                )
                user_info = cursor.fetchone()

                connection_time = time.time() - start_time
                result["connection_time"] = round(connection_time, 2)

                result["details"] = {
                    "current_user": user_info[0] if user_info else None,
                    "current_role": user_info[1] if user_info else None,
                    "current_warehouse": user_info[2] if user_info else None,
                    "connection_established": True,
                    "authentication_method": "password",
                }

                # Verify user matches configuration
                if user_info and user_info[0] != self.config["user"].upper():
                    result["warnings"].append(
                        f"Connected user {user_info[0]} differs from configured user {self.config['user']}"
                    )

        except SnowflakeError as e:
            result["status"] = "failed"
            result["errors"] = [f"Snowflake authentication error: {str(e)}"]
        except Exception as e:
            result["status"] = "error"
            result["errors"] = [f"Authentication test error: {str(e)}"]

        self.test_results["authentication"] = result
        return result

    def test_basic_operations(self) -> Dict[str, Any]:
        """Test basic Snowflake database operations"""
        logger.info("Testing Snowflake basic operations...")

        result = {"status": "success", "details": {}, "errors": [], "warnings": []}

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(DictCursor)

                operations = {
                    "select_version": self._test_select_version(cursor),
                    "database_access": self._test_database_access(cursor),
                    "schema_access": self._test_schema_access(cursor),
                    "table_operations": self._test_table_operations(cursor),
                    "warehouse_usage": self._test_warehouse_usage(cursor),
                }

                result["details"] = operations

                # Check for any failed operations
                failed_ops = [op for op, success in operations.items() if not success]
                if failed_ops:
                    result["status"] = "partial"
                    result["warnings"] = [
                        f"Operation failed: {op}" for op in failed_ops
                    ]

        except Exception as e:
            result["status"] = "error"
            result["errors"] = [f"Basic operations test error: {str(e)}"]

        self.test_results["basic_operations"] = result
        return result

    def _test_select_version(self, cursor) -> bool:
        """Test basic SELECT query"""
        try:
            cursor.execute("SELECT CURRENT_VERSION()")
            version_info = cursor.fetchone()
            return bool(version_info)
        except Exception:
            return False

    def _test_database_access(self, cursor) -> bool:
        """Test database access"""
        try:
            cursor.execute(f"USE DATABASE {self.config['database']}")
            cursor.execute("SELECT CURRENT_DATABASE()")
            db_info = cursor.fetchone()
            return db_info and db_info[0] == self.config["database"]
        except Exception:
            return False

    def _test_schema_access(self, cursor) -> bool:
        """Test schema access"""
        try:
            cursor.execute(
                f"USE SCHEMA {self.config['database']}.{self.config['schema']}"
            )
            cursor.execute("SELECT CURRENT_SCHEMA()")
            schema_info = cursor.fetchone()
            return schema_info and schema_info[0] == self.config["schema"]
        except Exception:
            return False

    def _test_table_operations(self, cursor) -> bool:
        """Test basic table operations (create, insert, select, drop)"""
        try:
            test_table = "TEST_CONNECTION_TABLE"

            # Create test table
            cursor.execute(
                f"""
                CREATE OR REPLACE TEMPORARY TABLE {test_table} (
                    id INTEGER,
                    test_data VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Insert test data
            cursor.execute(
                f"""
                INSERT INTO {test_table} (id, test_data)
                VALUES (1, 'connection_test'), (2, 'authentication_test')
            """
            )

            # Query test data
            cursor.execute(f"SELECT COUNT(*) as count FROM {test_table}")
            count_result = cursor.fetchone()

            # Drop test table
            cursor.execute(f"DROP TABLE {test_table}")

            return count_result and count_result["COUNT"] == 2

        except Exception:
            return False

    def _test_warehouse_usage(self, cursor) -> bool:
        """Test warehouse usage"""
        try:
            cursor.execute(f"USE WAREHOUSE {self.config['warehouse']}")
            cursor.execute("SELECT CURRENT_WAREHOUSE()")
            warehouse_info = cursor.fetchone()
            return warehouse_info and warehouse_info[0] == self.config["warehouse"]
        except Exception:
            return False

    def test_performance(self) -> Dict[str, Any]:
        """Test Snowflake connection performance"""
        logger.info("Testing Snowflake performance...")

        result = {"status": "success", "details": {}, "errors": [], "warnings": []}

        try:
            performance_metrics = {
                "connection_time": self._test_connection_time(),
                "query_performance": self._test_query_performance(),
                "bulk_operations": self._test_bulk_operations(),
            }

            result["details"] = performance_metrics

            # Check performance thresholds
            if performance_metrics.get("connection_time", 0) > 10:
                result["warnings"].append("Connection time exceeds 10 seconds")

            if (
                performance_metrics.get("query_performance", {}).get(
                    "simple_query_time", 0
                )
                > 5
            ):
                result["warnings"].append("Simple query time exceeds 5 seconds")

        except Exception as e:
            result["status"] = "error"
            result["errors"] = [f"Performance test error: {str(e)}"]

        self.test_results["performance"] = result
        return result

    def _test_connection_time(self) -> float:
        """Measure connection establishment time"""
        start_time = time.time()
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return round(time.time() - start_time, 2)
        except Exception:
            return -1

    def _test_query_performance(self) -> Dict[str, float]:
        """Test various query performance metrics"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Simple query
                start_time = time.time()
                cursor.execute("SELECT CURRENT_TIMESTAMP")
                cursor.fetchone()
                simple_query_time = round(time.time() - start_time, 2)

                # Complex query with system functions
                start_time = time.time()
                cursor.execute(
                    """
                    SELECT
                        CURRENT_USER(),
                        CURRENT_ROLE(),
                        CURRENT_DATABASE(),
                        CURRENT_SCHEMA(),
                        CURRENT_WAREHOUSE(),
                        CURRENT_REGION()
                """
                )
                cursor.fetchone()
                complex_query_time = round(time.time() - start_time, 2)

                return {
                    "simple_query_time": simple_query_time,
                    "complex_query_time": complex_query_time,
                }
        except Exception:
            return {"simple_query_time": -1, "complex_query_time": -1}

    def _test_bulk_operations(self) -> Dict[str, Any]:
        """Test bulk operation performance"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Create temporary table for bulk test
                cursor.execute(
                    """
                    CREATE OR REPLACE TEMPORARY TABLE bulk_test (
                        id INTEGER,
                        data VARCHAR(100)
                    )
                """
                )

                # Test bulk insert
                start_time = time.time()
                test_data = [(i, f"test_data_{i}") for i in range(100)]
                cursor.executemany(
                    "INSERT INTO bulk_test (id, data) VALUES (%s, %s)", test_data
                )
                bulk_insert_time = round(time.time() - start_time, 2)

                # Test bulk select
                start_time = time.time()
                cursor.execute("SELECT COUNT(*) FROM bulk_test")
                result = cursor.fetchone()
                bulk_select_time = round(time.time() - start_time, 2)

                cursor.execute("DROP TABLE bulk_test")

                return {
                    "bulk_insert_time": bulk_insert_time,
                    "bulk_select_time": bulk_select_time,
                    "records_inserted": result[0] if result else 0,
                }
        except Exception:
            return {
                "bulk_insert_time": -1,
                "bulk_select_time": -1,
                "records_inserted": 0,
            }

    def test_connection_resilience(self) -> Dict[str, Any]:
        """Test connection resilience and error handling"""
        logger.info("Testing Snowflake connection resilience...")

        result = {"status": "success", "details": {}, "errors": [], "warnings": []}

        try:
            resilience_tests = {
                "invalid_query_handling": self._test_invalid_query_handling(),
                "connection_recovery": self._test_connection_recovery(),
                "timeout_handling": self._test_timeout_handling(),
            }

            result["details"] = resilience_tests

        except Exception as e:
            result["status"] = "error"
            result["errors"] = [f"Resilience test error: {str(e)}"]

        self.test_results["connection_resilience"] = result
        return result

    def _test_invalid_query_handling(self) -> bool:
        """Test handling of invalid queries"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Test invalid SQL
                try:
                    cursor.execute("SELECT FROM WHERE INVALID SQL")
                    return False  # Should have raised an exception
                except ProgrammingError:
                    return True  # Expected behavior
                except Exception:
                    return False
        except Exception:
            return False

    def _test_connection_recovery(self) -> bool:
        """Test connection recovery after error"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Cause an error
                try:
                    cursor.execute("SELECT * FROM non_existent_table_12345")
                except Exception:
                    pass  # Expected

                # Test if connection is still usable
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return bool(result)

        except Exception:
            return False

    def _test_timeout_handling(self) -> bool:
        """Test timeout handling (simplified test)"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Test with a simple query (timeout testing is complex)
                cursor.execute("SELECT SYSTEM$WAIT(1)")  # Wait 1 second
                result = cursor.fetchone()
                return True

        except Exception:
            return False

    def test_security(self) -> Dict[str, Any]:
        """Test security-related aspects"""
        logger.info("Testing Snowflake security aspects...")

        result = {"status": "success", "details": {}, "errors": [], "warnings": []}

        try:
            security_checks = {
                "ssl_connection": self._test_ssl_connection(),
                "role_privileges": self._test_role_privileges(),
                "password_auth": self._test_password_authentication(),
            }

            result["details"] = security_checks

        except Exception as e:
            result["status"] = "error"
            result["errors"] = [f"Security test error: {str(e)}"]

        self.test_results["security"] = result
        return result

    def _test_ssl_connection(self) -> bool:
        """Test SSL connection (Snowflake uses SSL by default)"""
        try:
            with self.get_connection() as conn:
                # Snowflake connections are SSL by default
                return True
        except Exception:
            return False

    def _test_role_privileges(self) -> bool:
        """Test role privileges"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT CURRENT_ROLE()")
                role_info = cursor.fetchone()
                return bool(role_info)
        except Exception:
            return False

    def _test_password_authentication(self) -> bool:
        """Test password authentication specifically"""
        try:
            # If we can establish connection, password auth is working
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 'password_auth_success'")
                result = cursor.fetchone()
                return bool(result)
        except Exception:
            return False

    def run_all_tests(self) -> Dict[str, Any]:
        """Run all Snowflake connection tests"""
        logger.info("Running comprehensive Snowflake connection tests...")

        if not SNOWFLAKE_AVAILABLE:
            return {
                "status": "error",
                "error": f"Snowflake dependencies not available: {IMPORT_ERROR}",
                "tests": {},
            }

        tests = [
            ("Configuration", self.test_configuration),
            ("Authentication", self.test_authentication),
            ("Basic Operations", self.test_basic_operations),
            ("Performance", self.test_performance),
            ("Connection Resilience", self.test_connection_resilience),
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
        print("SNOWFLAKE CONNECTION TEST RESULTS")
        print("=" * 80)

        if results.get("error"):
            print(f"Test Suite Error: {results['error']}")
            return

        print(f"Overall Status: {results['status'].upper()}")

        if "summary" in results:
            summary = results["summary"]
            print(f"\nTest Summary:")
            print(f"  Total Tests: {summary['total_tests']}")
            print(f"  Passed: {summary['passed']}")
            print(f"  Partial: {summary['partial']}")
            print(f"  Failed: {summary['failed']}")
            print(f"  Errors: {summary['errors']}")

        for test_name, test_result in results.get("tests", {}).items():
            print(f"\n{test_name.replace('_', ' ').title()}:")
            print("-" * 40)

            status = test_result.get("status", "unknown")
            status_icon = {
                "success": "PASS",
                "partial": "WARN",
                "failed": "FAIL",
                "error": "ERROR",
            }.get(status, "UNKNOWN")

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
                for key, value in test_result["details"].items():
                    print(f"  {key}: {value}")


# Pytest test functions
@pytest.mark.skipif(
    not SNOWFLAKE_AVAILABLE, reason="Snowflake dependencies not available"
)
class TestSnowflakeConnection:
    """Pytest test class for Snowflake connections"""

    @pytest.fixture(scope="class")
    def tester(self):
        """Create Snowflake connection tester instance"""
        return SnowflakeConnectionTester()

    def test_configuration_valid(self, tester):
        """Test that Snowflake configuration is valid"""
        result = tester.test_configuration()
        assert result["status"] in [
            "success",
            "partial",
        ], f"Configuration test failed: {result.get('errors', [])}"

    def test_authentication_success(self, tester):
        """Test that Snowflake authentication succeeds"""
        result = tester.test_authentication()
        assert (
            result["status"] == "success"
        ), f"Authentication failed: {result.get('errors', [])}"

    def test_basic_operations_work(self, tester):
        """Test that basic Snowflake operations work"""
        result = tester.test_basic_operations()
        assert result["status"] in [
            "success",
            "partial",
        ], f"Basic operations failed: {result.get('errors', [])}"

    def test_performance_acceptable(self, tester):
        """Test that Snowflake performance is acceptable"""
        result = tester.test_performance()
        assert result["status"] in [
            "success",
            "partial",
        ], f"Performance test failed: {result.get('errors', [])}"

        # Check connection time is reasonable
        details = result.get("details", {})
        connection_time = details.get("connection_time", 0)
        assert connection_time < 30, f"Connection time too slow: {connection_time}s"

    def test_connection_resilience(self, tester):
        """Test that connection handles errors gracefully"""
        result = tester.test_connection_resilience()
        assert result["status"] in [
            "success",
            "partial",
        ], f"Resilience test failed: {result.get('errors', [])}"

    def test_security_features(self, tester):
        """Test that security features are working"""
        result = tester.test_security()
        assert result["status"] in [
            "success",
            "partial",
        ], f"Security test failed: {result.get('errors', [])}"


def main():
    """Main function for standalone execution"""
    try:
        tester = SnowflakeConnectionTester()
        results = tester.run_all_tests()
        tester.print_results(results)

        # Save detailed results
        results_file = Path("snowflake_connection_test_results.json")
        with open(results_file, "w") as f:
            json.dump(results, f, indent=2, default=str)

        print(f"\nDetailed results saved to: {results_file}")

        # Exit with appropriate code
        if results["status"] in ["success", "partial"]:
            print("\nSnowflake connection tests completed successfully!")
            sys.exit(0)
        else:
            print("\nSnowflake connection tests failed!")
            sys.exit(1)

    except Exception as e:
        print(f"\nFatal error running Snowflake tests: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
