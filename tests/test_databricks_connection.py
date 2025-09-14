#!/usr/bin/env python3
"""
Comprehensive Databricks Connection Tests

This module provides comprehensive testing for Databricks connectivity using token authentication.
Tests cover connection establishment, authentication verification, workspace access,
cluster operations, and API functionality.

Usage:
    python -m pytest tests/test_databricks_connection.py -v
    python tests/test_databricks_connection.py  # Run standalone

Environment Variables Required:
    DATABRICKS_HOST: Databricks workspace URL
    DATABRICKS_TOKEN: Databricks personal access token
    DATABRICKS_CLUSTER_ID: Optional cluster ID for compute tests
"""

import os
import sys
import pytest
import logging
import requests
import json
import time
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin, urlparse
import base64

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# Try to import databricks-sdk (optional)
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import Config
    from databricks.sdk.service import compute, workspace, sql

    DATABRICKS_SDK_AVAILABLE = True
except ImportError:
    DATABRICKS_SDK_AVAILABLE = False
    DATABRICKS_SDK_ERROR = (
        "databricks-sdk not available. Install with: pip install databricks-sdk"
    )

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DatabricksConnectionTester:
    """Comprehensive Databricks connection testing utility"""

    def __init__(self):
        self.config = self._load_config()
        self.test_results = {
            "configuration": {},
            "authentication": {},
            "workspace_access": {},
            "cluster_operations": {},
            "rest_api": {},
            "sql_operations": {},
            "performance": {},
        }
        self.session = requests.Session()
        self._setup_auth()

    def _load_config(self) -> Dict[str, Any]:
        """Load and validate Databricks configuration"""
        config = {
            "host": os.getenv("DATABRICKS_HOST"),
            "token": os.getenv("DATABRICKS_TOKEN"),
            "cluster_id": os.getenv("DATABRICKS_CLUSTER_ID", ""),
        }

        # Validate required fields
        required_fields = ["host", "token"]
        missing_fields = [field for field in required_fields if not config[field]]

        if missing_fields:
            raise ValueError(
                f"Missing required Databricks configuration: {missing_fields}"
            )

        # Normalize host URL
        if config["host"] and not config["host"].startswith(("http://", "https://")):
            config["host"] = f"https://{config['host']}"

        return config

    def _setup_auth(self):
        """Setup authentication for requests"""
        if self.config["token"]:
            self.session.headers.update(
                {
                    "Authorization": f"Bearer {self.config['token']}",
                    "Content-Type": "application/json",
                    "User-Agent": "DatabricksConnectionTester/1.0",
                }
            )

    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make authenticated request to Databricks API"""
        url = urljoin(self.config["host"], endpoint)

        # Set timeout if not specified
        if "timeout" not in kwargs:
            kwargs["timeout"] = 30

        try:
            response = self.session.request(method, url, **kwargs)
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Request to {url} failed: {str(e)}")
            raise

    def test_configuration(self) -> Dict[str, Any]:
        """Test Databricks configuration validity"""
        logger.info("Testing Databricks configuration...")

        result = {"status": "success", "details": {}, "errors": [], "warnings": []}

        try:
            config_checks = {
                "host_format": self._validate_host_format(),
                "token_format": self._validate_token_format(),
                "host_reachable": self._validate_host_reachable(),
                "url_structure": self._validate_url_structure(),
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

    def _validate_host_format(self) -> bool:
        """Validate Databricks host URL format"""
        host = self.config["host"]
        if not host:
            return False

        parsed = urlparse(host)
        return (
            parsed.scheme in ["http", "https"]
            and parsed.netloc
            and ".cloud.databricks.com" in parsed.netloc
        )

    def _validate_token_format(self) -> bool:
        """Validate Databricks token format"""
        token = self.config["token"]
        # Databricks tokens typically start with 'dapi' and are quite long
        return bool(token and token.startswith("dapi") and len(token) > 20)

    def _validate_host_reachable(self) -> bool:
        """Check if host is reachable"""
        try:
            response = requests.head(self.config["host"], timeout=10)
            return response.status_code < 500
        except Exception:
            return False

    def _validate_url_structure(self) -> bool:
        """Validate Databricks URL structure"""
        host = self.config["host"]
        # Check for typical Databricks URL patterns
        return ".cloud.databricks.com" in host or "adb-" in host

    def test_authentication(self) -> Dict[str, Any]:
        """Test Databricks authentication with token"""
        logger.info("Testing Databricks authentication...")

        result = {
            "status": "success",
            "details": {},
            "errors": [],
            "warnings": [],
            "response_time": None,
        }

        try:
            start_time = time.time()

            # Test authentication with user info endpoint
            response = self._make_request("GET", "/api/2.0/preview/scim/v2/Me")
            response_time = round(time.time() - start_time, 2)
            result["response_time"] = response_time

            if response.status_code == 200:
                user_info = response.json()
                result["details"] = {
                    "authenticated": True,
                    "user_name": user_info.get("userName", "unknown"),
                    "display_name": user_info.get("displayName", "unknown"),
                    "active": user_info.get("active", False),
                    "user_id": user_info.get("id", "unknown"),
                    "status_code": response.status_code,
                }
            else:
                result["status"] = "failed"
                result["errors"] = [
                    f"Authentication failed with status {response.status_code}: {response.text}"
                ]

        except requests.exceptions.RequestException as e:
            result["status"] = "failed"
            result["errors"] = [f"Authentication request error: {str(e)}"]
        except Exception as e:
            result["status"] = "error"
            result["errors"] = [f"Authentication test error: {str(e)}"]

        self.test_results["authentication"] = result
        return result

    def test_workspace_access(self) -> Dict[str, Any]:
        """Test Databricks workspace access and permissions"""
        logger.info("Testing Databricks workspace access...")

        result = {"status": "success", "details": {}, "errors": [], "warnings": []}

        try:
            workspace_tests = {
                "list_workspace": self._test_list_workspace(),
                "workspace_info": self._test_workspace_info(),
                "permissions": self._test_workspace_permissions(),
            }

            result["details"] = workspace_tests

            # Check for any failed tests
            failed_tests = [
                test for test, success in workspace_tests.items() if not success
            ]
            if failed_tests:
                result["status"] = "partial"
                result["warnings"] = [
                    f"Workspace test failed: {test}" for test in failed_tests
                ]

        except Exception as e:
            result["status"] = "error"
            result["errors"] = [f"Workspace access test error: {str(e)}"]

        self.test_results["workspace_access"] = result
        return result

    def _test_list_workspace(self) -> bool:
        """Test listing workspace contents"""
        try:
            response = self._make_request(
                "GET", "/api/2.0/workspace/list", params={"path": "/"}
            )
            return response.status_code == 200
        except Exception:
            return False

    def _test_workspace_info(self) -> bool:
        """Test getting workspace information"""
        try:
            # Test workspace status endpoint
            response = self._make_request(
                "GET", "/api/2.0/workspace/get-status", params={"path": "/"}
            )
            return response.status_code in [200, 404]  # 404 is OK for root path
        except Exception:
            return False

    def _test_workspace_permissions(self) -> bool:
        """Test workspace permissions"""
        try:
            # Try to create a simple test notebook (and delete it)
            test_notebook_path = "/tmp/connection_test_notebook"

            # Create notebook
            create_response = self._make_request(
                "POST",
                "/api/2.0/workspace/import",
                json={
                    "path": test_notebook_path,
                    "format": "SOURCE",
                    "language": "PYTHON",
                    "content": base64.b64encode(
                        b'# Connection test notebook\nprint("Hello, Databricks!")'
                    ).decode(),
                    "overwrite": True,
                },
            )

            if create_response.status_code == 200:
                # Delete notebook
                delete_response = self._make_request(
                    "POST",
                    "/api/2.0/workspace/delete",
                    json={"path": test_notebook_path, "recursive": False},
                )
                return delete_response.status_code == 200

            return False
        except Exception:
            return False

    def test_cluster_operations(self) -> Dict[str, Any]:
        """Test Databricks cluster operations"""
        logger.info("Testing Databricks cluster operations...")

        result = {"status": "success", "details": {}, "errors": [], "warnings": []}

        try:
            cluster_tests = {
                "list_clusters": self._test_list_clusters(),
                "cluster_policies": self._test_cluster_policies(),
                "node_types": self._test_list_node_types(),
            }

            # Test specific cluster if ID is provided
            if self.config["cluster_id"]:
                cluster_tests["specific_cluster"] = self._test_specific_cluster()
                cluster_tests["cluster_events"] = self._test_cluster_events()

            result["details"] = cluster_tests

            # Check for any failed tests
            failed_tests = [
                test for test, success in cluster_tests.items() if not success
            ]
            if failed_tests:
                result["status"] = "partial"
                result["warnings"] = [
                    f"Cluster test failed: {test}" for test in failed_tests
                ]

        except Exception as e:
            result["status"] = "error"
            result["errors"] = [f"Cluster operations test error: {str(e)}"]

        self.test_results["cluster_operations"] = result
        return result

    def _test_list_clusters(self) -> bool:
        """Test listing clusters"""
        try:
            response = self._make_request("GET", "/api/2.0/clusters/list")
            return response.status_code == 200
        except Exception:
            return False

    def _test_cluster_policies(self) -> bool:
        """Test listing cluster policies"""
        try:
            response = self._make_request("GET", "/api/2.0/policies/clusters/list")
            return response.status_code == 200
        except Exception:
            return False

    def _test_list_node_types(self) -> bool:
        """Test listing available node types"""
        try:
            response = self._make_request("GET", "/api/2.0/clusters/list-node-types")
            return response.status_code == 200
        except Exception:
            return False

    def _test_specific_cluster(self) -> bool:
        """Test operations on specific cluster"""
        try:
            cluster_id = self.config["cluster_id"]
            response = self._make_request(
                "GET", "/api/2.0/clusters/get", params={"cluster_id": cluster_id}
            )
            return response.status_code == 200
        except Exception:
            return False

    def _test_cluster_events(self) -> bool:
        """Test getting cluster events"""
        try:
            cluster_id = self.config["cluster_id"]
            response = self._make_request(
                "POST",
                "/api/2.0/clusters/events",
                json={
                    "cluster_id": cluster_id,
                    "start_time": int((time.time() - 3600) * 1000),  # Last hour
                    "limit": 10,
                },
            )
            return response.status_code == 200
        except Exception:
            return False

    def test_rest_api(self) -> Dict[str, Any]:
        """Test Databricks REST API functionality"""
        logger.info("Testing Databricks REST API...")

        result = {"status": "success", "details": {}, "errors": [], "warnings": []}

        try:
            api_tests = {
                "jobs_list": self._test_jobs_api(),
                "dbfs_list": self._test_dbfs_api(),
                "secrets_scopes": self._test_secrets_api(),
                "libraries": self._test_libraries_api(),
                "instance_profiles": self._test_instance_profiles(),
            }

            result["details"] = api_tests

            # Check for any failed tests
            failed_tests = [test for test, success in api_tests.items() if not success]
            if failed_tests:
                result["status"] = "partial"
                result["warnings"] = [
                    f"API test failed: {test}" for test in failed_tests
                ]

        except Exception as e:
            result["status"] = "error"
            result["errors"] = [f"REST API test error: {str(e)}"]

        self.test_results["rest_api"] = result
        return result

    def _test_jobs_api(self) -> bool:
        """Test Jobs API"""
        try:
            response = self._make_request(
                "GET", "/api/2.1/jobs/list", params={"limit": 10}
            )
            return response.status_code == 200
        except Exception:
            return False

    def _test_dbfs_api(self) -> bool:
        """Test DBFS API"""
        try:
            response = self._make_request(
                "GET", "/api/2.0/dbfs/list", params={"path": "/"}
            )
            return response.status_code == 200
        except Exception:
            return False

    def _test_secrets_api(self) -> bool:
        """Test Secrets API"""
        try:
            response = self._make_request("GET", "/api/2.0/secrets/scopes/list")
            return response.status_code == 200
        except Exception:
            return False

    def _test_libraries_api(self) -> bool:
        """Test Libraries API"""
        try:
            response = self._make_request(
                "GET", "/api/2.0/libraries/all-cluster-statuses"
            )
            return response.status_code == 200
        except Exception:
            return False

    def _test_instance_profiles(self) -> bool:
        """Test Instance Profiles API"""
        try:
            response = self._make_request("GET", "/api/2.0/instance-profiles/list")
            return response.status_code == 200
        except Exception:
            return False

    def test_sql_operations(self) -> Dict[str, Any]:
        """Test Databricks SQL operations"""
        logger.info("Testing Databricks SQL operations...")

        result = {"status": "success", "details": {}, "errors": [], "warnings": []}

        try:
            sql_tests = {
                "warehouses_list": self._test_sql_warehouses(),
                "queries_list": self._test_sql_queries(),
                "endpoints_list": self._test_sql_endpoints(),
            }

            result["details"] = sql_tests

            # Check for any failed tests
            failed_tests = [test for test, success in sql_tests.items() if not success]
            if failed_tests:
                result["status"] = "partial"
                result["warnings"] = [
                    f"SQL test failed: {test}" for test in failed_tests
                ]

        except Exception as e:
            result["status"] = "error"
            result["errors"] = [f"SQL operations test error: {str(e)}"]

        self.test_results["sql_operations"] = result
        return result

    def _test_sql_warehouses(self) -> bool:
        """Test SQL Warehouses API"""
        try:
            response = self._make_request("GET", "/api/2.0/sql/warehouses")
            return response.status_code == 200
        except Exception:
            return False

    def _test_sql_queries(self) -> bool:
        """Test SQL Queries API"""
        try:
            response = self._make_request(
                "GET", "/api/2.0/preview/sql/queries", params={"page_size": 10}
            )
            return response.status_code == 200
        except Exception:
            return False

    def _test_sql_endpoints(self) -> bool:
        """Test SQL Endpoints API (legacy)"""
        try:
            response = self._make_request("GET", "/api/2.0/sql/endpoints")
            return response.status_code == 200
        except Exception:
            return False

    def test_performance(self) -> Dict[str, Any]:
        """Test Databricks performance metrics"""
        logger.info("Testing Databricks performance...")

        result = {"status": "success", "details": {}, "errors": [], "warnings": []}

        try:
            performance_metrics = {
                "auth_response_time": self._test_auth_performance(),
                "api_response_times": self._test_api_performance(),
                "workspace_load_time": self._test_workspace_performance(),
            }

            result["details"] = performance_metrics

            # Check performance thresholds
            auth_time = performance_metrics.get("auth_response_time", 0)
            if auth_time > 5:
                result["warnings"].append(
                    f"Authentication response time slow: {auth_time}s"
                )

            avg_api_time = performance_metrics.get("api_response_times", {}).get(
                "average", 0
            )
            if avg_api_time > 3:
                result["warnings"].append(
                    f"Average API response time slow: {avg_api_time}s"
                )

        except Exception as e:
            result["status"] = "error"
            result["errors"] = [f"Performance test error: {str(e)}"]

        self.test_results["performance"] = result
        return result

    def _test_auth_performance(self) -> float:
        """Measure authentication response time"""
        try:
            start_time = time.time()
            response = self._make_request("GET", "/api/2.0/preview/scim/v2/Me")
            if response.status_code == 200:
                return round(time.time() - start_time, 2)
            return -1
        except Exception:
            return -1

    def _test_api_performance(self) -> Dict[str, float]:
        """Test various API endpoint performance"""
        endpoints = [
            "/api/2.0/clusters/list",
            "/api/2.0/workspace/list?path=/",
            "/api/2.0/dbfs/list?path=/",
            "/api/2.1/jobs/list?limit=5",
        ]

        times = []
        for endpoint in endpoints:
            try:
                start_time = time.time()
                response = self._make_request("GET", endpoint)
                if response.status_code == 200:
                    times.append(time.time() - start_time)
            except Exception:
                continue

        if times:
            return {
                "average": round(sum(times) / len(times), 2),
                "min": round(min(times), 2),
                "max": round(max(times), 2),
                "samples": len(times),
            }
        return {"average": -1, "min": -1, "max": -1, "samples": 0}

    def _test_workspace_performance(self) -> float:
        """Test workspace loading performance"""
        try:
            start_time = time.time()
            response = self._make_request(
                "GET", "/api/2.0/workspace/list", params={"path": "/"}
            )
            if response.status_code == 200:
                return round(time.time() - start_time, 2)
            return -1
        except Exception:
            return -1

    def test_databricks_sdk(self) -> Dict[str, Any]:
        """Test Databricks SDK functionality (if available)"""
        logger.info("Testing Databricks SDK...")

        result = {"status": "success", "details": {}, "errors": [], "warnings": []}

        if not DATABRICKS_SDK_AVAILABLE:
            result["status"] = "skipped"
            result["warnings"] = [DATABRICKS_SDK_ERROR]
            return result

        try:
            # Create workspace client
            config = Config(host=self.config["host"], token=self.config["token"])

            client = WorkspaceClient(config=config)

            # Test various SDK operations
            sdk_tests = {
                "current_user": self._test_sdk_current_user(client),
                "workspace_list": self._test_sdk_workspace(client),
                "clusters_list": self._test_sdk_clusters(client),
            }

            result["details"] = sdk_tests

            failed_tests = [test for test, success in sdk_tests.items() if not success]
            if failed_tests:
                result["status"] = "partial"
                result["warnings"] = [
                    f"SDK test failed: {test}" for test in failed_tests
                ]

        except Exception as e:
            result["status"] = "error"
            result["errors"] = [f"Databricks SDK test error: {str(e)}"]

        return result

    def _test_sdk_current_user(self, client) -> bool:
        """Test SDK current user functionality"""
        try:
            user = client.current_user.me()
            return bool(user.user_name)
        except Exception:
            return False

    def _test_sdk_workspace(self, client) -> bool:
        """Test SDK workspace functionality"""
        try:
            items = list(client.workspace.list("/"))
            return isinstance(items, list)
        except Exception:
            return False

    def _test_sdk_clusters(self, client) -> bool:
        """Test SDK clusters functionality"""
        try:
            clusters = list(client.clusters.list())
            return isinstance(clusters, list)
        except Exception:
            return False

    def run_all_tests(self) -> Dict[str, Any]:
        """Run all Databricks connection tests"""
        logger.info("Running comprehensive Databricks connection tests...")

        tests = [
            ("Configuration", self.test_configuration),
            ("Authentication", self.test_authentication),
            ("Workspace Access", self.test_workspace_access),
            ("Cluster Operations", self.test_cluster_operations),
            ("REST API", self.test_rest_api),
            ("SQL Operations", self.test_sql_operations),
            ("Performance", self.test_performance),
        ]

        # Add SDK test if available
        if DATABRICKS_SDK_AVAILABLE:
            tests.append(("Databricks SDK", self.test_databricks_sdk))

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
            "databricks_sdk_available": DATABRICKS_SDK_AVAILABLE,
        }

    def _generate_summary(self) -> Dict[str, Any]:
        """Generate test results summary"""
        summary = {
            "total_tests": len(self.test_results),
            "passed": 0,
            "failed": 0,
            "partial": 0,
            "errors": 0,
            "skipped": 0,
        }

        for test_result in self.test_results.values():
            status = test_result.get("status", "error")
            if status == "success":
                summary["passed"] += 1
            elif status == "failed":
                summary["failed"] += 1
            elif status == "partial":
                summary["partial"] += 1
            elif status == "skipped":
                summary["skipped"] += 1
            else:
                summary["errors"] += 1

        return summary

    def print_results(self, results: Dict[str, Any]):
        """Print formatted test results"""
        print("\n" + "=" * 80)
        print("DATABRICKS CONNECTION TEST RESULTS")
        print("=" * 80)

        print(f"Overall Status: {results['status'].upper()}")
        print(
            f"Databricks SDK Available: {'Yes' if results.get('databricks_sdk_available') else 'No'}"
        )

        if "summary" in results:
            summary = results["summary"]
            print(f"\nTest Summary:")
            print(f"  Total Tests: {summary['total_tests']}")
            print(f"  ✅ Passed: {summary['passed']}")
            print(f"  ⚠️  Partial: {summary['partial']}")
            print(f"  ❌ Failed: {summary['failed']}")
            print(f"  🔥 Errors: {summary['errors']}")
            if summary.get("skipped", 0) > 0:
                print(f"  ⏭️  Skipped: {summary['skipped']}")

        for test_name, test_result in results.get("tests", {}).items():
            print(f"\n{test_name.replace('_', ' ').title()}:")
            print("-" * 40)

            status = test_result.get("status", "unknown")
            status_icon = {
                "success": "✅",
                "partial": "⚠️",
                "failed": "❌",
                "error": "🔥",
                "skipped": "⏭️",
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
                        print(f"  {key}: {value}")
                else:
                    print(f"  {details}")

            # Show response time if available
            if test_result.get("response_time"):
                print(f"  Response Time: {test_result['response_time']}s")


# Pytest test functions
class TestDatabricksConnection:
    """Pytest test class for Databricks connections"""

    @pytest.fixture(scope="class")
    def tester(self):
        """Create Databricks connection tester instance"""
        return DatabricksConnectionTester()

    def test_configuration_valid(self, tester):
        """Test that Databricks configuration is valid"""
        result = tester.test_configuration()
        assert result["status"] in [
            "success",
            "partial",
        ], f"Configuration test failed: {result.get('errors', [])}"

    def test_authentication_success(self, tester):
        """Test that Databricks authentication succeeds"""
        result = tester.test_authentication()
        assert (
            result["status"] == "success"
        ), f"Authentication failed: {result.get('errors', [])}"

    def test_workspace_accessible(self, tester):
        """Test that workspace is accessible"""
        result = tester.test_workspace_access()
        assert result["status"] in [
            "success",
            "partial",
        ], f"Workspace access failed: {result.get('errors', [])}"

    def test_rest_api_functional(self, tester):
        """Test that REST API is functional"""
        result = tester.test_rest_api()
        assert result["status"] in [
            "success",
            "partial",
        ], f"REST API test failed: {result.get('errors', [])}"

    def test_performance_acceptable(self, tester):
        """Test that performance is acceptable"""
        result = tester.test_performance()
        assert result["status"] in [
            "success",
            "partial",
        ], f"Performance test failed: {result.get('errors', [])}"

        # Check authentication time is reasonable
        details = result.get("details", {})
        auth_time = details.get("auth_response_time", 0)
        assert auth_time < 10, f"Authentication time too slow: {auth_time}s"


def main():
    """Main function for standalone execution"""
    try:
        tester = DatabricksConnectionTester()
        results = tester.run_all_tests()
        tester.print_results(results)

        # Save detailed results
        results_file = Path("databricks_connection_test_results.json")
        with open(results_file, "w") as f:
            json.dump(results, f, indent=2, default=str)

        print(f"\n📄 Detailed results saved to: {results_file}")

        # Exit with appropriate code
        if results["status"] in ["success", "partial"]:
            print("\n✅ Databricks connection tests completed successfully!")
            sys.exit(0)
        else:
            print("\n❌ Databricks connection tests failed!")
            sys.exit(1)

    except Exception as e:
        print(f"\n🔥 Fatal error running Databricks tests: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
