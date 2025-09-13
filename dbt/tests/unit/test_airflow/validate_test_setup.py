#!/usr/bin/env python3
"""
Validation script to verify test setup and environment
"""

import sys
import os
from pathlib import Path
import importlib.util


def validate_python_path():
    """Validate Python path includes necessary directories"""
    print("Validating Python Path...")

    project_root = Path(__file__).parent.parent.parent
    dag_dir = project_root / "airflow" / "dags"

    required_paths = [str(project_root), str(dag_dir)]

    for path in required_paths:
        if path not in sys.path:
            sys.path.insert(0, path)
            print(f"   Added to Python path: {path}")
        else:
            print(f"   Already in Python path: {path}")

    return True


def validate_dag_imports():
    """Validate that all DAG files can be imported"""
    print("\nValidating DAG Imports...")

    dag_files = [
        "cloud_sales_pipeline_dag",
        "pipeline_monitoring_dag",
        "maintenance_dag",
    ]

    success = True
    for dag_file in dag_files:
        try:
            module = importlib.import_module(dag_file)

            # Find DAG object
            dag_found = False
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if hasattr(attr, "dag_id") and hasattr(attr, "tasks"):
                    print(
                        f"   {dag_file}: Found DAG '{attr.dag_id}' with {len(attr.tasks)} tasks"
                    )
                    dag_found = True
                    break

            if not dag_found:
                print(f"   ERROR: {dag_file}: No DAG object found")
                success = False

        except Exception as e:
            print(f"   ERROR: {dag_file}: Import failed - {e}")
            success = False

    return success


def validate_test_dependencies():
    """Validate test dependencies are available"""
    print("\nValidating Test Dependencies...")

    required_packages = [
        ("pytest", "Testing framework"),
        ("airflow", "Apache Airflow"),
        ("unittest.mock", "Mock library"),
        ("datetime", "Date/time handling"),
        ("json", "JSON handling"),
        ("os", "Operating system interface"),
        ("sys", "System interface"),
    ]

    success = True
    for package, description in required_packages:
        try:
            if "." in package:
                # Handle submodules
                parts = package.split(".")
                module = importlib.import_module(parts[0])
                for part in parts[1:]:
                    module = getattr(module, part)
            else:
                importlib.import_module(package)
            print(f"   {package}: {description}")
        except ImportError as e:
            print(f"   ERROR: {package}: Missing - {e}")
            success = False

    return success


def validate_environment_variables():
    """Validate environment variables are set up correctly"""
    print("\nValidating Environment Variables...")

    # Check if we're in test mode
    test_mode = os.getenv("AIRFLOW__CORE__UNIT_TEST_MODE")
    if test_mode == "True":
        print("   Test mode enabled")
    else:
        print("   WARNING: Test mode not enabled (will be set during testing)")

    # Check for example environment file
    project_root = Path(__file__).parent.parent.parent
    env_example = project_root / ".env.example"

    if env_example.exists():
        print("   .env.example file found")
    else:
        print("   WARNING: .env.example file not found")

    return True


def validate_test_files():
    """Validate test files exist and are properly structured"""
    print("\nValidating Test Files...")

    test_dir = Path(__file__).parent
    expected_files = [
        "conftest.py",
        "test_utils.py",
        "pytest.ini",
        "test_cloud_sales_pipeline_dag.py",
        "test_pipeline_monitoring_dag.py",
        "test_maintenance_dag.py",
        "test_dag_integration.py",
        "test_dag_performance.py",
        "test_dag_edge_cases.py",
        "README.md",
    ]

    success = True
    for file_name in expected_files:
        file_path = test_dir / file_name
        if file_path.exists():
            file_size = file_path.stat().st_size
            print(f"   {file_name}: {file_size:,} bytes")
        else:
            print(f"   ERROR: {file_name}: Missing")
            success = False

    return success


def validate_test_structure():
    """Validate test structure and syntax"""
    print("\nValidating Test Structure...")

    test_files = [
        "test_cloud_sales_pipeline_dag.py",
        "test_pipeline_monitoring_dag.py",
        "test_maintenance_dag.py",
        "test_dag_integration.py",
        "test_dag_performance.py",
        "test_dag_edge_cases.py",
    ]

    success = True
    for test_file in test_files:
        try:
            spec = importlib.util.spec_from_file_location(
                test_file[:-3], Path(__file__).parent / test_file
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Count test classes and functions
            test_classes = [name for name in dir(module) if name.startswith("Test")]
            test_functions = [name for name in dir(module) if name.startswith("test_")]

            print(
                f"   {test_file}: {len(test_classes)} test classes, {len(test_functions)} test functions"
            )

        except Exception as e:
            print(f"   ERROR: {test_file}: Syntax error - {e}")
            success = False

    return success


def run_sample_test():
    """Run a simple sample test to verify everything works"""
    print("\nRunning Sample Test...")

    try:
        # Simple import test
        from test_utils import MockAirflowContext, get_test_env_vars

        # Create test context
        context = MockAirflowContext()
        assert context.ds == "2024-01-01"

        # Get test environment variables
        env_vars = get_test_env_vars()
        assert isinstance(env_vars, dict)
        assert "SNOWFLAKE_ACCOUNT" in env_vars

        print("   Sample test passed")
        return True

    except Exception as e:
        print(f"   ERROR: Sample test failed - {e}")
        return False


def main():
    """Main validation function"""
    print("Airflow DAG Test Setup Validation")
    print("=" * 50)

    validations = [
        validate_python_path,
        validate_test_dependencies,
        validate_environment_variables,
        validate_test_files,
        validate_test_structure,
        validate_dag_imports,
        run_sample_test,
    ]

    results = []
    for validation in validations:
        try:
            result = validation()
            results.append(result)
        except Exception as e:
            print(f"   ERROR: Validation failed - {e}")
            results.append(False)

    print("\n" + "=" * 50)
    print("Validation Summary")
    print("=" * 50)

    total_validations = len(results)
    passed_validations = sum(results)

    print(f"Total Validations: {total_validations}")
    print(f"Passed: {passed_validations}")
    print(f"Failed: {total_validations - passed_validations}")
    print(f"Success Rate: {passed_validations/total_validations*100:.1f}%")

    if all(results):
        print("\nAll validations passed! Test setup is ready.")
        print("\nNext steps:")
        print("1. Run tests: python tests/airflow/run_tests.py --quick")
        print("2. View documentation: tests/airflow/README.md")
        print(
            "3. Run specific test: pytest tests/airflow/test_cloud_sales_pipeline_dag.py -v"
        )
        return True
    else:
        print("\nWARNING: Some validations failed. Please fix the issues above.")
        print("\nTroubleshooting:")
        print("1. Install dependencies: pip install -r requirements-dev.txt")
        print("2. Check Python path and imports")
        print("3. Verify DAG files are accessible")
        print("4. Review test documentation")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
