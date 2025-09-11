#!/usr/bin/env python3
"""
Test runner script for Airflow DAG tests
Provides easy command-line interface for running different test suites
"""

import argparse
import sys
import subprocess
import os
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "airflow" / "dags"))


def run_command(cmd, description=""):
    """Run a command and handle the output"""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*60}")

    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        print(f"\n{description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n{description} failed with exit code {e.returncode}")
        return False
    except KeyboardInterrupt:
        print(f"\nWARNING: {description} interrupted by user")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Run Airflow DAG tests",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_tests.py --all                    # Run all tests
  python run_tests.py --unit                   # Run unit tests only
  python run_tests.py --integration            # Run integration tests
  python run_tests.py --performance            # Run performance tests
  python run_tests.py --coverage               # Run with coverage report
  python run_tests.py --file test_cloud_*      # Run specific test files
  python run_tests.py --dag cloud_sales        # Test specific DAG
  python run_tests.py --quick                  # Quick test suite (unit + integration)
  python run_tests.py --ci                     # CI/CD optimized run
        """,
    )

    # Test selection arguments
    test_group = parser.add_mutually_exclusive_group()
    test_group.add_argument(
        "--all", action="store_true", help="Run all tests (default)"
    )
    test_group.add_argument("--unit", action="store_true", help="Run unit tests only")
    test_group.add_argument(
        "--integration", action="store_true", help="Run integration tests only"
    )
    test_group.add_argument(
        "--performance", action="store_true", help="Run performance tests only"
    )
    test_group.add_argument(
        "--external", action="store_true", help="Run external service tests only"
    )
    test_group.add_argument(
        "--quick",
        action="store_true",
        help="Run quick test suite (unit + integration, no slow tests)",
    )
    test_group.add_argument(
        "--ci", action="store_true", help="Run CI/CD optimized test suite"
    )

    # Specific test selection
    parser.add_argument(
        "--file", type=str, help="Run specific test file(s) (supports wildcards)"
    )
    parser.add_argument(
        "--dag",
        type=str,
        choices=["cloud_sales", "monitoring", "maintenance"],
        help="Test specific DAG",
    )
    parser.add_argument(
        "--function", type=str, help="Run specific test function (use with --file)"
    )

    # Output and reporting options
    parser.add_argument(
        "--coverage", action="store_true", help="Generate coverage report"
    )
    parser.add_argument(
        "--html-report", action="store_true", help="Generate HTML test report"
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument(
        "--quiet", "-q", action="store_true", help="Quiet output (minimal)"
    )
    parser.add_argument(
        "--parallel",
        "-n",
        type=int,
        default=1,
        help="Run tests in parallel (requires pytest-xdist)",
    )

    # Test behavior options
    parser.add_argument(
        "--fail-fast", "-x", action="store_true", help="Stop on first failure"
    )
    parser.add_argument(
        "--rerun-failures",
        action="store_true",
        help="Only rerun failed tests from last run",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Timeout per test in seconds (default: 300)",
    )

    # Debug options
    parser.add_argument(
        "--pdb", action="store_true", help="Drop into debugger on failure"
    )
    parser.add_argument(
        "--capture",
        choices=["yes", "no", "sys"],
        default="yes",
        help="Capture stdout/stderr",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set log level",
    )

    args = parser.parse_args()

    # Build pytest command
    cmd = ["pytest", "tests/airflow/"]

    # Add test selection markers
    if args.unit:
        cmd.extend(["-m", "unit"])
    elif args.integration:
        cmd.extend(["-m", "integration"])
    elif args.performance:
        cmd.extend(["-m", "slow"])
    elif args.external:
        cmd.extend(["-m", "external"])
    elif args.quick:
        cmd.extend(["-m", "unit or integration and not slow"])
    elif args.ci:
        cmd.extend(["-m", "unit or integration"])
        cmd.extend(["--maxfail=5"])  # Stop after 5 failures in CI
    elif args.all:
        pass  # Run all tests
    elif not any([args.file, args.dag, args.function]):
        # Default to all tests if no specific selection
        pass

    # Add specific file/DAG selection
    if args.file:
        cmd = ["pytest"] + [f"tests/airflow/{args.file}"]
    elif args.dag:
        dag_file_map = {
            "cloud_sales": "test_cloud_sales_pipeline_dag.py",
            "monitoring": "test_pipeline_monitoring_dag.py",
            "maintenance": "test_maintenance_dag.py",
        }
        cmd = ["pytest", f"tests/airflow/{dag_file_map[args.dag]}"]

    # Add specific function
    if args.function:
        if "::" not in cmd[-1]:
            cmd[-1] += f"::{args.function}"
        else:
            cmd[-1] += f"::{args.function}"

    # Add coverage options
    if args.coverage:
        cmd.extend(["--cov=airflow/dags", "--cov-report=term-missing"])
        if args.html_report:
            cmd.extend(["--cov-report=html"])

    # Add output options
    if args.verbose:
        cmd.append("-v")
    elif args.quiet:
        cmd.append("-q")

    if args.html_report and not args.coverage:
        cmd.extend(["--html=test-report.html", "--self-contained-html"])

    # Add parallel execution
    if args.parallel > 1:
        cmd.extend(["-n", str(args.parallel)])

    # Add behavior options
    if args.fail_fast:
        cmd.append("-x")

    if args.rerun_failures:
        cmd.append("--lf")

    if args.timeout:
        cmd.extend(["--timeout", str(args.timeout)])

    # Add debug options
    if args.pdb:
        cmd.append("--pdb")

    if args.capture != "yes":
        cmd.extend(["-s"] if args.capture == "no" else ["--capture=sys"])

    # Set log level
    cmd.extend(["--log-cli-level", args.log_level])

    # Add JUnit XML for CI
    if args.ci:
        cmd.extend(["--junitxml=test-results.xml"])

    # Set environment variables
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{PROJECT_ROOT}:{PROJECT_ROOT}/airflow/dags"
    env["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
    env["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    env["AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS"] = "False"

    # Show configuration
    print("Airflow DAG Test Runner")
    print(f"Project Root: {PROJECT_ROOT}")
    print(f"Python Path: {env.get('PYTHONPATH', 'Not set')}")
    print(f"Test Mode: {'CI/CD' if args.ci else 'Development'}")

    # Run the tests
    success = run_command(cmd, "Airflow DAG Tests")

    if success:
        print("\nAll tests completed successfully!")

        # Show additional information
        if args.coverage:
            print("\nCoverage report generated")
            if args.html_report:
                print("   HTML report: htmlcov/index.html")

        if args.html_report and not args.coverage:
            print("\nHTML test report: test-report.html")

        if args.ci:
            print("\nJUnit XML report: test-results.xml")

    else:
        print("\nSome tests failed!")
        print("\nTroubleshooting tips:")
        print(
            "  • Check that all dependencies are installed: pip install -r requirements-dev.txt"
        )
        print("  • Verify environment variables are set correctly")
        print("  • Run with --verbose for more detailed output")
        print("  • Use --pdb to debug failing tests")
        print("  • Check the test documentation: tests/airflow/README.md")

        sys.exit(1)


if __name__ == "__main__":
    main()
