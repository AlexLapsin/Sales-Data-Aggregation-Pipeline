#!/usr/bin/env python3
"""
Comprehensive dbt testing orchestration script
Runs all tests in order of priority and generates detailed reports
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import yaml


class DBTTestOrchestrator:
    """Orchestrates comprehensive dbt testing with detailed reporting"""

    def __init__(self, project_dir: str, profiles_dir: str = None, target: str = "dev"):
        self.project_dir = Path(project_dir)
        self.profiles_dir = profiles_dir or str(self.project_dir)
        self.target = target
        self.results = {}
        self.start_time = datetime.now()

        # Test categories in priority order
        self.test_categories = {
            "compilation": {
                "name": "Compilation & Syntax",
                "commands": ["parse", "compile"],
                "critical": True,
            },
            "source_freshness": {
                "name": "Source Freshness",
                "commands": ["source freshness"],
                "critical": False,
            },
            "schema_tests": {
                "name": "Schema Tests",
                "commands": ["test --select test_type:schema"],
                "critical": True,
            },
            "singular_tests": {
                "name": "Singular Tests",
                "commands": ["test --select test_type:singular"],
                "critical": True,
            },
            "custom_tests": {
                "name": "Custom Data Quality Tests",
                "commands": [
                    "test --select test_source_data_quality",
                    "test --select test_dimensional_model_integrity",
                    "test --select test_business_logic_validation",
                ],
                "critical": False,
            },
            "documentation": {
                "name": "Documentation Generation",
                "commands": ["docs generate"],
                "critical": False,
            },
        }

    def run_dbt_command(self, command: str, timeout: int = 300) -> Dict[str, Any]:
        """Execute dbt command with proper error handling"""
        full_command = [
            "dbt",
            *command.split(),
            "--project-dir",
            str(self.project_dir),
            "--profiles-dir",
            self.profiles_dir,
            "--target",
            self.target,
        ]

        print(f"Executing: {' '.join(full_command)}")

        try:
            result = subprocess.run(
                full_command,
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=self.project_dir,
            )

            return {
                "command": command,
                "success": result.returncode == 0,
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "duration": None,  # Would need timing logic
            }

        except subprocess.TimeoutExpired:
            return {
                "command": command,
                "success": False,
                "returncode": -1,
                "stdout": "",
                "stderr": f"Command timed out after {timeout} seconds",
                "duration": timeout,
            }
        except Exception as e:
            return {
                "command": command,
                "success": False,
                "returncode": -1,
                "stdout": "",
                "stderr": f"Execution error: {str(e)}",
                "duration": None,
            }

    def run_test_category(self, category: str) -> Dict[str, Any]:
        """Run all tests in a category"""
        category_config = self.test_categories[category]
        print(f"\n{'='*60}")
        print(f"Running {category_config['name']} Tests")
        print(f"{'='*60}")

        category_results = {
            "name": category_config["name"],
            "critical": category_config["critical"],
            "tests": [],
            "success": True,
            "summary": {"total": 0, "passed": 0, "failed": 0, "errors": 0},
        }

        for command in category_config["commands"]:
            print(f"\nRunning: {command}")
            result = self.run_dbt_command(command)
            category_results["tests"].append(result)
            category_results["summary"]["total"] += 1

            if result["success"]:
                category_results["summary"]["passed"] += 1
                print(f"✓ PASSED: {command}")
            else:
                category_results["summary"]["failed"] += 1
                category_results["success"] = False
                print(f"✗ FAILED: {command}")
                print(f"Error: {result['stderr']}")

        return category_results

    def analyze_test_results(self) -> Dict[str, Any]:
        """Analyze and parse detailed test results from dbt output"""
        run_results_path = self.project_dir / "target" / "run_results.json"

        analysis = {
            "total_models": 0,
            "total_tests": 0,
            "test_failures": [],
            "model_failures": [],
            "warnings": [],
            "performance_metrics": {},
        }

        if run_results_path.exists():
            try:
                with open(run_results_path, "r") as f:
                    run_results = json.load(f)

                for result in run_results.get("results", []):
                    resource_type = result.get("resource_type", "")
                    status = result.get("status", "")

                    if resource_type == "test":
                        analysis["total_tests"] += 1
                        if status in ["fail", "error"]:
                            analysis["test_failures"].append(
                                {
                                    "name": result.get("unique_id", ""),
                                    "status": status,
                                    "message": result.get("message", ""),
                                    "execution_time": result.get("execution_time", 0),
                                }
                            )
                    elif resource_type == "model":
                        analysis["total_models"] += 1
                        if status in ["fail", "error"]:
                            analysis["model_failures"].append(
                                {
                                    "name": result.get("unique_id", ""),
                                    "status": status,
                                    "message": result.get("message", ""),
                                }
                            )

                        # Collect performance metrics
                        exec_time = result.get("execution_time", 0)
                        if exec_time > 0:
                            model_name = result.get("unique_id", "").split(".")[-1]
                            analysis["performance_metrics"][model_name] = exec_time

            except Exception as e:
                print(f"Warning: Could not analyze run results: {e}")

        return analysis

    def generate_report(self) -> str:
        """Generate comprehensive test report"""
        end_time = datetime.now()
        duration = end_time - self.start_time

        # Calculate overall statistics
        total_categories = len(self.results)
        passed_categories = sum(1 for r in self.results.values() if r["success"])
        critical_failures = sum(
            1 for r in self.results.values() if not r["success"] and r["critical"]
        )

        # Analyze detailed results
        analysis = self.analyze_test_results()

        report = []
        report.append("=" * 80)
        report.append("DBT COMPREHENSIVE TEST REPORT")
        report.append("=" * 80)
        report.append(f"Execution Time: {duration.total_seconds():.2f} seconds")
        report.append(f"Target Environment: {self.target}")
        report.append(f"Project Directory: {self.project_dir}")
        report.append("")

        # Executive Summary
        report.append("EXECUTIVE SUMMARY")
        report.append("-" * 40)
        report.append(f"Test Categories: {passed_categories}/{total_categories} passed")
        report.append(f"Critical Failures: {critical_failures}")
        report.append(f"Total Models: {analysis['total_models']}")
        report.append(f"Total Tests: {analysis['total_tests']}")
        report.append(f"Test Failures: {len(analysis['test_failures'])}")
        report.append("")

        # Category Results
        report.append("CATEGORY RESULTS")
        report.append("-" * 40)
        for category, results in self.results.items():
            status = "PASS" if results["success"] else "FAIL"
            critical = " (CRITICAL)" if results["critical"] else ""
            report.append(f"{results['name']}: {status}{critical}")
            report.append(
                f"  Tests: {results['summary']['passed']}/{results['summary']['total']} passed"
            )

            # Show failures
            for test in results["tests"]:
                if not test["success"]:
                    report.append(f"    ✗ {test['command']}")
                    report.append(f"      Error: {test['stderr'][:200]}...")
            report.append("")

        # Test Failures Detail
        if analysis["test_failures"]:
            report.append("TEST FAILURES")
            report.append("-" * 40)
            for failure in analysis["test_failures"]:
                report.append(f"Test: {failure['name']}")
                report.append(f"Status: {failure['status']}")
                report.append(f"Message: {failure['message'][:300]}...")
                report.append(f"Execution Time: {failure['execution_time']:.3f}s")
                report.append("")

        # Performance Metrics
        if analysis["performance_metrics"]:
            report.append("PERFORMANCE METRICS")
            report.append("-" * 40)
            sorted_models = sorted(
                analysis["performance_metrics"].items(),
                key=lambda x: x[1],
                reverse=True,
            )
            for model, exec_time in sorted_models[:10]:  # Top 10 slowest
                report.append(f"{model}: {exec_time:.3f}s")
            report.append("")

        # Recommendations
        report.append("RECOMMENDATIONS")
        report.append("-" * 40)
        if critical_failures > 0:
            report.append("❌ CRITICAL FAILURES DETECTED - DO NOT DEPLOY")
        elif len(analysis["test_failures"]) > 0:
            report.append("WARNING: Test failures detected - Review before deployment")
        else:
            report.append("SUCCESS: All tests passed - Ready for deployment")

        return "\n".join(report)

    def run_all_tests(self, categories: Optional[List[str]] = None) -> bool:
        """Run all test categories"""
        categories_to_run = categories or list(self.test_categories.keys())

        print(f"Starting comprehensive dbt testing at {self.start_time}")
        print(f"Categories to run: {', '.join(categories_to_run)}")

        overall_success = True

        for category in categories_to_run:
            if category not in self.test_categories:
                print(f"Warning: Unknown test category '{category}', skipping")
                continue

            category_result = self.run_test_category(category)
            self.results[category] = category_result

            # If critical test fails, we might want to stop
            if not category_result["success"] and category_result["critical"]:
                print(f"\n❌ CRITICAL TEST FAILURE in {category}")
                overall_success = False
                # Continue running other tests for full report

        # Generate and display report
        report = self.generate_report()
        print("\n" + report)

        # Save report to file
        report_path = self.project_dir / "target" / "test_report.txt"
        report_path.parent.mkdir(exist_ok=True)
        with open(report_path, "w") as f:
            f.write(report)
        print(f"\nDetailed report saved to: {report_path}")

        return overall_success


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Run comprehensive dbt tests")
    parser.add_argument("--project-dir", default=".", help="dbt project directory")
    parser.add_argument(
        "--profiles-dir", help="dbt profiles directory (defaults to project dir)"
    )
    parser.add_argument("--target", default="dev", help="dbt target environment")
    parser.add_argument(
        "--categories", nargs="+", help="Specific test categories to run"
    )
    parser.add_argument(
        "--fail-fast", action="store_true", help="Stop on first critical failure"
    )

    args = parser.parse_args()

    orchestrator = DBTTestOrchestrator(
        project_dir=args.project_dir, profiles_dir=args.profiles_dir, target=args.target
    )

    success = orchestrator.run_all_tests(categories=args.categories)

    if not success:
        print("\n❌ TESTS FAILED")
        sys.exit(1)
    else:
        print("\nALL TESTS PASSED")
        sys.exit(0)


if __name__ == "__main__":
    main()
