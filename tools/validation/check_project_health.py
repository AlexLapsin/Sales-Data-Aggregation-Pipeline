#!/usr/bin/env python3
"""
dbt Project Health Checker
Validates project structure, documentation completeness, and best practices
"""

import json
import os
import re
import yaml
from pathlib import Path
from typing import Dict, List, Any, Set
from dataclasses import dataclass
from collections import defaultdict


@dataclass
class HealthIssue:
    """Represents a project health issue"""

    category: str
    severity: str  # 'error', 'warning', 'info'
    message: str
    file_path: str = ""
    line_number: int = 0


class DBTProjectHealthChecker:
    """Comprehensive dbt project health checker"""

    def __init__(self, project_dir: str):
        self.project_dir = Path(project_dir)
        self.issues: List[HealthIssue] = []
        self.manifest = {}
        self.catalog = {}
        self.project_config = {}

        self.load_project_artifacts()

    def load_project_artifacts(self):
        """Load dbt project artifacts for analysis"""
        try:
            # Load dbt_project.yml
            project_yml_path = self.project_dir / "dbt_project.yml"
            if project_yml_path.exists():
                with open(project_yml_path, "r") as f:
                    self.project_config = yaml.safe_load(f)

            # Load manifest.json
            manifest_path = self.project_dir / "target" / "manifest.json"
            if manifest_path.exists():
                with open(manifest_path, "r") as f:
                    self.manifest = json.load(f)

            # Load catalog.json
            catalog_path = self.project_dir / "target" / "catalog.json"
            if catalog_path.exists():
                with open(catalog_path, "r") as f:
                    self.catalog = json.load(f)

        except Exception as e:
            self.add_issue(
                "artifacts", "error", f"Failed to load project artifacts: {e}"
            )

    def add_issue(
        self,
        category: str,
        severity: str,
        message: str,
        file_path: str = "",
        line_number: int = 0,
    ):
        """Add a health issue"""
        self.issues.append(
            HealthIssue(category, severity, message, file_path, line_number)
        )

    def check_project_structure(self):
        """Check basic project structure and required files"""
        required_files = ["dbt_project.yml", "models", "macros"]

        for required in required_files:
            path = self.project_dir / required
            if not path.exists():
                self.add_issue(
                    "structure", "error", f"Required file/directory missing: {required}"
                )

        # Check for profiles.yml or profiles.yml.example
        profiles_path = self.project_dir / "profiles.yml"
        profiles_example_path = self.project_dir / "profiles.yml.example"
        if not profiles_path.exists() and not profiles_example_path.exists():
            self.add_issue(
                "structure", "warning", "No profiles.yml or profiles.yml.example found"
            )

        # Check for .gitignore
        gitignore_path = self.project_dir / ".gitignore"
        if not gitignore_path.exists():
            self.add_issue("structure", "warning", "No .gitignore file found")
        else:
            self.check_gitignore_content()

    def check_gitignore_content(self):
        """Check .gitignore has appropriate dbt entries"""
        gitignore_path = self.project_dir / ".gitignore"
        required_entries = ["target/", "dbt_packages/", "logs/", "profiles.yml"]

        try:
            with open(gitignore_path, "r") as f:
                gitignore_content = f.read()

            for entry in required_entries:
                if entry not in gitignore_content:
                    self.add_issue(
                        "structure",
                        "warning",
                        f"Missing recommended .gitignore entry: {entry}",
                        str(gitignore_path),
                    )
        except Exception as e:
            self.add_issue("structure", "error", f"Could not read .gitignore: {e}")

    def check_model_documentation(self):
        """Check model and column documentation completeness"""
        if not self.manifest:
            self.add_issue(
                "documentation",
                "warning",
                "No manifest available for documentation check",
            )
            return

        nodes = self.manifest.get("nodes", {})
        undocumented_models = []
        poorly_documented_models = []

        for node_id, node in nodes.items():
            if node.get("resource_type") != "model":
                continue

            model_name = node_id.split(".")[-1]
            description = node.get("description", "").strip()

            # Check model description
            if not description:
                undocumented_models.append(model_name)
            elif len(description) < 20:
                poorly_documented_models.append(model_name)

            # Check column documentation
            columns = node.get("columns", {})
            total_columns = len(columns)
            documented_columns = sum(
                1 for col in columns.values() if col.get("description", "").strip()
            )

            if total_columns > 0:
                doc_percentage = documented_columns / total_columns
                if doc_percentage < 0.5:  # Less than 50% documented
                    self.add_issue(
                        "documentation",
                        "warning",
                        f"Model {model_name} has {doc_percentage:.1%} column documentation",
                    )

        if undocumented_models:
            self.add_issue(
                "documentation",
                "error",
                f"Models without descriptions: {', '.join(undocumented_models[:10])}"
                + (
                    f" and {len(undocumented_models)-10} more"
                    if len(undocumented_models) > 10
                    else ""
                ),
            )

        if poorly_documented_models:
            self.add_issue(
                "documentation",
                "warning",
                f"Models with brief descriptions: {', '.join(poorly_documented_models[:5])}"
                + (
                    f" and {len(poorly_documented_models)-5} more"
                    if len(poorly_documented_models) > 5
                    else ""
                ),
            )

    def check_model_naming_conventions(self):
        """Check model naming follows conventions"""
        if not self.manifest:
            return

        nodes = self.manifest.get("nodes", {})
        naming_issues = []

        for node_id, node in nodes.items():
            if node.get("resource_type") != "model":
                continue

            model_name = node_id.split(".")[-1]
            model_path = node.get("original_file_path", "")

            # Check staging model naming
            if "staging" in model_path:
                if not model_name.startswith("stg_"):
                    naming_issues.append(
                        f"Staging model {model_name} should start with 'stg_'"
                    )

            # Check intermediate model naming
            elif "intermediate" in model_path:
                if not model_name.startswith("int_"):
                    naming_issues.append(
                        f"Intermediate model {model_name} should start with 'int_'"
                    )

            # Check marts model naming
            elif "marts" in model_path:
                if not (
                    model_name.startswith("dim_")
                    or model_name.startswith("fact_")
                    or model_name.startswith("fct_")
                ):
                    naming_issues.append(
                        f"Marts model {model_name} should start with 'dim_', 'fact_', or 'fct_'"
                    )

        for issue in naming_issues:
            self.add_issue("conventions", "warning", issue)

    def check_test_coverage(self):
        """Check test coverage across models"""
        if not self.manifest:
            return

        nodes = self.manifest.get("nodes", {})
        tests = self.manifest.get("tests", {})

        # Count models and tests
        models = {k: v for k, v in nodes.items() if v.get("resource_type") == "model"}
        model_tests = defaultdict(list)

        # Map tests to models
        for test_id, test in tests.items():
            depends_on = test.get("depends_on", {}).get("nodes", [])
            for dep in depends_on:
                if dep in models:
                    model_tests[dep].append(test_id)

        # Check coverage
        untested_models = []
        lightly_tested_models = []

        for model_id, model in models.items():
            model_name = model_id.split(".")[-1]
            test_count = len(model_tests.get(model_id, []))

            if test_count == 0:
                untested_models.append(model_name)
            elif test_count < 3:  # Arbitrary threshold
                lightly_tested_models.append(model_name)

        if untested_models:
            self.add_issue(
                "testing",
                "error",
                f"Models with no tests: {', '.join(untested_models[:10])}"
                + (
                    f" and {len(untested_models)-10} more"
                    if len(untested_models) > 10
                    else ""
                ),
            )

        if lightly_tested_models:
            self.add_issue(
                "testing",
                "warning",
                f"Models with minimal tests: {', '.join(lightly_tested_models[:10])}"
                + (
                    f" and {len(lightly_tested_models)-10} more"
                    if len(lightly_tested_models) > 10
                    else ""
                ),
            )

    def check_sql_style(self):
        """Check SQL style and best practices"""
        models_dir = self.project_dir / "models"
        if not models_dir.exists():
            return

        sql_files = list(models_dir.rglob("*.sql"))

        for sql_file in sql_files:
            try:
                with open(sql_file, "r") as f:
                    content = f.read()

                # Check for common SQL anti-patterns
                if "select *" in content.lower() and "from (" not in content.lower():
                    self.add_issue(
                        "style",
                        "warning",
                        f"Contains 'SELECT *' which may impact performance",
                        str(sql_file),
                    )

                # Check for proper indentation (basic check)
                lines = content.split("\n")
                for i, line in enumerate(lines, 1):
                    # Check for tabs (should use spaces)
                    if "\t" in line:
                        self.add_issue(
                            "style",
                            "info",
                            f"Uses tabs instead of spaces for indentation",
                            str(sql_file),
                            i,
                        )
                        break

                # Check for model structure
                if not content.strip().startswith(
                    "{{"
                ) and not content.strip().startswith("--"):
                    if "config(" not in content:
                        self.add_issue(
                            "style",
                            "info",
                            f"Model missing config block",
                            str(sql_file),
                        )

            except Exception as e:
                self.add_issue(
                    "style", "error", f"Could not read SQL file: {e}", str(sql_file)
                )

    def check_macro_usage(self):
        """Check for proper macro usage and organization"""
        macros_dir = self.project_dir / "macros"
        if not macros_dir.exists():
            return

        macro_files = list(macros_dir.rglob("*.sql"))

        if len(macro_files) == 0:
            self.add_issue("macros", "warning", "No custom macros found")
            return

        for macro_file in macro_files:
            try:
                with open(macro_file, "r") as f:
                    content = f.read()

                # Check for macro documentation
                macro_count = content.count("{% macro")
                doc_count = content.count("--") + content.count("{#")

                if macro_count > 0 and doc_count == 0:
                    self.add_issue(
                        "macros",
                        "warning",
                        f"Macro file has no documentation comments",
                        str(macro_file),
                    )

            except Exception as e:
                self.add_issue(
                    "macros",
                    "error",
                    f"Could not read macro file: {e}",
                    str(macro_file),
                )

    def check_dependencies(self):
        """Check package dependencies and versions"""
        packages_yml = self.project_dir / "packages.yml"
        if not packages_yml.exists():
            self.add_issue("dependencies", "warning", "No packages.yml found")
            return

        try:
            with open(packages_yml, "r") as f:
                packages_config = yaml.safe_load(f)

            packages = packages_config.get("packages", [])

            for package in packages:
                if "version" not in package:
                    package_name = package.get("package", package.get("git", "unknown"))
                    self.add_issue(
                        "dependencies",
                        "warning",
                        f"Package {package_name} has no version specified",
                    )

        except Exception as e:
            self.add_issue(
                "dependencies", "error", f"Could not parse packages.yml: {e}"
            )

    def run_all_checks(self):
        """Run all health checks"""
        print("Running dbt project health checks...")

        self.check_project_structure()
        self.check_model_documentation()
        self.check_model_naming_conventions()
        self.check_test_coverage()
        self.check_sql_style()
        self.check_macro_usage()
        self.check_dependencies()

    def generate_report(self) -> str:
        """Generate health check report"""
        # Group issues by category and severity
        categories = defaultdict(lambda: defaultdict(list))
        for issue in self.issues:
            categories[issue.category][issue.severity].append(issue)

        report = []
        report.append("=" * 80)
        report.append("DBT PROJECT HEALTH REPORT")
        report.append("=" * 80)

        # Summary
        total_issues = len(self.issues)
        error_count = sum(1 for issue in self.issues if issue.severity == "error")
        warning_count = sum(1 for issue in self.issues if issue.severity == "warning")
        info_count = sum(1 for issue in self.issues if issue.severity == "info")

        report.append(f"Total Issues: {total_issues}")
        report.append(f"Errors: {error_count}")
        report.append(f"Warnings: {warning_count}")
        report.append(f"Info: {info_count}")
        report.append("")

        # Health Score
        max_score = 100
        error_penalty = error_count * 10
        warning_penalty = warning_count * 3
        info_penalty = info_count * 1

        health_score = max(
            0, max_score - error_penalty - warning_penalty - info_penalty
        )
        report.append(f"Health Score: {health_score}/100")

        if health_score >= 90:
            report.append("Status: EXCELLENT")
        elif health_score >= 75:
            report.append("Status: GOOD")
        elif health_score >= 50:
            report.append("Status: NEEDS IMPROVEMENT")
        else:
            report.append("Status: POOR")

        report.append("")

        # Detailed issues by category
        for category, severities in categories.items():
            report.append(f"{category.upper()} ISSUES")
            report.append("-" * 40)

            for severity in ["error", "warning", "info"]:
                if severity in severities:
                    issues = severities[severity]
                    report.append(f"{severity.upper()} ({len(issues)} issues):")
                    for issue in issues:
                        prefix = (
                            "ERROR: "
                            if severity == "error"
                            else "WARNING: "
                            if severity == "warning"
                            else "INFO: "
                        )
                        report.append(f"  {prefix} {issue.message}")
                        if issue.file_path:
                            location = issue.file_path
                            if issue.line_number:
                                location += f":{issue.line_number}"
                            report.append(f"    Location: {location}")
                    report.append("")
            report.append("")

        return "\n".join(report)


def main():
    """Main CLI entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Check dbt project health")
    parser.add_argument("--project-dir", default=".", help="dbt project directory")
    parser.add_argument("--output-file", help="Save report to file")

    args = parser.parse_args()

    checker = DBTProjectHealthChecker(args.project_dir)
    checker.run_all_checks()

    report = checker.generate_report()
    print(report)

    if args.output_file:
        with open(args.output_file, "w") as f:
            f.write(report)
        print(f"\nReport saved to: {args.output_file}")


if __name__ == "__main__":
    main()
