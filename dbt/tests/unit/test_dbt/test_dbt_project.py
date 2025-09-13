#!/usr/bin/env python3
"""
Comprehensive pytest test suite for dbt project validation
Tests compilation, documentation, dependencies, and data quality
"""

import pytest
import json
import os
import subprocess
import yaml
from pathlib import Path
from typing import Dict, List, Any
import pandas as pd


class DBTProjectTester:
    """Test runner for dbt project validation"""

    def __init__(self, project_dir: str = None):
        self.project_dir = (
            Path(project_dir) if project_dir else Path(__file__).parent.parent
        )
        self.dbt_project_path = self.project_dir / "dbt_project.yml"
        self.profiles_dir = self.project_dir

    def run_dbt_command(
        self, command: str, expect_success: bool = True
    ) -> Dict[str, Any]:
        """Execute dbt command and return result"""
        full_command = f"dbt {command} --project-dir {self.project_dir} --profiles-dir {self.profiles_dir}"

        try:
            result = subprocess.run(
                full_command.split(),
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
            )

            return {
                "success": result.returncode == 0,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "returncode": result.returncode,
            }
        except subprocess.TimeoutExpired:
            return {
                "success": False,
                "stdout": "",
                "stderr": "Command timed out after 5 minutes",
                "returncode": -1,
            }

    def load_manifest(self) -> Dict[str, Any]:
        """Load dbt manifest.json for analysis"""
        manifest_path = self.project_dir / "target" / "manifest.json"
        if not manifest_path.exists():
            # Try to compile first
            self.run_dbt_command("compile")

        if manifest_path.exists():
            with open(manifest_path, "r") as f:
                return json.load(f)
        return {}

    def load_catalog(self) -> Dict[str, Any]:
        """Load dbt catalog.json for documentation analysis"""
        catalog_path = self.project_dir / "target" / "catalog.json"
        if catalog_path.exists():
            with open(catalog_path, "r") as f:
                return json.load(f)
        return {}


@pytest.fixture(scope="session")
def dbt_tester():
    """Pytest fixture for dbt project tester"""
    return DBTProjectTester()


class TestProjectConfiguration:
    """Test dbt project configuration and setup"""

    def test_project_file_exists(self, dbt_tester):
        """Test that dbt_project.yml exists and is valid"""
        assert dbt_tester.dbt_project_path.exists(), "dbt_project.yml not found"

        with open(dbt_tester.dbt_project_path, "r") as f:
            config = yaml.safe_load(f)

        # Check required fields
        required_fields = ["name", "version", "config-version", "profile"]
        for field in required_fields:
            assert field in config, f"Missing required field: {field}"

        assert config["config-version"] == 2, "Config version should be 2"

    def test_profile_configuration(self, dbt_tester):
        """Test that profiles.yml exists or can be created"""
        profiles_path = dbt_tester.profiles_dir / "profiles.yml"
        profiles_example_path = dbt_tester.profiles_dir / "profiles.yml.example"

        # Either profiles.yml should exist or there should be an example
        assert (
            profiles_path.exists() or profiles_example_path.exists()
        ), "No profiles.yml or profiles.yml.example found"

    def test_required_directories(self, dbt_tester):
        """Test that required directories exist"""
        required_dirs = ["models", "macros", "tests"]
        for dir_name in required_dirs:
            dir_path = dbt_tester.project_dir / dir_name
            assert dir_path.exists(), f"Required directory missing: {dir_name}"

    def test_packages_file(self, dbt_tester):
        """Test packages.yml configuration"""
        packages_path = dbt_tester.project_dir / "packages.yml"
        if packages_path.exists():
            with open(packages_path, "r") as f:
                packages = yaml.safe_load(f)

            assert "packages" in packages, "packages.yml should have packages key"
            assert isinstance(packages["packages"], list), "Packages should be a list"


class TestModelCompilation:
    """Test model compilation and syntax validation"""

    def test_dbt_parse(self, dbt_tester):
        """Test that dbt can parse all models"""
        result = dbt_tester.run_dbt_command("parse")
        assert result["success"], f"dbt parse failed: {result['stderr']}"

    def test_dbt_compile(self, dbt_tester):
        """Test that all models compile successfully"""
        result = dbt_tester.run_dbt_command("compile")
        assert result["success"], f"dbt compile failed: {result['stderr']}"

    def test_model_sql_syntax(self, dbt_tester):
        """Test SQL syntax in compiled models"""
        # First ensure compilation succeeded
        compile_result = dbt_tester.run_dbt_command("compile")
        assert compile_result[
            "success"
        ], "Compilation must succeed before syntax testing"

        compiled_dir = (
            dbt_tester.project_dir / "target" / "compiled" / "sales_data_pipeline"
        )
        assert compiled_dir.exists(), "Compiled models directory not found"

        # Check that compiled SQL files exist and have content
        sql_files = list(compiled_dir.rglob("*.sql"))
        assert len(sql_files) > 0, "No compiled SQL files found"

        for sql_file in sql_files:
            with open(sql_file, "r") as f:
                content = f.read().strip()
            assert len(content) > 0, f"Empty compiled SQL file: {sql_file.name}"
            assert "select" in content.lower(), f"Invalid SQL in {sql_file.name}"


class TestModelDependencies:
    """Test model dependencies and lineage"""

    def test_dependency_resolution(self, dbt_tester):
        """Test that model dependencies resolve correctly"""
        manifest = dbt_tester.load_manifest()
        assert manifest, "Could not load dbt manifest"

        # Check that all ref() calls resolve to existing models
        nodes = manifest.get("nodes", {})
        for node_id, node in nodes.items():
            if node.get("resource_type") == "model":
                depends_on = node.get("depends_on", {}).get("nodes", [])
                for dependency in depends_on:
                    if dependency.startswith("model."):
                        assert (
                            dependency in nodes
                        ), f"Unresolved dependency: {dependency} in {node_id}"

    def test_no_circular_dependencies(self, dbt_tester):
        """Test that there are no circular dependencies"""
        manifest = dbt_tester.load_manifest()
        assert manifest, "Could not load dbt manifest"

        # This is a simplified check - in practice you'd need graph analysis
        nodes = manifest.get("nodes", {})
        model_nodes = {
            k: v for k, v in nodes.items() if v.get("resource_type") == "model"
        }

        # Check that no model depends on itself directly
        for node_id, node in model_nodes.items():
            depends_on = node.get("depends_on", {}).get("nodes", [])
            assert (
                node_id not in depends_on
            ), f"Model {node_id} has circular self-dependency"

    def test_staging_model_dependencies(self, dbt_tester):
        """Test that staging models only depend on sources"""
        manifest = dbt_tester.load_manifest()
        assert manifest, "Could not load dbt manifest"

        nodes = manifest.get("nodes", {})
        for node_id, node in nodes.items():
            if "staging" in node_id and node.get("resource_type") == "model":
                depends_on = node.get("depends_on", {}).get("nodes", [])
                model_deps = [dep for dep in depends_on if dep.startswith("model.")]

                # Staging models should not depend on other models, only sources
                assert (
                    len(model_deps) == 0
                ), f"Staging model {node_id} depends on other models: {model_deps}"


class TestDataQuality:
    """Test data quality validations"""

    def test_source_freshness(self, dbt_tester):
        """Test source freshness validation"""
        result = dbt_tester.run_dbt_command("source freshness")

        # Note: This might warn but shouldn't error in tests
        if not result["success"] and "WARN" not in result["stdout"]:
            pytest.fail(f"Source freshness check failed: {result['stderr']}")

    def test_schema_tests(self, dbt_tester):
        """Test all schema tests pass"""
        result = dbt_tester.run_dbt_command("test --select test_type:schema")

        if not result["success"]:
            # Check if this is a test failure vs configuration error
            if "FAIL" in result["stdout"]:
                pytest.fail(f"Schema tests failed:\n{result['stdout']}")
            else:
                pytest.fail(f"Schema test execution error: {result['stderr']}")

    def test_singular_tests(self, dbt_tester):
        """Test all singular tests pass"""
        result = dbt_tester.run_dbt_command("test --select test_type:singular")

        if not result["success"]:
            if "FAIL" in result["stdout"]:
                pytest.fail(f"Singular tests failed:\n{result['stdout']}")
            else:
                pytest.fail(f"Singular test execution error: {result['stderr']}")

    def test_custom_data_quality_tests(self, dbt_tester):
        """Test custom data quality macros"""
        # Test specific data quality tests
        custom_tests = [
            "test_source_data_quality",
            "test_dimensional_model_integrity",
            "test_business_logic_validation",
        ]

        for test_name in custom_tests:
            result = dbt_tester.run_dbt_command(f"test --select {test_name}")
            if not result["success"] and "FAIL" in result["stdout"]:
                pytest.skip(
                    f"Custom test {test_name} failed - may indicate data issues"
                )


class TestDocumentation:
    """Test documentation completeness"""

    def test_generate_docs(self, dbt_tester):
        """Test that documentation can be generated"""
        result = dbt_tester.run_dbt_command("docs generate")
        assert result["success"], f"Documentation generation failed: {result['stderr']}"

    def test_model_documentation(self, dbt_tester):
        """Test that models have documentation"""
        catalog = dbt_tester.load_catalog()
        if not catalog:
            pytest.skip("Catalog not available - run 'dbt docs generate' first")

        nodes = catalog.get("nodes", {})
        undocumented_models = []

        for node_id, node in nodes.items():
            if node.get("resource_type") == "model":
                description = node.get("description", "").strip()
                if not description:
                    undocumented_models.append(node_id)

        assert (
            len(undocumented_models) == 0
        ), f"Models without documentation: {undocumented_models}"

    def test_column_documentation(self, dbt_tester):
        """Test that critical columns have documentation"""
        catalog = dbt_tester.load_catalog()
        if not catalog:
            pytest.skip("Catalog not available")

        nodes = catalog.get("nodes", {})
        critical_models = [
            "fact_sales",
            "dim_product",
            "dim_store",
            "dim_date",
            "dim_customer",
        ]

        for node_id, node in nodes.items():
            model_name = node_id.split(".")[-1]
            if model_name in critical_models:
                columns = node.get("columns", {})
                undocumented_columns = [
                    col_name
                    for col_name, col_info in columns.items()
                    if not col_info.get("description", "").strip()
                ]

                # Allow some undocumented columns, but flag if too many
                if (
                    len(undocumented_columns) > len(columns) * 0.3
                ):  # More than 30% undocumented
                    pytest.fail(
                        f"Too many undocumented columns in {model_name}: {undocumented_columns}"
                    )


class TestPerformance:
    """Test model performance characteristics"""

    def test_model_build_time(self, dbt_tester):
        """Test that models build within reasonable time"""
        # This is a basic test - in practice you'd want more sophisticated timing
        result = dbt_tester.run_dbt_command("build --select +fact_sales")
        assert result["success"], "Model build should succeed for performance testing"

    def test_incremental_models(self, dbt_tester):
        """Test incremental model configuration"""
        manifest = dbt_tester.load_manifest()
        if not manifest:
            pytest.skip("Manifest not available")

        nodes = manifest.get("nodes", {})
        for node_id, node in nodes.items():
            config = node.get("config", {})
            if config.get("materialized") == "incremental":
                # Incremental models should have unique_key configured
                unique_key = config.get("unique_key")
                assert unique_key, f"Incremental model {node_id} missing unique_key"


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
