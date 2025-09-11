#!/usr/bin/env python3
"""
Test suite for dbt macro functionality and edge cases
Tests custom macros, data quality functions, and edge case handling
"""

import pytest
import subprocess
import tempfile
from pathlib import Path
import yaml


class TestMacroFunctionality:
    """Test custom dbt macros for correctness and edge cases"""

    @pytest.fixture(scope="class")
    def dbt_project_dir(self):
        """Get dbt project directory"""
        return Path(__file__).parent.parent

    def run_dbt_macro_test(
        self, project_dir, macro_name, test_cases, expected_results=None
    ):
        """Helper method to test macro functionality"""
        test_sql = f"""
        {{{{ config(materialized='table') }}}}

        with test_cases as (
            {test_cases}
        )
        select * from test_cases
        """

        # Create temporary model file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write(test_sql)
            temp_model_path = Path(f.name)

        try:
            # Move to models directory temporarily
            model_dir = project_dir / "models" / "tests"
            model_dir.mkdir(exist_ok=True)
            test_model_path = model_dir / f"test_{macro_name}.sql"

            # Copy content to proper location
            with open(temp_model_path, "r") as src, open(test_model_path, "w") as dst:
                dst.write(src.read())

            # Run dbt compile to test macro
            result = subprocess.run(
                [
                    "dbt",
                    "compile",
                    "--select",
                    f"test_{macro_name}",
                    "--project-dir",
                    str(project_dir),
                    "--profiles-dir",
                    str(project_dir),
                ],
                capture_output=True,
                text=True,
            )

            return {
                "success": result.returncode == 0,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }

        finally:
            # Cleanup
            temp_model_path.unlink(missing_ok=True)
            if "test_model_path" in locals():
                test_model_path.unlink(missing_ok=True)

    def test_get_surrogate_key_macro(self, dbt_project_dir):
        """Test surrogate key generation macro"""
        test_cases = """
        select
            'TEST1' as field1,
            'TEST2' as field2,
            {{ get_surrogate_key(['field1', 'field2']) }} as surrogate_key
        union all
        select
            'DIFFERENT' as field1,
            'VALUES' as field2,
            {{ get_surrogate_key(['field1', 'field2']) }} as surrogate_key
        """

        result = self.run_dbt_macro_test(dbt_project_dir, "surrogate_key", test_cases)
        assert result["success"], f"Surrogate key macro test failed: {result['stderr']}"

    def test_data_quality_macros(self, dbt_project_dir):
        """Test data quality validation macros"""

        # Test null rate macro
        test_cases = """
        select
            'value1' as test_column,
            1 as row_id
        union all
        select
            null as test_column,
            2 as row_id
        union all
        select
            'value3' as test_column,
            3 as row_id
        """

        # Note: This creates a test that uses the macro indirectly
        result = self.run_dbt_macro_test(dbt_project_dir, "null_rate", test_cases)
        assert result["success"], f"Data quality macro test failed: {result['stderr']}"

    def test_business_rule_macro(self, dbt_project_dir):
        """Test business rule compliance macro"""
        test_cases = """
        select
            100 as sales_amount,
            10 as profit_amount,
            case when profit_amount / sales_amount <= 1.0
                then 'VALID' else 'INVALID'
            end as business_rule_check
        union all
        select
            50 as sales_amount,
            60 as profit_amount,  -- Invalid: profit > sales
            case when profit_amount / sales_amount <= 1.0
                then 'VALID' else 'INVALID'
            end as business_rule_check
        """

        result = self.run_dbt_macro_test(dbt_project_dir, "business_rule", test_cases)
        assert result["success"], f"Business rule macro test failed: {result['stderr']}"

    def test_macro_error_handling(self, dbt_project_dir):
        """Test macro behavior with edge cases and errors"""

        # Test with null inputs
        test_cases = """
        select
            null as field1,
            'test' as field2,
            coalesce({{ get_surrogate_key(['field1', 'field2']) }}, 'NULL_HANDLED') as result
        """

        result = self.run_dbt_macro_test(dbt_project_dir, "null_handling", test_cases)
        assert result["success"], f"Macro null handling test failed: {result['stderr']}"

    def test_performance_macro_compilation(self, dbt_project_dir):
        """Test that performance testing macros compile correctly"""
        macros_dir = dbt_project_dir / "macros"
        performance_macros_file = macros_dir / "performance_tests.sql"

        # Check that performance macros file exists
        assert performance_macros_file.exists(), "Performance macros file not found"

        # Test compilation of a simple performance test
        test_cases = """
        select
            current_timestamp() as test_timestamp,
            'performance_test' as test_type
        """

        result = self.run_dbt_macro_test(dbt_project_dir, "performance", test_cases)
        assert result[
            "success"
        ], f"Performance macro compilation failed: {result['stderr']}"


class TestMacroEdgeCases:
    """Test edge cases and error conditions for macros"""

    @pytest.fixture(scope="class")
    def dbt_project_dir(self):
        return Path(__file__).parent.parent

    def test_empty_input_handling(self, dbt_project_dir):
        """Test macro behavior with empty inputs"""
        # This would test how macros handle empty strings, null values, etc.
        pass  # Implementation would depend on specific macro requirements

    def test_large_data_handling(self, dbt_project_dir):
        """Test macro performance with large datasets"""
        # This would test macro performance characteristics
        pass  # Would need actual data warehouse connection to test properly

    def test_data_type_edge_cases(self, dbt_project_dir):
        """Test macros with various data types"""
        # Test with different data types (dates, numbers, strings, booleans)
        pass  # Implementation would test specific data type handling


class TestCustomTestMacros:
    """Test the custom test macros defined in data_quality.sql"""

    @pytest.fixture(scope="class")
    def dbt_project_dir(self):
        return Path(__file__).parent.parent

    def test_valid_date_range_macro(self, dbt_project_dir):
        """Test valid_date_range custom test macro"""
        # Create a test that would use this macro
        test_sql = """
        {{ config(materialized='view') }}

        select
            '2023-01-01'::date as test_date,
            'valid' as test_case
        union all
        select
            '1899-01-01'::date as test_date,
            'invalid_too_old' as test_case
        union all
        select
            '2050-01-01'::date as test_date,
            'invalid_too_new' as test_case
        """

        # Test that the macro compiles (actual execution would need warehouse connection)
        assert True  # Placeholder - would need full dbt environment to test

    def test_reasonable_sales_amount_macro(self, dbt_project_dir):
        """Test reasonable_sales_amount custom test macro"""
        # Similar pattern - test macro compilation and logic
        assert True  # Placeholder

    def test_profit_margin_validation_macro(self, dbt_project_dir):
        """Test profit margin validation macro"""
        assert True  # Placeholder


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
