#!/usr/bin/env python3
"""
Script to update test files from password to key-pair authentication.
Replaces SNOWFLAKE_PASSWORD references with SNOWFLAKE_PRIVATE_KEY_PATH and SNOWFLAKE_KEY_PASSPHRASE.
"""

import re
import sys
from pathlib import Path

# Files to update
TEST_FILES = [
    "tests/airflow/test_cloud_sales_pipeline_dag.py",
    "tests/run_spark_tests.py",
    "tests/test_snowflake_connection.py",
    "tests/unit/config/test_config_validation.py",
    "tests/unit/config/test_config_validator.py",
    "tests/unit/spark/test_config.py",
    "tests/unit/spark/test_error_handling.py",
    "tests/unit/spark/test_integration.py",
    "tests/unit/spark/test_performance.py",
    "tests/unit/spark/test_sales_etl_job.py",
    "tests/unit/spark/test_spark_job.py",
]

# Patterns to replace
REPLACEMENTS = [
    # Replace env var dict entries
    (
        r'"SNOWFLAKE_PASSWORD":\s*"[^"]*"',
        '"SNOWFLAKE_PRIVATE_KEY_PATH": "/tmp/test_rsa_key.p8",\n                "SNOWFLAKE_KEY_PASSPHRASE": "test_passphrase"',
    ),
    # Replace os.environ references
    (
        r'os\.environ\["SNOWFLAKE_PASSWORD"\]\s*=\s*"[^"]*"',
        'os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"] = "/tmp/test_rsa_key.p8"\n    os.environ["SNOWFLAKE_KEY_PASSPHRASE"] = "test_passphrase"',
    ),
    # Replace os.getenv references
    (
        r'"SNOWFLAKE_PASSWORD",',
        '"SNOWFLAKE_PRIVATE_KEY_PATH",\n            "SNOWFLAKE_KEY_PASSPHRASE",',
    ),
    # Replace password= in connector.connect
    (
        r'password=os\.getenv\("SNOWFLAKE_PASSWORD"\)',
        "private_key=get_private_key_bytes()  # Use key-pair auth",
    ),
]


def update_file(file_path: Path) -> bool:
    """Update a single test file."""
    if not file_path.exists():
        print(f"[WARN] File not found: {file_path}")
        return False

    try:
        content = file_path.read_text(encoding="utf-8")
        original_content = content

        # Apply all replacements
        for pattern, replacement in REPLACEMENTS:
            content = re.sub(pattern, replacement, content)

        if content != original_content:
            file_path.write_text(content, encoding="utf-8")
            print(f"[OK] Updated: {file_path}")
            return True
        else:
            print(f"[SKIP] No changes needed: {file_path}")
            return False

    except Exception as e:
        print(f"[ERROR] Error updating {file_path}: {e}")
        return False


def main():
    """Update all test files."""
    print("=" * 80)
    print("Updating Test Files: Password to Key-Pair Authentication")
    print("=" * 80)

    project_root = Path(__file__).parent.parent
    updated_count = 0

    for file_rel_path in TEST_FILES:
        file_path = project_root / file_rel_path
        if update_file(file_path):
            updated_count += 1

    print("\n" + "=" * 80)
    print(f"[DONE] Updated {updated_count}/{len(TEST_FILES)} files")
    print("=" * 80)

    return 0 if updated_count > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
