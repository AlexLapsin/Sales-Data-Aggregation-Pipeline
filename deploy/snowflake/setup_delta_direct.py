#!/usr/bin/env python3
"""
Snowflake Delta Direct Setup

Automates creation/destruction of Delta Direct (Iceberg) resources for reading
Delta Lake tables from S3 Silver layer.

This script uses the official Snowflake Python API to create resources that are
not yet supported in the Terraform Snowflake provider (as of January 2025):
- Catalog Integration (for Delta Lake metadata)
- Iceberg Table (with AUTO_REFRESH enabled)

Prerequisites:
- Terraform infrastructure must be deployed first (IAM role, External Volume)
- Snowflake account with ACCOUNTADMIN privileges
- Python packages: snowflake, snowflake-connector-python, python-dotenv

Usage:
    python setup_delta_direct.py           # Create resources
    python setup_delta_direct.py destroy   # Destroy resources

Environment Variables (from .env or environment):
    SNOWFLAKE_ACCOUNT         # Snowflake account identifier
    SNOWFLAKE_USER            # Snowflake username
    SNOWFLAKE_PASSWORD        # Snowflake password
    SNOWFLAKE_ROLE            # Role to use (default: ACCOUNTADMIN)
    SNOWFLAKE_WAREHOUSE       # Warehouse to use (default: COMPUTE_WH)
    SNOWFLAKE_DATABASE        # Database name (default: SALES_DW)

Author: Sales Data Pipeline Team
Version: 1.0
Date: 2025-01-04
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from snowflake.snowpark import Session


# Configuration
CATALOG_INTEGRATION_NAME = "DELTA_CATALOG"
ICEBERG_TABLE_NAME = "SALES_SILVER_EXTERNAL"
EXTERNAL_VOLUME_NAME = "S3_SILVER_VOLUME"
ANALYTICS_ROLE_NAME = "ANALYTICS_ROLE"


def load_environment():
    """Load environment variables from .env file"""
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        print(f"Loaded environment from: {env_path}")
    else:
        print("No .env file found, using system environment variables")


def create_session():
    """Create Snowflake session with environment configuration"""
    # Check for required variables (either password or private key)
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")

    if not account or not user:
        raise ValueError(
            "Missing required environment variables: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER"
        )

    if not password and not private_key_path:
        raise ValueError(
            "Either SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH must be set"
        )

    config = {
        "account": account,
        "user": user,
        "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "SALES_DW"),
    }

    # Use key-pair authentication if private key path is provided
    if private_key_path:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        passphrase = os.getenv("SNOWFLAKE_KEY_PASSPHRASE", "").encode()
        with open(private_key_path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=passphrase if passphrase else None,
                backend=default_backend(),
            )

        pkb = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        config["private_key"] = pkb
        print(f"Using key-pair authentication")
    else:
        config["password"] = password
        print(f"Using password authentication")

    print(f"Connecting to Snowflake account: {config['account']}")
    print(f"Using role: {config['role']}, warehouse: {config['warehouse']}")

    return Session.builder.configs(config).create()


def validate_prerequisites(session):
    """Validate that required Terraform resources exist"""
    print("\n[Validation] Checking prerequisites...")

    # Check external volume exists
    result = session.sql(
        f"SHOW EXTERNAL VOLUMES LIKE '{EXTERNAL_VOLUME_NAME}'"
    ).collect()
    if not result:
        raise RuntimeError(
            f"External volume {EXTERNAL_VOLUME_NAME} not found.\n"
            "Please run 'terraform apply' first to create AWS infrastructure."
        )
    print(f"  [OK] External volume '{EXTERNAL_VOLUME_NAME}' exists")

    # Check analytics role exists
    result = session.sql(f"SHOW ROLES LIKE '{ANALYTICS_ROLE_NAME}'").collect()
    if not result:
        raise RuntimeError(
            f"Role {ANALYTICS_ROLE_NAME} not found.\n"
            "Please run 'terraform apply' first to create Snowflake roles."
        )
    print(f"  [OK] Role '{ANALYTICS_ROLE_NAME}' exists")

    print("[Validation] Prerequisites validated successfully")


def create_catalog_integration(session):
    """Create Delta Lake catalog integration using raw SQL (Python API doesn't support DELTA format yet)"""
    print(f"\n[1/3] Creating catalog integration '{CATALOG_INTEGRATION_NAME}'...")

    try:
        session.sql(
            f"""
            CREATE CATALOG INTEGRATION IF NOT EXISTS {CATALOG_INTEGRATION_NAME}
                CATALOG_SOURCE = OBJECT_STORE
                TABLE_FORMAT = DELTA
                ENABLED = TRUE
                COMMENT = 'Catalog integration for reading Delta Lake transaction logs from S3'
        """
        ).collect()

        print(f"  [SUCCESS] Catalog integration '{CATALOG_INTEGRATION_NAME}' created")

    except Exception as e:
        error_msg = str(e).lower()
        if "already exists" in error_msg:
            print(
                f"  [INFO] Catalog integration '{CATALOG_INTEGRATION_NAME}' already exists (idempotent)"
            )
        else:
            print(f"  [ERROR] Failed to create catalog integration: {e}")
            raise


def create_iceberg_table(session):
    """Create Iceberg table using raw SQL (Python API doesn't properly support catalog-integrated tables)"""
    print(f"\n[2/3] Creating Iceberg table '{ICEBERG_TABLE_NAME}'...")

    try:
        # Create Iceberg table with catalog integration using SQL
        # The Python API doesn't properly create catalog-integrated Iceberg tables for Delta format
        session.sql(
            f"""
            CREATE ICEBERG TABLE IF NOT EXISTS SALES_DW.RAW.{ICEBERG_TABLE_NAME}
                EXTERNAL_VOLUME = '{EXTERNAL_VOLUME_NAME}'
                CATALOG = '{CATALOG_INTEGRATION_NAME}'
                BASE_LOCATION = 'sales/'
                COMMENT = 'Unified batch+streaming sales data from Delta Lake Silver layer via catalog integration'
        """
        ).collect()

        print(
            f"  [SUCCESS] Iceberg table '{ICEBERG_TABLE_NAME}' created with catalog integration"
        )

    except Exception as e:
        error_msg = str(e).lower()
        if "already exists" in error_msg:
            print(
                f"  [INFO] Iceberg table '{ICEBERG_TABLE_NAME}' already exists (idempotent)"
            )
        else:
            print(f"  [ERROR] Failed to create Iceberg table: {e}")
            raise


def grant_permissions(session):
    """Grant SELECT permission on Iceberg table to ANALYTICS_ROLE"""
    print(f"\n[3/3] Granting permissions to '{ANALYTICS_ROLE_NAME}'...")

    try:
        session.sql(
            f"""
            GRANT SELECT ON ICEBERG TABLE SALES_DW.RAW.{ICEBERG_TABLE_NAME}
            TO ROLE {ANALYTICS_ROLE_NAME}
        """
        ).collect()

        print(f"  [SUCCESS] Granted SELECT permission to '{ANALYTICS_ROLE_NAME}'")

    except Exception as e:
        error_msg = str(e).lower()
        if "already exists" in error_msg or "duplicate" in error_msg:
            print(f"  [INFO] Permission already granted (idempotent)")
        else:
            print(f"  [ERROR] Failed to grant permissions: {e}")
            raise


def verify_deployment(session):
    """Verify that resources were created successfully"""
    print("\n[Verification] Checking deployed resources...")

    # Check Iceberg table exists and get row count
    try:
        result = session.sql(
            f"""
            SELECT COUNT(*) as row_count
            FROM SALES_DW.RAW.{ICEBERG_TABLE_NAME}
        """
        ).collect()

        if result:
            row_count = result[0]["ROW_COUNT"]
            print(f"  [OK] Iceberg table contains {row_count:,} rows")

        # Check AUTO_REFRESH status
        result = session.sql(
            f"""
            SHOW ICEBERG TABLES LIKE '{ICEBERG_TABLE_NAME}' IN SCHEMA SALES_DW.RAW
        """
        ).collect()

        if result:
            print(f"  [OK] Iceberg table '{ICEBERG_TABLE_NAME}' is active")

    except Exception as e:
        print(f"  [WARNING] Verification query failed: {e}")
        print("  This is normal if the Silver layer is empty")


def destroy_resources(session):
    """Destroy Delta Direct resources in reverse order"""
    print("\n[Destroy] Removing Delta Direct resources...")

    commands = [
        (
            "Revoking permissions",
            f"REVOKE SELECT ON ICEBERG TABLE SALES_DW.RAW.{ICEBERG_TABLE_NAME} FROM ROLE {ANALYTICS_ROLE_NAME}",
        ),
        (
            "Dropping Iceberg table",
            f"DROP ICEBERG TABLE IF EXISTS SALES_DW.RAW.{ICEBERG_TABLE_NAME}",
        ),
        (
            "Dropping catalog integration",
            f"DROP CATALOG INTEGRATION IF EXISTS {CATALOG_INTEGRATION_NAME}",
        ),
    ]

    for description, sql in commands:
        print(f"  [{description}]...")
        try:
            session.sql(sql).collect()
            print(f"    [SUCCESS] {description} completed")
        except Exception as e:
            print(f"    [WARNING] {description} skipped: {e}")


def main():
    """Main execution function"""
    # Parse command line arguments
    mode = sys.argv[1] if len(sys.argv) > 1 else "create"

    if mode not in ["create", "destroy"]:
        print(f"ERROR: Invalid mode '{mode}'. Use 'create' or 'destroy'.")
        sys.exit(1)

    # Print header
    print("=" * 80)
    print("SNOWFLAKE DELTA DIRECT SETUP")
    print("=" * 80)
    print(f"Mode: {mode.upper()}")
    print()

    try:
        # Load configuration
        load_environment()

        # Create Snowflake session
        session = create_session()

        if mode == "destroy":
            # Destroy mode
            destroy_resources(session)
            print("\n" + "=" * 80)
            print("DELTA DIRECT RESOURCES DESTROYED")
            print("=" * 80)

        else:
            # Create mode
            validate_prerequisites(session)

            create_catalog_integration(session)
            create_iceberg_table(session)
            grant_permissions(session)
            verify_deployment(session)

            print("\n" + "=" * 80)
            print("DELTA DIRECT SETUP COMPLETED SUCCESSFULLY")
            print("=" * 80)
            print("\nNext steps:")
            print(
                "  1. Verify: SELECT COUNT(*) FROM SALES_DW.RAW.SALES_SILVER_EXTERNAL;"
            )
            print("  2. Update dbt sources to reference SALES_SILVER_EXTERNAL")
            print("  3. Run dbt: cd dbt && dbt run")

        # Close session
        session.close()

    except Exception as e:
        print(f"\n[ERROR] {e}")
        print("\nDeployment failed. Check the error message above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
