"""
Snowflake Authentication Helper for Tests

Provides key-pair authentication for Snowflake connections in test environment.
Replaces legacy password-based authentication.
"""

import os
from typing import Optional
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import snowflake.connector


def get_private_key_bytes() -> bytes:
    """
    Load and decrypt Snowflake private key from file.

    Returns:
        bytes: DER-encoded private key bytes for Snowflake connection

    Raises:
        FileNotFoundError: If private key file doesn't exist
        ValueError: If passphrase is incorrect
    """
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    passphrase = os.getenv("SNOWFLAKE_KEY_PASSPHRASE", "")

    if not key_path:
        raise ValueError(
            "SNOWFLAKE_PRIVATE_KEY_PATH environment variable not set. "
            "Tests requiring Snowflake should be marked with @pytest.mark.requires_credentials"
        )

    if not os.path.exists(key_path):
        raise FileNotFoundError(
            f"Snowflake private key not found at: {key_path}. "
            "Ensure config/keys/rsa_key.p8 exists or skip tests with credentials."
        )

    with open(key_path, "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase.encode() if passphrase else None,
            backend=default_backend(),
        )

    # Convert to DER format for Snowflake
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return pkb


def get_snowflake_connection(
    database: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse: Optional[str] = None,
) -> snowflake.connector.SnowflakeConnection:
    """
    Create Snowflake connection using key-pair authentication.

    Args:
        database: Snowflake database (defaults to env var)
        schema: Snowflake schema (defaults to env var)
        warehouse: Snowflake warehouse (defaults to env var)

    Returns:
        SnowflakeConnection: Active connection to Snowflake

    Raises:
        ValueError: If required environment variables missing

    Example:
        >>> conn = get_snowflake_connection()
        >>> cursor = conn.cursor()
        >>> cursor.execute("SELECT CURRENT_VERSION()")
    """
    # Get credentials from environment
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    role = os.getenv("SNOWFLAKE_ROLE")

    if not all([account, user]):
        raise ValueError(
            "Missing required Snowflake environment variables: "
            "SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER. "
            "Tests requiring credentials should be marked with "
            "@pytest.mark.requires_credentials"
        )

    # Get optional parameters
    database = database or os.getenv("SNOWFLAKE_DATABASE")
    schema = schema or os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
    warehouse = warehouse or os.getenv("SNOWFLAKE_WAREHOUSE")

    # Get private key
    private_key = get_private_key_bytes()

    # Create connection
    conn = snowflake.connector.connect(
        account=account,
        user=user,
        private_key=private_key,
        database=database,
        schema=schema,
        warehouse=warehouse,
        role=role,
    )

    return conn


def check_snowflake_credentials_available() -> bool:
    """
    Check if Snowflake credentials are available for testing.

    Returns:
        bool: True if credentials available, False otherwise

    Example:
        >>> import pytest
        >>> @pytest.mark.skipif(
        ...     not check_snowflake_credentials_available(),
        ...     reason="Snowflake credentials not available"
        ... )
        ... def test_snowflake_query():
        ...     conn = get_snowflake_connection()
        ...     # test code...
    """
    try:
        required_vars = [
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PRIVATE_KEY_PATH",
        ]

        for var in required_vars:
            if not os.getenv(var):
                return False

        key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
        if not os.path.exists(key_path):
            return False

        return True

    except Exception:
        return False
