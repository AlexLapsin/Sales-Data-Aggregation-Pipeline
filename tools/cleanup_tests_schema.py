#!/usr/bin/env python3
"""
Drop all tables in TESTS schema to fix permission issues.
Old tables were created by ACCOUNTADMIN, need to drop and recreate with ANALYTICS_ROLE.
"""
import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Load private key for keypair authentication
with open("config/keys/rsa_key.p8", "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=os.getenv("SNOWFLAKE_KEY_PASSPHRASE").encode(),
        backend=None,
    )

pkb = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

# Connect using ACCOUNTADMIN with keypair auth
conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    private_key=pkb,
    role="ACCOUNTADMIN",  # Use ACCOUNTADMIN to drop old tables
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema="TESTS",
)

cursor = conn.cursor()

try:
    # Get all tables in TESTS schema
    cursor.execute("SHOW TABLES IN SCHEMA SALES_DW.TESTS")
    tables = cursor.fetchall()

    print(f"Found {len(tables)} tables in SALES_DW.TESTS schema")

    # Drop each table
    for table in tables:
        table_name = table[1]  # Table name is in column 1
        print(f"Dropping table: {table_name}")
        cursor.execute(f"DROP TABLE IF EXISTS SALES_DW.TESTS.{table_name}")

    print("\nAll tables dropped successfully!")
    print("ANALYTICS_ROLE can now create new test tables with proper ownership.")

finally:
    cursor.close()
    conn.close()
