#!/usr/bin/env python3
"""
Check ownership and permissions on TESTS schema
"""
import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv

load_dotenv()

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

print("=== Connecting as ACCOUNTADMIN to check ownership ===\n")
conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    private_key=pkb,
    role="ACCOUNTADMIN",
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
)

cursor = conn.cursor()

print("1. CHECKING TESTS SCHEMA OWNER:")
cursor.execute("SHOW SCHEMAS LIKE 'TESTS' IN DATABASE SALES_DW")
for row in cursor.fetchall():
    print(f"   Schema: {row[1]}, Owner: {row[4]}")

print("\n2. CHECKING ALL GRANTS ON TESTS SCHEMA:")
cursor.execute("SHOW GRANTS ON SCHEMA SALES_DW.TESTS")
grants = cursor.fetchall()
if grants:
    for row in grants:
        print(f"   Granted: {row[1]} | On: {row[2]} | To: {row[3]} | Grantee: {row[4]}")
else:
    print("   No grants found!")

print("\n3. CHECKING GRANTS TO ANALYTICS_ROLE:")
cursor.execute("SHOW GRANTS TO ROLE ANALYTICS_ROLE")
for row in cursor.fetchall():
    if "TESTS" in str(row):
        print(f"   {row}")

print("\n4. TESTING ANALYTICS_ROLE PERMISSIONS:")
try:
    cursor.execute("USE ROLE ANALYTICS_ROLE")
    cursor.execute("USE WAREHOUSE COMPUTE_WH")
    cursor.execute("CREATE TABLE IF NOT EXISTS SALES_DW.TESTS.permission_test (id INT)")
    print("   ✓ SUCCESS: Can create table")
    cursor.execute("DROP TABLE IF EXISTS SALES_DW.TESTS.permission_test")
    print("   ✓ SUCCESS: Can drop table")
except Exception as e:
    print(f"   ✗ FAILED: {e}")

cursor.close()
conn.close()
