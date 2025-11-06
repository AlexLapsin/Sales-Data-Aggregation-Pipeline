import os
import sys

sys.path.insert(0, "../../")
from dotenv import load_dotenv
import snowflake.connector
from cryptography.hazmat.primitives import serialization

load_dotenv("../../.env")

with open("../../config/keys/rsa_key.p8", "rb") as key_file:
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
)

cursor = conn.cursor()

print("1. CHECKING TESTS SCHEMA OWNER:")
cursor.execute("SHOW SCHEMAS LIKE 'TESTS' IN DATABASE SALES_DW")
for row in cursor.fetchall():
    print(f"   Schema: {row[1]}, Owner: {row[4]}")

print("\n2. CHECKING ALL GRANTS ON TESTS SCHEMA:")
cursor.execute("SHOW GRANTS ON SCHEMA SALES_DW.TESTS")
for row in cursor.fetchall():
    print(f"   {row}")

print("\n3. CHECKING IF ANALYTICS_ROLE CAN CREATE TABLES:")
try:
    cursor.execute("USE ROLE ANALYTICS_ROLE")
    cursor.execute("USE WAREHOUSE COMPUTE_WH")
    cursor.execute("CREATE TABLE IF NOT EXISTS SALES_DW.TESTS.permission_test (id INT)")
    cursor.execute("DROP TABLE IF EXISTS SALES_DW.TESTS.permission_test")
    print("   ✓ SUCCESS: ANALYTICS_ROLE CAN create tables in TESTS schema")
except Exception as e:
    print(f"   ✗ FAILED: ANALYTICS_ROLE CANNOT create tables: {e}")

cursor.close()
conn.close()
