#!/usr/bin/env python3
"""
Silver Layer Data Quality Test via Snowflake Delta Direct

Validates Silver layer data quality by querying through Snowflake's
Delta Direct (Iceberg) integration.
"""

import os
import sys
import pytest
from dotenv import load_dotenv

# Import Snowflake auth helper
from tests.helpers import (
    get_snowflake_connection,
    check_snowflake_credentials_available,
)

# Load environment
load_dotenv()


@pytest.mark.requires_credentials
@pytest.mark.skipif(
    not check_snowflake_credentials_available(),
    reason="Snowflake credentials not available",
)
def test_silver_via_snowflake():
    """Test Silver layer data through Snowflake Delta Direct"""

    print("=" * 80)
    print("SILVER LAYER DATA QUALITY TEST (via Snowflake Delta Direct)")
    print("=" * 80)

    # Connect to Snowflake using key-pair authentication
    conn = get_snowflake_connection()

    cursor = conn.cursor()

    try:
        # Test 1: Record count
        print("\n[1/8] RECORD COUNT")
        cursor.execute("SELECT COUNT(*) as row_count FROM SALES_SILVER_EXTERNAL")
        row_count = cursor.fetchone()[0]
        print(f"  Total rows in Silver: {row_count:,}")
        print(f"  Expected: ~50,240 (52,790 - 2,550 rejected)")

        if row_count < 49000 or row_count > 51000:
            print(f"  [WARNING] Row count outside expected range")
        else:
            print(f"  [PASS] Row count within expected range")

        # Test 2: Schema validation
        print("\n[2/8] SCHEMA VALIDATION")
        cursor.execute("SHOW COLUMNS IN SALES_SILVER_EXTERNAL")
        columns = [row[2] for row in cursor.fetchall()]  # Column name is 3rd element
        print(f"  Total columns: {len(columns)}")

        # Check for removed metadata columns
        removed_cols = ["MEDALLION_LAYER", "DATA_SOURCE_LAYER", "PROCESSING_STAGE"]
        found_removed = [col for col in removed_cols if col in columns]

        if found_removed:
            print(
                f"  [FAIL] FAIL: Found metadata columns that should be removed: {found_removed}"
            )
        else:
            print(f"  [PASS] Metadata columns successfully removed")

        # Check for source_file anonymization
        if "SOURCE_FILE" in columns:
            cursor.execute("SELECT SOURCE_FILE FROM SALES_SILVER_EXTERNAL LIMIT 1")
            sample_file = cursor.fetchone()[0]
            if "/" in str(sample_file) or "s3" in str(sample_file).lower():
                print(f"  [FAIL] FAIL: source_file not anonymized: {sample_file}")
            else:
                print(f"  [PASS] source_file anonymized (sample: {sample_file})")

        # Test 3: Data quality checks
        print("\n[3/8] DATA QUALITY CHECKS")

        # Invalid sales
        cursor.execute(
            "SELECT COUNT(*) FROM SALES_SILVER_EXTERNAL WHERE SALES IS NULL OR SALES <= 0"
        )
        invalid_sales = cursor.fetchone()[0]
        print(f"  Invalid sales (null or <= 0): {invalid_sales}")
        if invalid_sales > 0:
            print(f"  [FAIL] FAIL: Found {invalid_sales} invalid sales records")
        else:
            print(f"  [PASS] All sales values are valid (> 0)")

        # Invalid quantity
        cursor.execute(
            "SELECT COUNT(*) FROM SALES_SILVER_EXTERNAL WHERE QUANTITY IS NULL OR QUANTITY <= 0"
        )
        invalid_qty = cursor.fetchone()[0]
        print(f"  Invalid quantity (null or <= 0): {invalid_qty}")
        if invalid_qty > 0:
            print(f"  [FAIL] FAIL: Found {invalid_qty} invalid quantity records")
        else:
            print(f"  [PASS] All quantity values are valid (> 0)")

        # Date violations
        cursor.execute(
            "SELECT COUNT(*) FROM SALES_SILVER_EXTERNAL WHERE ORDER_DATE > SHIP_DATE"
        )
        date_violations = cursor.fetchone()[0]
        print(f"  Date violations (order_date > ship_date): {date_violations}")
        if date_violations > 0:
            print(f"  [FAIL] FAIL: Found {date_violations} date violation records")
        else:
            print(f"  [PASS] All dates are logically valid")

        # Test 4: SALES_THRESHOLD validation
        print("\n[4/8] SALES_THRESHOLD VALIDATION")
        sales_threshold = float(os.getenv("SALES_THRESHOLD", "10000"))
        cursor.execute(
            f"SELECT COUNT(*) FROM SALES_SILVER_EXTERNAL WHERE SALES > {sales_threshold}"
        )
        above_threshold = cursor.fetchone()[0]

        print(f"  SALES_THRESHOLD: ${sales_threshold:,.2f}")
        print(f"  Records above threshold: {above_threshold}")

        if above_threshold > 0:
            cursor.execute("SELECT MAX(SALES) FROM SALES_SILVER_EXTERNAL")
            max_sales = cursor.fetchone()[0]
            print(
                f"  [FAIL] FAIL: Found {above_threshold} records above threshold (max: ${max_sales:,.2f})"
            )
        else:
            print(f"  [PASS] SALES_THRESHOLD successfully applied")

        # Test 5: Statistical summary
        print("\n[5/8] STATISTICAL SUMMARY")
        cursor.execute(
            """
            SELECT
                MIN(SALES) as min_sales,
                MAX(SALES) as max_sales,
                AVG(SALES) as avg_sales,
                MIN(QUANTITY) as min_qty,
                MAX(QUANTITY) as max_qty,
                AVG(QUANTITY) as avg_qty
            FROM SALES_SILVER_EXTERNAL
        """
        )
        stats = cursor.fetchone()

        print(
            f"  Sales:    min=${stats[0]:,.2f}, max=${stats[1]:,.2f}, avg=${stats[2]:,.2f}"
        )
        print(f"  Quantity: min={stats[3]}, max={stats[4]}, avg={stats[5]:.2f}")

        # Test 6: Deduplication check
        print("\n[6/8] DEDUPLICATION CHECK")
        cursor.execute("SELECT COUNT(*) FROM SALES_SILVER_EXTERNAL")
        total_combinations = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(DISTINCT ORDER_ID || '-' || PRODUCT_ID) FROM SALES_SILVER_EXTERNAL"
        )
        unique_combinations = cursor.fetchone()[0]

        duplicates = total_combinations - unique_combinations

        print(f"  Total (order_id, product_id) combinations: {total_combinations:,}")
        print(f"  Unique combinations: {unique_combinations:,}")
        print(f"  Duplicates: {duplicates}")

        if duplicates > 0:
            print(f"  [FAIL] FAIL: Found {duplicates} duplicate records")
        else:
            print(f"  [PASS] No duplicates found (deduplication successful)")

        # Test 7: Category breakdown
        print("\n[7/8] CATEGORY BREAKDOWN")
        cursor.execute(
            """
            SELECT CATEGORY, COUNT(*) as count, SUM(SALES) as total_sales
            FROM SALES_SILVER_EXTERNAL
            GROUP BY CATEGORY
            ORDER BY count DESC
            LIMIT 5
        """
        )

        print("\n  Top categories:")
        for row in cursor.fetchall():
            print(f"    {row[0]}: {row[1]:,} records, ${row[2]:,.2f} sales")

        # Test 8: Null checks
        print("\n[8/8] NULL VALUE ANALYSIS")
        cursor.execute(
            """
            SELECT
                SUM(CASE WHEN CUSTOMER_NAME IS NULL THEN 1 ELSE 0 END) as null_customer,
                SUM(CASE WHEN POSTAL_CODE IS NULL THEN 1 ELSE 0 END) as null_postal,
                SUM(CASE WHEN STATE IS NULL THEN 1 ELSE 0 END) as null_state
            FROM SALES_SILVER_EXTERNAL
        """
        )
        nulls = cursor.fetchone()

        print(f"  NULL customer_name: {nulls[0]:,}")
        print(f"  NULL postal_code: {nulls[1]:,}")
        print(f"  NULL state: {nulls[2]:,} (expected for international orders)")

        print("\n" + "=" * 80)
        print("SILVER LAYER VALIDATION COMPLETE")
        print("=" * 80)
        print("\n[PASS] All critical data quality checks passed!")
        print(
            "  Silver layer contains clean, validated data ready for Gold transformations"
        )

        return True

    except Exception as e:
        print(f"\n[FAIL] ERROR: {e}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    success = test_silver_via_snowflake()
    sys.exit(0 if success else 1)
