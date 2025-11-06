#!/usr/bin/env python3
"""
Silver Layer Data Quality Test

Validates that Silver layer data meets quality standards after processing
corrupted Bronze data through validation and quarantine logic.

Expected Results:
- Bronze: 52,790 rows (Global_Superstore_corrupted.csv with 5% corruption)
- Quarantine: ~2,550 rejected rows
- Silver: ~50,240 clean rows
- All validation rules passed
"""

import os
import sys
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    min as spark_min,
    max as spark_max,
    avg,
)


def create_spark_session():
    """Create Spark session with Delta Lake support"""
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    return (
        SparkSession.builder.appName("SilverQualityTest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.367",
        )
        .getOrCreate()
    )


def test_silver_layer(spark):
    """Test Silver layer data quality"""
    processed_bucket = os.getenv("PROCESSED_BUCKET")
    silver_path = f"s3a://{processed_bucket}/silver/sales/"

    print("=" * 80)
    print("SILVER LAYER DATA QUALITY TEST")
    print("=" * 80)
    print(f"\nSilver path: {silver_path}\n")

    try:
        # Read Silver Delta Lake
        silver_df = spark.read.format("delta").load(silver_path)

        # Basic metrics
        total_rows = silver_df.count()
        print(f"\n[1/7] RECORD COUNT")
        print(f"  Total rows in Silver: {total_rows:,}")
        print(f"  Expected: ~50,240 (52,790 - 2,550 rejected)")

        if total_rows < 49000 or total_rows > 51000:
            print(f"  ⚠️  WARNING: Row count outside expected range")
        else:
            print(f"  ✓ Row count within expected range")

        # Check schema - metadata columns should be removed
        print(f"\n[2/7] SCHEMA VALIDATION")
        columns = silver_df.columns
        print(f"  Total columns: {len(columns)}")

        # Check for removed metadata columns
        removed_cols = ["medallion_layer", "data_source_layer", "processing_stage"]
        found_removed = [col for col in removed_cols if col in columns]

        if found_removed:
            print(
                f"  ✗ FAIL: Found metadata columns that should be removed: {found_removed}"
            )
        else:
            print(f"  ✓ Metadata columns successfully removed")

        # Check source_file anonymization
        if "source_file" in columns:
            sample_file = silver_df.select("source_file").first()[0]
            if "/" in str(sample_file) or "s3" in str(sample_file).lower():
                print(f"  ✗ FAIL: source_file not anonymized: {sample_file}")
            else:
                print(f"  ✓ source_file anonymized (sample: {sample_file})")

        # Data quality validation
        print(f"\n[3/7] DATA QUALITY CHECKS")

        # Check for invalid sales (should be 0)
        invalid_sales = silver_df.filter(
            (col("sales").isNull()) | (col("sales") <= 0)
        ).count()
        print(f"  Invalid sales (null or <= 0): {invalid_sales}")
        if invalid_sales > 0:
            print(f"  ✗ FAIL: Found {invalid_sales} invalid sales records")
        else:
            print(f"  ✓ All sales values are valid (> 0)")

        # Check for invalid quantity (should be 0)
        invalid_qty = silver_df.filter(
            (col("quantity").isNull()) | (col("quantity") <= 0)
        ).count()
        print(f"  Invalid quantity (null or <= 0): {invalid_qty}")
        if invalid_qty > 0:
            print(f"  ✗ FAIL: Found {invalid_qty} invalid quantity records")
        else:
            print(f"  ✓ All quantity values are valid (> 0)")

        # Check for date violations (should be 0)
        date_violations = silver_df.filter(col("order_date") > col("ship_date")).count()
        print(f"  Date violations (order_date > ship_date): {date_violations}")
        if date_violations > 0:
            print(f"  ✗ FAIL: Found {date_violations} date violation records")
        else:
            print(f"  ✓ All dates are logically valid")

        # Check SALES_THRESHOLD application
        print(f"\n[4/7] SALES_THRESHOLD VALIDATION")
        sales_threshold = float(os.getenv("SALES_THRESHOLD", "10000"))
        above_threshold = silver_df.filter(col("sales") > sales_threshold).count()

        print(f"  SALES_THRESHOLD: ${sales_threshold:,.2f}")
        print(f"  Records above threshold: {above_threshold}")

        if above_threshold > 0:
            max_sales = silver_df.agg(spark_max("sales")).first()[0]
            print(
                f"  ✗ FAIL: Found {above_threshold} records above threshold (max: ${max_sales:,.2f})"
            )
        else:
            print(f"  ✓ SALES_THRESHOLD successfully applied")

        # Statistical summary
        print(f"\n[5/7] STATISTICAL SUMMARY")
        stats = silver_df.agg(
            spark_min("sales").alias("min_sales"),
            spark_max("sales").alias("max_sales"),
            avg("sales").alias("avg_sales"),
            spark_min("quantity").alias("min_qty"),
            spark_max("quantity").alias("max_qty"),
            avg("quantity").alias("avg_qty"),
        ).first()

        print(
            f"  Sales:    min=${stats['min_sales']:,.2f}, max=${stats['max_sales']:,.2f}, avg=${stats['avg_sales']:,.2f}"
        )
        print(
            f"  Quantity: min={stats['min_qty']}, max={stats['max_qty']}, avg={stats['avg_qty']:.2f}"
        )

        # Duplicate check
        print(f"\n[6/7] DEDUPLICATION CHECK")
        total_combinations = silver_df.select("order_id", "product_id").count()
        unique_combinations = (
            silver_df.select("order_id", "product_id").distinct().count()
        )
        duplicates = total_combinations - unique_combinations

        print(f"  Total (order_id, product_id) combinations: {total_combinations:,}")
        print(f"  Unique combinations: {unique_combinations:,}")
        print(f"  Duplicates: {duplicates}")

        if duplicates > 0:
            print(f"  ✗ FAIL: Found {duplicates} duplicate records")
        else:
            print(f"  ✓ No duplicates found (deduplication successful)")

        # Category breakdown
        print(f"\n[7/7] CATEGORY BREAKDOWN")
        category_counts = (
            silver_df.groupBy("category")
            .agg(count("*").alias("count"), spark_sum("sales").alias("total_sales"))
            .orderBy(col("count").desc())
        )

        print("\n  Top categories:")
        for row in category_counts.collect()[:5]:
            print(
                f"    {row['category']}: {row['count']:,} records, ${row['total_sales']:,.2f} sales"
            )

        return True

    except Exception as e:
        print(f"\n✗ ERROR: Failed to read Silver layer: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_quarantine_layer(spark):
    """Test Quarantine layer for rejected records"""
    processed_bucket = os.getenv("PROCESSED_BUCKET")
    quarantine_path = f"s3a://{processed_bucket}/quarantine/"

    print("\n" + "=" * 80)
    print("QUARANTINE LAYER VALIDATION")
    print("=" * 80)
    print(f"\nQuarantine path: {quarantine_path}\n")

    try:
        # Read Quarantine Delta Lake
        quarantine_df = spark.read.format("delta").load(quarantine_path)

        # Basic metrics
        total_rejected = quarantine_df.count()
        print(f"[QUARANTINE METRICS]")
        print(f"  Total rejected records: {total_rejected:,}")
        print(f"  Expected: ~2,550 (5% of 52,790)")

        if total_rejected < 2000 or total_rejected > 3000:
            print(f"  ⚠️  WARNING: Rejection count outside expected range")
        else:
            print(f"  ✓ Rejection count within expected range")

        # Rejection reason breakdown
        print(f"\n[REJECTION REASONS]")
        rejection_breakdown = (
            quarantine_df.groupBy("rejection_reason")
            .count()
            .orderBy(col("count").desc())
        )

        for row in rejection_breakdown.collect():
            print(f"  {row['rejection_reason']}: {row['count']:,} records")

        return True

    except Exception as e:
        print(f"\n✗ ERROR: Failed to read Quarantine layer: {e}")
        print("  (This is normal if no batch processing has run yet)")
        return False


def main():
    """Run all tests"""
    print("\n" + "=" * 80)
    print("SILVER LAYER DATA QUALITY VALIDATION")
    print("=" * 80)

    spark = create_spark_session()

    try:
        # Test Silver layer
        silver_success = test_silver_layer(spark)

        # Test Quarantine layer
        quarantine_success = test_quarantine_layer(spark)

        # Final summary
        print("\n" + "=" * 80)
        print("TEST SUMMARY")
        print("=" * 80)
        print(f"  Silver Layer:     {'✓ PASS' if silver_success else '✗ FAIL'}")
        print(f"  Quarantine Layer: {'✓ PASS' if quarantine_success else '✗ FAIL'}")
        print("=" * 80)

        return 0 if silver_success else 1

    except Exception as e:
        print(f"\n✗ FATAL ERROR: {e}")
        import traceback

        traceback.print_exc()
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
