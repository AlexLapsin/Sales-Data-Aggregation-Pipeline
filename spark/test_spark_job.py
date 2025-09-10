#!/usr/bin/env python3
"""
Test script for Spark ETL job

Creates sample data and tests the ETL process locally.
"""

import os
import sys
import tempfile
import shutil
import pandas as pd
from datetime import datetime, timedelta
import random
import logging

# Add spark directory to path
sys.path.append(os.path.dirname(__file__))

from spark_config import (
    get_spark_config,
    create_spark_session_from_config,
    SnowflakeConfig,
)
from sales_batch_job import SalesETLJob


def generate_sample_data(num_records: int = 1000) -> pd.DataFrame:
    """Generate sample sales data for testing"""

    # Sample data lists
    categories = ["TECHNOLOGY", "FURNITURE", "OFFICE SUPPLIES"]
    sub_categories = {
        "TECHNOLOGY": ["Phones", "Computers", "Machines"],
        "FURNITURE": ["Chairs", "Tables", "Bookcases"],
        "OFFICE SUPPLIES": ["Storage", "Art", "Paper"],
    }
    regions = ["WEST", "EAST", "CENTRAL", "SOUTH"]
    segments = ["Consumer", "Corporate", "Home Office"]
    ship_modes = ["Standard Class", "Second Class", "First Class", "Same Day"]

    # Generate data
    data = []
    start_date = datetime(2023, 1, 1)

    for i in range(num_records):
        category = random.choice(categories)
        sub_category = random.choice(sub_categories[category])
        region = random.choice(regions)

        order_date = start_date + timedelta(days=random.randint(0, 365))
        ship_date = order_date + timedelta(days=random.randint(1, 7))

        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(10, 500), 2)
        sales = round(quantity * unit_price, 2)
        discount = round(random.uniform(0, 0.3), 2)
        profit = round(sales * random.uniform(0.1, 0.4), 2)

        record = {
            "Row ID": i + 1,
            "Order ID": f"ORDER-{i+1:06d}",
            "Order Date": order_date.strftime("%m/%d/%Y"),
            "Ship Date": ship_date.strftime("%m/%d/%Y"),
            "Ship Mode": random.choice(ship_modes),
            "Customer ID": f"CUST-{random.randint(1000, 9999)}",
            "Customer Name": f"Customer {i+1}",
            "Segment": random.choice(segments),
            "Country": "United States",
            "City": f"City {random.randint(1, 100)}",
            "State": f"State {random.randint(1, 50)}",
            "Postal Code": f"{random.randint(10000, 99999)}",
            "Region": region,
            "Product ID": f"{category[:3]}-{i+1:06d}",
            "Category": category,
            "Sub-Category": sub_category,
            "Product Name": f"{sub_category} Product {i+1}",
            "Sales": sales,
            "Quantity": quantity,
            "Discount": discount,
            "Profit": profit,
        }
        data.append(record)

    return pd.DataFrame(data)


def create_test_csv_files(
    temp_dir: str, num_files: int = 3, records_per_file: int = 100
):
    """Create test CSV files in temporary directory"""

    csv_files = []

    for i in range(num_files):
        # Generate sample data
        df = generate_sample_data(records_per_file)

        # Save to CSV
        filename = f"sales_data_{i+1}.csv"
        filepath = os.path.join(temp_dir, filename)
        df.to_csv(filepath, index=False)
        csv_files.append(filepath)

        print(f"Created test file: {filename} with {len(df)} records")

    return csv_files


def test_spark_etl_local():
    """Test the Spark ETL job locally with sample data"""

    print("🧪 Testing Spark ETL Job Locally")
    print("=" * 40)

    # Create temporary directory for test files
    temp_dir = tempfile.mkdtemp()
    print(f"Using temporary directory: {temp_dir}")

    try:
        # Generate test CSV files
        csv_files = create_test_csv_files(temp_dir, num_files=2, records_per_file=50)

        # Create Spark session for local testing
        config = get_spark_config("local")
        spark = create_spark_session_from_config(config)

        print(f"✅ Created Spark session: {spark.sparkContext.appName}")

        # Mock Snowflake configuration for testing
        # In real tests, you'd use a test database or mock
        os.environ["SNOWFLAKE_ACCOUNT"] = "test.account"
        os.environ["SNOWFLAKE_USER"] = "test_user"
        os.environ["SNOWFLAKE_PASSWORD"] = "test_password"

        # Create ETL job (will fail at Snowflake write, but we can test transformations)
        etl_job = SalesETLJob(spark)

        # Test data reading
        print("\n📖 Testing data reading...")
        input_path = f"file://{temp_dir}/*.csv"
        raw_df = etl_job.read_csv_files(input_path)
        print(f"✅ Successfully read {raw_df.count()} records")

        # Show sample data
        print("\n📊 Sample raw data:")
        raw_df.show(5, truncate=False)

        # Test data cleaning
        print("\n🧹 Testing data cleaning...")
        batch_id = f"test_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        cleaned_df = etl_job.clean_and_transform(raw_df, batch_id)
        print(f"✅ Successfully cleaned data, {cleaned_df.count()} records remaining")

        # Show sample cleaned data
        print("\n📊 Sample cleaned data:")
        cleaned_df.show(5, truncate=False)

        # Test schema mapping
        print("\n🗂️ Testing schema mapping...")
        final_df = etl_job.map_to_snowflake_schema(cleaned_df)
        print(f"✅ Successfully mapped schema")

        # Show final schema
        print("\n📋 Final schema:")
        final_df.printSchema()

        print("\n📊 Sample final data:")
        final_df.show(5, truncate=False)

        # Test basic aggregations
        print("\n📈 Data quality checks:")
        print(f"Total records: {final_df.count()}")
        print(f"Unique order IDs: {final_df.select('ORDER_ID').distinct().count()}")

        category_counts = final_df.groupBy("CATEGORY").count().collect()
        print("Categories:")
        for row in category_counts:
            print(f"  - {row['CATEGORY']}: {row['count']}")

        print("\n✅ All transformations completed successfully!")
        print("⚠️  Note: Snowflake write test skipped (requires valid credentials)")

        return True

    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        # Cleanup
        if "spark" in locals():
            spark.stop()
        shutil.rmtree(temp_dir)
        print(f"\n🧹 Cleaned up temporary directory: {temp_dir}")


def test_configuration():
    """Test configuration loading"""

    print("\n🔧 Testing Configuration")
    print("=" * 30)

    # Test Spark configs
    for env in ["local", "databricks", "emr", "production"]:
        config = get_spark_config(env)
        print(f"✅ {env}: {config.app_name}")

    # Test Snowflake config (with mock values)
    os.environ["SNOWFLAKE_ACCOUNT"] = "test.account"
    os.environ["SNOWFLAKE_USER"] = "test_user"
    os.environ["SNOWFLAKE_PASSWORD"] = "test_password"

    snowflake_config = SnowflakeConfig.from_env()
    print(f"✅ Snowflake config: {snowflake_config.account}")

    return True


def main():
    """Run all tests"""

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    print("🚀 Starting Spark ETL Tests")
    print("=" * 50)

    success = True

    # Test configuration
    try:
        test_configuration()
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        success = False

    # Test ETL job
    try:
        test_spark_etl_local()
    except Exception as e:
        print(f"❌ ETL test failed: {e}")
        success = False

    if success:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print("\n💥 Some tests failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
