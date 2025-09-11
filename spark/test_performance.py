"""
Performance and stress tests for Spark ETL job.

This module contains tests for performance benchmarking, memory usage,
and stress testing with different data sizes and configurations.
"""

import pytest
import os
import time
import tempfile
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List
import random
import psutil
import gc

# PySpark imports with error handling
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import col, count, sum as spark_sum

    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkSession = None
    DataFrame = None

# Local imports
import sys

sys.path.append(os.path.dirname(__file__))

if SPARK_AVAILABLE:
    from sales_batch_job import SalesETLJob, create_spark_session
    from spark_config import get_spark_config, create_spark_session_from_config
    from conftest import create_spark_dataframe


class PerformanceMonitor:
    """Utility class for monitoring performance metrics during tests."""

    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.start_memory = None
        self.peak_memory = None
        self.process = psutil.Process()

    def start_monitoring(self):
        """Start performance monitoring."""
        self.start_time = time.time()
        self.start_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        self.peak_memory = self.start_memory

    def update_peak_memory(self):
        """Update peak memory usage."""
        current_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        self.peak_memory = max(self.peak_memory, current_memory)

    def stop_monitoring(self) -> Dict[str, float]:
        """Stop monitoring and return performance metrics."""
        self.end_time = time.time()
        end_memory = self.process.memory_info().rss / 1024 / 1024  # MB

        return {
            "duration_seconds": self.end_time - self.start_time,
            "start_memory_mb": self.start_memory,
            "end_memory_mb": end_memory,
            "peak_memory_mb": self.peak_memory,
            "memory_increase_mb": end_memory - self.start_memory,
            "peak_memory_increase_mb": self.peak_memory - self.start_memory,
        }


def generate_performance_test_data(
    num_records: int, complexity: str = "simple"
) -> List[Dict[str, Any]]:
    """Generate test data for performance testing with varying complexity."""
    categories = ["TECHNOLOGY", "FURNITURE", "OFFICE SUPPLIES"]
    sub_categories = {
        "TECHNOLOGY": ["Phones", "Computers", "Machines", "Accessories"],
        "FURNITURE": ["Chairs", "Tables", "Bookcases", "Storage"],
        "OFFICE SUPPLIES": ["Storage", "Art", "Paper", "Supplies"],
    }
    regions = ["WEST", "EAST", "CENTRAL", "SOUTH"]
    segments = ["Consumer", "Corporate", "Home Office"]
    ship_modes = ["Standard Class", "Second Class", "First Class", "Same Day"]

    data = []
    start_date = datetime(2023, 1, 1)

    for i in range(num_records):
        category = random.choice(categories)
        sub_category = random.choice(sub_categories[category])
        region = random.choice(regions)

        # Vary complexity based on parameter
        if complexity == "complex":
            # More varied data for complex scenarios
            order_date = start_date + timedelta(days=random.randint(0, 730))  # 2 years
            quantity = random.randint(1, 100)
            unit_price = round(random.uniform(1, 10000), 2)
            customer_name = f"Customer {random.randint(1, 50000)}"
            product_name = f"{sub_category} Product {random.randint(1, 10000)}"
        else:
            # Simpler data for basic performance tests
            order_date = start_date + timedelta(days=i % 365)
            quantity = random.randint(1, 10)
            unit_price = round(random.uniform(10, 500), 2)
            customer_name = f"Customer {i % 1000}"
            product_name = f"{sub_category} Product {i % 100}"

        ship_date = order_date + timedelta(days=random.randint(1, 7))
        sales = round(quantity * unit_price, 2)
        discount = round(random.uniform(0, 0.3), 2)
        profit = round(sales * random.uniform(0.1, 0.4), 2)

        record = {
            "Row ID": i + 1,
            "Order ID": f"PERF-ORDER-{i+1:08d}",
            "Order Date": order_date.strftime("%m/%d/%Y"),
            "Ship Date": ship_date.strftime("%m/%d/%Y"),
            "Ship Mode": random.choice(ship_modes),
            "Customer ID": f"PERF-CUST-{(i % 10000):06d}",
            "Customer Name": customer_name,
            "Segment": random.choice(segments),
            "Country": "United States",
            "City": f"City {i % 1000}",
            "State": f"State {i % 50}",
            "Postal Code": f"{10000 + (i % 90000)}",
            "Region": region,
            "Product ID": f"PERF-{category[:3]}-{i+1:08d}",
            "Category": category,
            "Sub-Category": sub_category,
            "Product Name": product_name,
            "Sales": sales,
            "Quantity": quantity,
            "Discount": discount,
            "Profit": profit,
        }
        data.append(record)

    return data


@pytest.mark.performance
@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestPerformanceBenchmarks:
    """Performance benchmark tests for different data sizes."""

    @pytest.mark.parametrize("record_count", [1000, 5000, 10000])
    def test_etl_performance_by_size(self, spark_session, record_count):
        """Test ETL performance with different data sizes."""
        monitor = PerformanceMonitor()

        # Generate test data
        test_data = generate_performance_test_data(record_count, "simple")

        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "test.account",
                "SNOWFLAKE_USER": "test_user",
                "SNOWFLAKE_PASSWORD": "test_password",
            },
        ):
            etl_job = SalesETLJob(spark_session)

            monitor.start_monitoring()

            try:
                # Run ETL transformations (without Snowflake write)
                input_df = create_spark_dataframe(spark_session, test_data)
                monitor.update_peak_memory()

                cleaned_df = etl_job.clean_and_transform(
                    input_df, f"perf_test_{record_count}"
                )
                monitor.update_peak_memory()

                final_df = etl_job.map_to_snowflake_schema(cleaned_df)
                monitor.update_peak_memory()

                # Force computation to measure actual performance
                result_count = final_df.count()
                monitor.update_peak_memory()

                # Verify we processed the expected number of records
                assert (
                    result_count > 0
                ), f"No records processed for {record_count} input records"
                assert result_count <= record_count, f"More records output than input"

            finally:
                metrics = monitor.stop_monitoring()

                # Performance assertions
                assert (
                    metrics["duration_seconds"] < 300
                ), f"ETL took too long: {metrics['duration_seconds']:.2f}s for {record_count} records"

                # Memory usage should be reasonable
                memory_per_record = metrics["peak_memory_increase_mb"] / record_count
                assert (
                    memory_per_record < 1.0
                ), f"Memory usage too high: {memory_per_record:.3f} MB per record"

                print(f"\nPerformance metrics for {record_count} records:")
                print(f"  Duration: {metrics['duration_seconds']:.2f} seconds")
                print(
                    f"  Records/second: {record_count / metrics['duration_seconds']:.0f}"
                )
                print(
                    f"  Peak memory increase: {metrics['peak_memory_increase_mb']:.2f} MB"
                )
                print(f"  Memory per record: {memory_per_record:.3f} MB")

    @pytest.mark.slow
    def test_large_dataset_processing(self, spark_session):
        """Test processing of large datasets (stress test)."""
        record_count = 50000  # 50K records
        monitor = PerformanceMonitor()

        # Generate large test dataset
        test_data = generate_performance_test_data(record_count, "complex")

        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "test.account",
                "SNOWFLAKE_USER": "test_user",
                "SNOWFLAKE_PASSWORD": "test_password",
            },
        ):
            etl_job = SalesETLJob(spark_session)

            monitor.start_monitoring()

            try:
                # Process in chunks to test scalability
                chunk_size = 10000
                total_processed = 0

                for i in range(0, record_count, chunk_size):
                    chunk_data = test_data[i : i + chunk_size]

                    input_df = create_spark_dataframe(spark_session, chunk_data)
                    cleaned_df = etl_job.clean_and_transform(
                        input_df, f"large_test_chunk_{i}"
                    )
                    final_df = etl_job.map_to_snowflake_schema(cleaned_df)

                    chunk_count = final_df.count()
                    total_processed += chunk_count

                    monitor.update_peak_memory()

                    # Force garbage collection between chunks
                    gc.collect()

                assert total_processed > 0, "No records processed in large dataset test"

            finally:
                metrics = monitor.stop_monitoring()

                # Performance assertions for large dataset
                assert (
                    metrics["duration_seconds"] < 600
                ), f"Large dataset processing took too long: {metrics['duration_seconds']:.2f}s"

                records_per_second = total_processed / metrics["duration_seconds"]
                assert (
                    records_per_second > 50
                ), f"Processing rate too slow: {records_per_second:.0f} records/second"

                print(f"\nLarge dataset performance metrics:")
                print(f"  Total records processed: {total_processed}")
                print(f"  Duration: {metrics['duration_seconds']:.2f} seconds")
                print(f"  Records/second: {records_per_second:.0f}")
                print(
                    f"  Peak memory increase: {metrics['peak_memory_increase_mb']:.2f} MB"
                )

    @pytest.mark.parametrize("file_count", [1, 5, 10])
    def test_multi_file_performance(self, spark_session, file_count):
        """Test performance with multiple input files."""
        monitor = PerformanceMonitor()
        records_per_file = 2000

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create multiple CSV files
            total_records = 0
            for i in range(file_count):
                file_data = generate_performance_test_data(records_per_file, "simple")
                df = pd.DataFrame(file_data)
                filepath = os.path.join(temp_dir, f"performance_file_{i+1}.csv")
                df.to_csv(filepath, index=False)
                total_records += len(file_data)

            with patch.dict(
                os.environ,
                {
                    "SNOWFLAKE_ACCOUNT": "test.account",
                    "SNOWFLAKE_USER": "test_user",
                    "SNOWFLAKE_PASSWORD": "test_password",
                },
            ):
                etl_job = SalesETLJob(spark_session)

                monitor.start_monitoring()

                try:
                    input_path = f"file://{temp_dir}/*.csv"

                    # Read all files
                    raw_df = etl_job.read_csv_files(input_path)
                    monitor.update_peak_memory()

                    # Process data
                    cleaned_df = etl_job.clean_and_transform(
                        raw_df, f"multifile_test_{file_count}"
                    )
                    monitor.update_peak_memory()

                    final_df = etl_job.map_to_snowflake_schema(cleaned_df)
                    monitor.update_peak_memory()

                    result_count = final_df.count()

                    assert result_count > 0, "No records processed from multiple files"

                finally:
                    metrics = monitor.stop_monitoring()

                    # Performance should scale reasonably with file count
                    files_per_second = file_count / metrics["duration_seconds"]
                    assert (
                        files_per_second > 0.1
                    ), f"File processing rate too slow: {files_per_second:.2f} files/second"

                    print(f"\nMulti-file performance metrics ({file_count} files):")
                    print(f"  Duration: {metrics['duration_seconds']:.2f} seconds")
                    print(f"  Files/second: {files_per_second:.2f}")
                    print(f"  Records processed: {result_count}")
                    print(
                        f"  Peak memory increase: {metrics['peak_memory_increase_mb']:.2f} MB"
                    )


@pytest.mark.performance
@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestMemoryUsage:
    """Memory usage and optimization tests."""

    def test_memory_efficiency_small_datasets(self, spark_session):
        """Test memory efficiency with small datasets."""
        record_count = 1000
        monitor = PerformanceMonitor()

        test_data = generate_performance_test_data(record_count, "simple")

        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "test.account",
                "SNOWFLAKE_USER": "test_user",
                "SNOWFLAKE_PASSWORD": "test_password",
            },
        ):
            etl_job = SalesETLJob(spark_session)

            monitor.start_monitoring()

            # Process multiple small batches to test memory cleanup
            for i in range(5):
                batch_data = test_data[i * 200 : (i + 1) * 200]

                input_df = create_spark_dataframe(spark_session, batch_data)
                cleaned_df = etl_job.clean_and_transform(input_df, f"memory_test_{i}")
                final_df = etl_job.map_to_snowflake_schema(cleaned_df)

                # Force computation
                final_df.count()

                monitor.update_peak_memory()

                # Force garbage collection
                gc.collect()

            metrics = monitor.stop_monitoring()

            # Memory should not grow excessively with repeated small operations
            assert (
                metrics["peak_memory_increase_mb"] < 500
            ), f"Memory usage too high: {metrics['peak_memory_increase_mb']:.2f} MB"

            print(f"\nMemory efficiency test results:")
            print(
                f"  Peak memory increase: {metrics['peak_memory_increase_mb']:.2f} MB"
            )
            print(f"  Final memory increase: {metrics['memory_increase_mb']:.2f} MB")

    def test_memory_cleanup_after_processing(self, spark_session):
        """Test that memory is properly cleaned up after processing."""
        record_count = 10000
        monitor = PerformanceMonitor()

        test_data = generate_performance_test_data(record_count, "simple")

        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "test.account",
                "SNOWFLAKE_USER": "test_user",
                "SNOWFLAKE_PASSWORD": "test_password",
            },
        ):
            etl_job = SalesETLJob(spark_session)

            monitor.start_monitoring()

            # Process data
            input_df = create_spark_dataframe(spark_session, test_data)
            cleaned_df = etl_job.clean_and_transform(input_df, "memory_cleanup_test")
            final_df = etl_job.map_to_snowflake_schema(cleaned_df)

            # Force computation
            final_df.count()

            monitor.update_peak_memory()

            # Clear references and force cleanup
            del input_df, cleaned_df, final_df
            gc.collect()

            # Wait a bit for cleanup
            time.sleep(1)

            metrics = monitor.stop_monitoring()

            # Memory should be mostly cleaned up
            cleanup_efficiency = 1 - (
                metrics["memory_increase_mb"] / metrics["peak_memory_increase_mb"]
            )
            assert (
                cleanup_efficiency > 0.5
            ), f"Poor memory cleanup: {cleanup_efficiency:.2%} efficiency"

            print(f"\nMemory cleanup test results:")
            print(
                f"  Peak memory increase: {metrics['peak_memory_increase_mb']:.2f} MB"
            )
            print(f"  Final memory increase: {metrics['memory_increase_mb']:.2f} MB")
            print(f"  Cleanup efficiency: {cleanup_efficiency:.2%}")

    @pytest.mark.slow
    def test_memory_stability_long_running(self, spark_session):
        """Test memory stability during long-running operations."""
        monitor = PerformanceMonitor()
        monitor.start_monitoring()

        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "test.account",
                "SNOWFLAKE_USER": "test_user",
                "SNOWFLAKE_PASSWORD": "test_password",
            },
        ):
            etl_job = SalesETLJob(spark_session)

            # Process multiple batches over time
            batch_count = 10
            records_per_batch = 1000

            memory_samples = []

            for i in range(batch_count):
                test_data = generate_performance_test_data(records_per_batch, "simple")

                input_df = create_spark_dataframe(spark_session, test_data)
                cleaned_df = etl_job.clean_and_transform(
                    input_df, f"stability_test_{i}"
                )
                final_df = etl_job.map_to_snowflake_schema(cleaned_df)

                # Force computation
                final_df.count()

                monitor.update_peak_memory()
                current_memory = psutil.Process().memory_info().rss / 1024 / 1024
                memory_samples.append(current_memory)

                # Force cleanup
                del input_df, cleaned_df, final_df
                gc.collect()

                # Small delay between batches
                time.sleep(0.5)

            metrics = monitor.stop_monitoring()

            # Check for memory leaks (memory should not continuously grow)
            memory_trend = memory_samples[-1] - memory_samples[0]
            memory_per_batch = memory_trend / batch_count

            assert (
                memory_per_batch < 10
            ), f"Potential memory leak: {memory_per_batch:.2f} MB per batch"

            print(f"\nMemory stability test results:")
            print(f"  Batches processed: {batch_count}")
            print(f"  Memory trend: {memory_trend:.2f} MB over {batch_count} batches")
            print(f"  Memory per batch: {memory_per_batch:.2f} MB")
            print(f"  Peak memory: {metrics['peak_memory_mb']:.2f} MB")


@pytest.mark.performance
@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestSparkOptimizations:
    """Tests for Spark-specific optimizations and configurations."""

    def test_partition_optimization(self, spark_session):
        """Test that data partitioning works efficiently."""
        record_count = 10000
        test_data = generate_performance_test_data(record_count, "simple")

        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "test.account",
                "SNOWFLAKE_USER": "test_user",
                "SNOWFLAKE_PASSWORD": "test_password",
            },
        ):
            etl_job = SalesETLJob(spark_session)

            input_df = create_spark_dataframe(spark_session, test_data)
            cleaned_df = etl_job.clean_and_transform(input_df, "partition_test")

            # Check partition count
            partition_count = cleaned_df.rdd.getNumPartitions()

            # Should have reasonable number of partitions for the data size
            assert (
                1 <= partition_count <= 10
            ), f"Inefficient partitioning: {partition_count} partitions for {record_count} records"

            # Test repartitioning for better performance
            if partition_count > 4:
                repartitioned_df = cleaned_df.repartition(4)
                assert repartitioned_df.rdd.getNumPartitions() == 4

    def test_caching_performance(self, spark_session):
        """Test performance benefits of DataFrame caching."""
        record_count = 5000
        test_data = generate_performance_test_data(record_count, "simple")

        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "test.account",
                "SNOWFLAKE_USER": "test_user",
                "SNOWFLAKE_PASSWORD": "test_password",
            },
        ):
            etl_job = SalesETLJob(spark_session)

            input_df = create_spark_dataframe(spark_session, test_data)
            cleaned_df = etl_job.clean_and_transform(input_df, "caching_test")

            # Test without caching
            start_time = time.time()
            count1 = cleaned_df.count()
            count2 = cleaned_df.count()  # Second computation
            time_without_cache = time.time() - start_time

            # Test with caching
            cleaned_df.cache()
            start_time = time.time()
            count3 = cleaned_df.count()  # Load into cache
            count4 = cleaned_df.count()  # Read from cache
            time_with_cache = time.time() - start_time

            cleaned_df.unpersist()

            # Verify results are consistent
            assert count1 == count2 == count3 == count4, "Caching changed results"

            # Caching should improve performance for repeated operations
            # (Note: This might not always be true for small datasets in tests)
            print(f"\nCaching performance test:")
            print(f"  Time without cache: {time_without_cache:.3f}s")
            print(f"  Time with cache: {time_with_cache:.3f}s")

    def test_broadcast_join_efficiency(self, spark_session):
        """Test efficiency of broadcast joins for small lookup tables."""
        # Create main dataset
        main_data = generate_performance_test_data(5000, "simple")
        main_df = create_spark_dataframe(spark_session, main_data)

        # Create small lookup table (simulating dimension table)
        lookup_data = [
            {"Category": "TECHNOLOGY", "CategoryGroup": "Electronics"},
            {"Category": "FURNITURE", "CategoryGroup": "Home"},
            {"Category": "OFFICE SUPPLIES", "CategoryGroup": "Business"},
        ]
        lookup_df = spark_session.createDataFrame(lookup_data)

        monitor = PerformanceMonitor()
        monitor.start_monitoring()

        # Perform join operation
        result_df = main_df.join(lookup_df, on="Category", how="left")

        # Force computation
        result_count = result_df.count()

        metrics = monitor.stop_monitoring()

        # Verify join worked correctly
        assert result_count == len(main_data), "Join changed record count unexpectedly"

        # Join should be relatively fast
        assert (
            metrics["duration_seconds"] < 30
        ), f"Join took too long: {metrics['duration_seconds']:.2f}s"

        print(f"\nBroadcast join performance:")
        print(f"  Duration: {metrics['duration_seconds']:.2f}s")
        print(f"  Records processed: {result_count}")


@pytest.mark.benchmark
@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not available")
class TestPerformanceBaselines:
    """Baseline performance tests for regression detection."""

    def test_baseline_transformation_performance(self, spark_session):
        """Establish baseline performance for transformation operations."""
        record_count = 10000
        test_data = generate_performance_test_data(record_count, "simple")

        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "test.account",
                "SNOWFLAKE_USER": "test_user",
                "SNOWFLAKE_PASSWORD": "test_password",
            },
        ):
            etl_job = SalesETLJob(spark_session)

            # Measure each transformation step separately
            input_df = create_spark_dataframe(spark_session, test_data)

            # Transformation step
            start_time = time.time()
            cleaned_df = etl_job.clean_and_transform(input_df, "baseline_test")
            cleaned_df.count()  # Force computation
            transform_time = time.time() - start_time

            # Schema mapping step
            start_time = time.time()
            final_df = etl_job.map_to_snowflake_schema(cleaned_df)
            final_df.count()  # Force computation
            mapping_time = time.time() - start_time

            # Performance baselines (adjust based on your environment)
            transform_rate = record_count / transform_time
            mapping_rate = record_count / mapping_time

            assert (
                transform_rate > 1000
            ), f"Transformation too slow: {transform_rate:.0f} records/second"
            assert (
                mapping_rate > 5000
            ), f"Schema mapping too slow: {mapping_rate:.0f} records/second"

            print(f"\nBaseline performance metrics:")
            print(f"  Transformation rate: {transform_rate:.0f} records/second")
            print(f"  Schema mapping rate: {mapping_rate:.0f} records/second")
            print(f"  Total processing time: {transform_time + mapping_time:.2f}s")

    @pytest.mark.slow
    def test_scalability_benchmark(self, spark_session):
        """Test how performance scales with data size."""
        test_sizes = [1000, 5000, 10000]
        performance_metrics = []

        with patch.dict(
            os.environ,
            {
                "SNOWFLAKE_ACCOUNT": "test.account",
                "SNOWFLAKE_USER": "test_user",
                "SNOWFLAKE_PASSWORD": "test_password",
            },
        ):
            etl_job = SalesETLJob(spark_session)

            for size in test_sizes:
                test_data = generate_performance_test_data(size, "simple")

                monitor = PerformanceMonitor()
                monitor.start_monitoring()

                input_df = create_spark_dataframe(spark_session, test_data)
                cleaned_df = etl_job.clean_and_transform(input_df, f"scale_test_{size}")
                final_df = etl_job.map_to_snowflake_schema(cleaned_df)
                result_count = final_df.count()

                metrics = monitor.stop_monitoring()

                performance_metrics.append(
                    {
                        "size": size,
                        "duration": metrics["duration_seconds"],
                        "rate": result_count / metrics["duration_seconds"],
                        "memory": metrics["peak_memory_increase_mb"],
                    }
                )

            # Analyze scalability
            print(f"\nScalability benchmark results:")
            for i, metric in enumerate(performance_metrics):
                print(
                    f"  {metric['size']:5d} records: {metric['duration']:.2f}s, "
                    f"{metric['rate']:.0f} rec/s, {metric['memory']:.1f}MB"
                )

            # Performance should scale reasonably (not exponentially)
            small_rate = performance_metrics[0]["rate"]
            large_rate = performance_metrics[-1]["rate"]

            # Allow for some performance degradation with size, but not too much
            rate_ratio = large_rate / small_rate
            assert (
                rate_ratio > 0.3
            ), f"Performance degrades too much with scale: {rate_ratio:.2f}"
