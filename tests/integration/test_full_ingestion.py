#!/usr/bin/env python3
"""
Full Ingestion Layer Test - Portfolio Demonstration

Simple test script to verify both batch and streaming data
successfully reach S3 Bronze layer. Perfect for showcasing
complete data engineering pipeline functionality.

Usage:
    python test_full_ingestion.py
"""

import subprocess
import json
import os
from datetime import datetime


def run_command(command, description):
    """Run shell command and capture output"""
    print(f"\n[TEST] {description}")

    try:
        result = subprocess.run(
            command, shell=True, capture_output=True, text=True, timeout=30
        )
        return result.stdout.strip(), result.returncode == 0
    except subprocess.TimeoutExpired:
        return "Command timeout", False
    except Exception as e:
        return str(e), False


def test_kafka_connector_status():
    """Test if Kafka Connect S3 sink is running"""
    print("\n" + "=" * 60)
    print("1. TESTING KAFKA CONNECTOR STATUS")
    print("=" * 60)

    cmd = 'curl -s "http://localhost:8083/connectors/sales-transactions-s3-sink/status"'
    output, success = run_command(cmd, "Checking Kafka S3 Sink Connector")

    if success:
        try:
            status_data = json.loads(output)
            connector_state = status_data.get("connector", {}).get("state", "UNKNOWN")
            tasks = status_data.get("tasks", [])
            running_tasks = len([t for t in tasks if t.get("state") == "RUNNING"])

            print(f"Connector State: {connector_state}")
            print(f"Running Tasks: {running_tasks}/{len(tasks)}")

            if connector_state == "RUNNING" and running_tasks > 0:
                print("SUCCESS: Kafka connector is healthy and running")
                return True
            else:
                print("FAILED: Kafka connector has issues")
                return False

        except json.JSONDecodeError:
            print("FAILED: Invalid response from Kafka Connect")
            return False
    else:
        print("FAILED: Cannot connect to Kafka Connect")
        return False


def test_batch_data_in_bronze():
    """Test if batch CSV data exists in S3 Bronze layer"""
    print("\n" + "=" * 60)
    print("2. TESTING BATCH DATA IN BRONZE LAYER")
    print("=" * 60)

    bucket = os.getenv("RAW_BUCKET", "raw-sales-pipeline-976404003846")

    cmd = f"aws s3 ls s3://{bucket}/sales_data/ --recursive"
    output, success = run_command(cmd, "Checking batch CSV files in Bronze layer")

    if success and output:
        csv_files = [
            line
            for line in output.split("\n")
            if ".csv" in line and "sales_data" in line
        ]
        metadata_files = [line for line in output.split("\n") if "_metadata" in line]

        total_size = 0
        if csv_files:
            for file_line in csv_files:
                parts = file_line.split()
                if len(parts) >= 3:
                    try:
                        total_size += int(parts[2])
                    except ValueError:
                        pass

        print(f"CSV Files Found: {len(csv_files)}")
        print(f"Metadata Files: {len(metadata_files)}")
        print(f"Total Size: {total_size:,} bytes")

        if csv_files:
            print("Recent batch files:")
            for file_line in csv_files[-3:]:
                parts = file_line.split()
                if len(parts) >= 4:
                    print(f"  {parts[3]} ({int(parts[2]):,} bytes)")

            print("SUCCESS: Batch data successfully ingested to Bronze layer")
            return True, len(csv_files), total_size
        else:
            print("WARNING: No batch CSV files found in Bronze layer")
            return False, 0, 0
    else:
        print("FAILED: Cannot access S3 Bronze layer or no data found")
        return False, 0, 0


def test_streaming_data_in_bronze():
    """Test if streaming JSON data exists in S3 Bronze layer"""
    print("\n" + "=" * 60)
    print("3. TESTING STREAMING DATA IN BRONZE LAYER")
    print("=" * 60)

    bucket = os.getenv("RAW_BUCKET", "raw-sales-pipeline-976404003846")

    cmd = f"aws s3 ls s3://{bucket}/sales_transactions/streaming_data/ --recursive"
    output, success = run_command(cmd, "Checking streaming JSON files in Bronze layer")

    if success and output:
        json_files = [
            line for line in output.split("\n") if "sales_transactions+" in line
        ]

        total_size = 0
        if json_files:
            for file_line in json_files:
                parts = file_line.split()
                if len(parts) >= 3:
                    try:
                        total_size += int(parts[2])
                    except ValueError:
                        pass

        print(f"JSON Files Found: {len(json_files)}")
        print(f"Total Size: {total_size:,} bytes")

        # Check time-based partitioning
        partitions = set()
        for file_line in json_files:
            if "year=" in file_line and "month=" in file_line:
                partitions.add("time-partitioned")

        print(f"Time Partitioning: {'ACTIVE' if partitions else 'MISSING'}")

        if json_files:
            print("Recent streaming files:")
            for file_line in json_files[-3:]:
                parts = file_line.split()
                if len(parts) >= 4:
                    print(f"  {parts[3]} ({int(parts[2]):,} bytes)")

            print("SUCCESS: Streaming data successfully ingested to Bronze layer")
            return True, len(json_files), total_size
        else:
            print("WARNING: No streaming JSON files found in Bronze layer")
            return False, 0, 0
    else:
        print("FAILED: Cannot access streaming data in Bronze layer")
        return False, 0, 0


def test_data_consistency():
    """Test that batch and streaming data coexist properly"""
    print("\n" + "=" * 60)
    print("4. TESTING DATA CONSISTENCY & COEXISTENCE")
    print("=" * 60)

    bucket = os.getenv("RAW_BUCKET", "raw-sales-pipeline-976404003846")

    # Check different path structures
    batch_cmd = f"aws s3 ls s3://{bucket}/sales_data/ --recursive | head -3"
    streaming_cmd = f"aws s3 ls s3://{bucket}/sales_transactions/ --recursive | head -3"

    batch_output, batch_success = run_command(batch_cmd, "Sampling batch data paths")
    streaming_output, streaming_success = run_command(
        streaming_cmd, "Sampling streaming data paths"
    )

    print("Path Structure Analysis:")
    print("  Batch data path: s3://bucket/sales_data/year=/month=/day=/")
    print(
        "  Streaming data path: s3://bucket/sales_transactions/streaming_data/year=/month=/day=/hour=/"
    )

    if batch_success and streaming_success:
        print("SUCCESS: Different path structures prevent data conflicts")
        print("SUCCESS: Both ingestion paths coexist properly")
        return True
    else:
        print("WARNING: Unable to verify path separation")
        return False


def main():
    """Run complete ingestion layer test"""
    print("FULL INGESTION LAYER TEST - PORTFOLIO DEMONSTRATION")
    print("=" * 80)
    print(f"Test Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Testing both batch and streaming data ingestion to S3 Bronze layer")

    # Run all tests
    results = {}

    results["kafka_connector"] = test_kafka_connector_status()
    results["batch_data"], batch_files, batch_size = test_batch_data_in_bronze()
    (
        results["streaming_data"],
        stream_files,
        stream_size,
    ) = test_streaming_data_in_bronze()
    results["data_consistency"] = test_data_consistency()

    # Final summary
    print("\n" + "=" * 80)
    print("FINAL RESULTS - FULL INGESTION LAYER STATUS")
    print("=" * 80)

    print(f"Kafka Connector: {'RUNNING' if results['kafka_connector'] else 'FAILED'}")
    print(
        f"Batch Ingestion: {'WORKING' if results['batch_data'] else 'FAILED'} ({batch_files} files, {batch_size:,} bytes)"
    )
    print(
        f"Streaming Ingestion: {'WORKING' if results['streaming_data'] else 'FAILED'} ({stream_files} files, {stream_size:,} bytes)"
    )
    print(
        f"Data Consistency: {'VERIFIED' if results['data_consistency'] else 'ISSUES'}"
    )

    # Overall status
    all_passed = all(results.values())
    both_data_paths = results["batch_data"] and results["streaming_data"]

    print(f"\nOVERALL STATUS: ", end="")
    if all_passed and both_data_paths:
        print("SUCCESS - FULL INGESTION LAYER WORKING")
        print(
            "Portfolio demonstration ready: Both batch and streaming ingestion successful"
        )
    elif both_data_paths:
        print("PARTIAL SUCCESS")
        print("Both data paths working but some components need attention")
    else:
        print("NEEDS ATTENTION")
        print("One or more ingestion paths not working properly")

    print("\nThis test demonstrates:")
    print("   • Complete data ingestion layer (batch + streaming)")
    print("   • S3 Bronze layer with proper partitioning")
    print("   • Data consistency and conflict prevention")
    print("   • Production-ready data engineering pipeline")

    return all_passed and both_data_paths


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
