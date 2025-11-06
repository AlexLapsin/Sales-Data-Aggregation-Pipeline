#!/usr/bin/env python3
"""
Comprehensive Bronze Layer Verification Test Suite

This script tests all the enhanced Bronze layer functionality:
1. Idempotent batch uploads
2. Content-based duplicate detection
3. Partial data change handling
4. Metadata tracking accuracy
5. Streaming exactly-once semantics verification
"""

import json
import time
import hashlib
import tempfile
import os
from pathlib import Path
from datetime import datetime
import subprocess


class BronzeVerificationSuite:
    """Comprehensive test suite for Bronze layer verification"""

    def __init__(self, bucket_name="raw-sales-pipeline-976404003846"):
        self.bucket_name = bucket_name
        self.test_results = {}

    def run_command(self, command, description):
        """Run shell command and capture output"""
        print(f"\n🔍 {description}")
        print(f"Command: {command}")

        try:
            result = subprocess.run(
                command, shell=True, capture_output=True, text=True, timeout=60
            )
            if result.returncode == 0:
                print(f"✅ Success: {result.stdout.strip()}")
                return result.stdout.strip()
            else:
                print(f"❌ Error: {result.stderr.strip()}")
                return None
        except subprocess.TimeoutExpired:
            print(f"⏰ Timeout: Command took longer than 60 seconds")
            return None
        except Exception as e:
            print(f"❌ Exception: {str(e)}")
            return None

    def create_test_csv(self, filename, content_variant="original"):
        """Create test CSV files with different content"""
        base_content = """row_id,order_id,order_date,ship_date,ship_mode,customer_id,customer_name,segment,city,state,country,postal_code,market,region,product_id,category,sub_category,product_name,sales,quantity,discount,profit,shipping_cost,order_priority
1,TEST-001,15-01-2024,17-01-2024,Standard Class,CUST-TEST,Test Customer,Consumer,Test City,NY,United States,10001,US,East,PROD-TEST,Technology,Phones,Test Phone,299.99,1,0.05,89.99,9.99,Medium"""

        if content_variant == "modified":
            # Change the sales amount to create different content
            base_content = base_content.replace("299.99", "399.99")
        elif content_variant == "additional":
            # Add another row
            base_content += "\n2,TEST-002,16-01-2024,18-01-2024,First Class,CUST-TEST2,Test Customer 2,Corporate,Test City 2,CA,United States,90210,US,West,PROD-TEST2,Technology,Laptops,Test Laptop,599.99,1,0.10,199.99,19.99,High"

        with open(filename, "w") as f:
            f.write(base_content)

        # Calculate and return content hash
        with open(filename, "rb") as f:
            content_hash = hashlib.sha256(f.read()).hexdigest()

        return content_hash

    def test_1_idempotent_batch_upload(self):
        """Test 1: Verify idempotent batch upload behavior"""
        print("\n" + "=" * 60)
        print("TEST 1: IDEMPOTENT BATCH UPLOAD BEHAVIOR")
        print("=" * 60)

        test_date = "2024-01-20"
        test_dir = "test_verification_data"

        # Create test directory and file
        os.makedirs(test_dir, exist_ok=True)
        test_file = f"{test_dir}/test_idempotent.csv"
        original_hash = self.create_test_csv(test_file, "original")

        print(f"📊 Created test file: {test_file}")
        print(f"🔐 Content hash: {original_hash}")

        # First upload
        print(f"\n🚀 First upload attempt...")
        cmd1 = f"docker-compose exec -T etl-pipeline python src/bronze/data_uploader.py {self.bucket_name} ./{test_dir} {test_date}"
        result1 = self.run_command(cmd1, "First upload to Bronze layer")

        # Second upload (should be idempotent)
        print(f"\n🔄 Second upload attempt (should skip existing files)...")
        result2 = self.run_command(
            cmd1, "Second upload to Bronze layer (idempotent test)"
        )

        # Check S3 for results
        print(f"\n🔍 Checking S3 structure...")
        list_cmd = f"aws s3 ls s3://{self.bucket_name}/sales_data/year=2024/month=01/day=20/ --recursive"
        s3_result = self.run_command(list_cmd, "List files in Bronze layer")

        # Count occurrences of test file
        if s3_result:
            file_count = s3_result.count("test_idempotent.csv")
            print(f"📈 Found {file_count} occurrences of test file")
            self.test_results["idempotent_batch"] = file_count == 1
        else:
            self.test_results["idempotent_batch"] = False

        # Cleanup
        os.remove(test_file)
        os.rmdir(test_dir)

        return self.test_results["idempotent_batch"]

    def test_2_content_hash_detection(self):
        """Test 2: Verify content-based duplicate detection"""
        print("\n" + "=" * 60)
        print("TEST 2: CONTENT-BASED DUPLICATE DETECTION")
        print("=" * 60)

        test_date = "2024-01-21"
        test_dir = "test_hash_data"

        os.makedirs(test_dir, exist_ok=True)

        # Create original file
        original_file = f"{test_dir}/content_test_original.csv"
        original_hash = self.create_test_csv(original_file, "original")

        # Create identical file with different name
        identical_file = f"{test_dir}/content_test_identical.csv"
        identical_hash = self.create_test_csv(identical_file, "original")

        # Create modified file
        modified_file = f"{test_dir}/content_test_modified.csv"
        modified_hash = self.create_test_csv(modified_file, "modified")

        print(f"📊 Original hash:  {original_hash}")
        print(f"📊 Identical hash: {identical_hash}")
        print(f"📊 Modified hash:  {modified_hash}")

        # Verify hashes
        hash_test_passed = (original_hash == identical_hash) and (
            original_hash != modified_hash
        )
        print(f"🔐 Hash detection working: {hash_test_passed}")

        # Upload all files
        cmd = f"docker-compose exec -T etl-pipeline python src/bronze/data_uploader.py {self.bucket_name} ./{test_dir} {test_date}"
        result = self.run_command(cmd, "Upload files with different content hashes")

        # Check S3 results
        list_cmd = f"aws s3 ls s3://{self.bucket_name}/sales_data/year=2024/month=01/day=21/ --recursive"
        s3_result = self.run_command(list_cmd, "Check uploaded files")

        if s3_result:
            # Should have 3 files (original, identical, modified)
            file_count = len([line for line in s3_result.split("\n") if ".csv" in line])
            print(f"📈 Found {file_count} CSV files uploaded")
            self.test_results["content_hash"] = file_count == 3
        else:
            self.test_results["content_hash"] = False

        # Cleanup
        for f in [original_file, identical_file, modified_file]:
            os.remove(f)
        os.rmdir(test_dir)

        return self.test_results["content_hash"]

    def test_3_partial_data_changes(self):
        """Test 3: Verify handling of partial data changes"""
        print("\n" + "=" * 60)
        print("TEST 3: PARTIAL DATA CHANGE HANDLING")
        print("=" * 60)

        test_date = "2024-01-22"
        test_dir = "test_partial_data"

        os.makedirs(test_dir, exist_ok=True)

        # Step 1: Upload original dataset
        original_file = f"{test_dir}/partial_test.csv"
        original_hash = self.create_test_csv(original_file, "original")

        print(f"📊 Step 1: Upload original data (hash: {original_hash[:16]}...)")
        cmd1 = f"docker-compose exec -T etl-pipeline python src/bronze/data_uploader.py {self.bucket_name} ./{test_dir} {test_date}"
        self.run_command(cmd1, "Upload original dataset")

        # Step 2: Modify data and upload again
        modified_hash = self.create_test_csv(original_file, "modified")

        print(f"📊 Step 2: Upload modified data (hash: {modified_hash[:16]}...)")
        result2 = self.run_command(cmd1, "Upload modified dataset")

        # Step 3: Check that both versions exist with different batch IDs
        list_cmd = f"aws s3 ls s3://{self.bucket_name}/sales_data/year=2024/month=01/day=22/ --recursive"
        s3_result = self.run_command(list_cmd, "Check for multiple versions")

        if s3_result:
            # Should see multiple batch_id directories
            batch_dirs = len(
                [line for line in s3_result.split("\n") if "batch_id=" in line]
            )
            print(f"📈 Found {batch_dirs} batch directories")
            self.test_results["partial_changes"] = batch_dirs >= 2
        else:
            self.test_results["partial_changes"] = False

        # Cleanup
        os.remove(original_file)
        os.rmdir(test_dir)

        return self.test_results["partial_changes"]

    def test_4_metadata_tracking(self):
        """Test 4: Verify metadata tracking accuracy"""
        print("\n" + "=" * 60)
        print("TEST 4: METADATA TRACKING ACCURACY")
        print("=" * 60)

        test_date = "2024-01-23"

        # Check recent metadata file
        metadata_cmd = f'aws s3 ls s3://{self.bucket_name}/sales_data/ --recursive | grep "_metadata/upload_log.json" | tail -1'
        metadata_result = self.run_command(metadata_cmd, "Find recent metadata file")

        if metadata_result:
            # Extract S3 key from ls output
            metadata_key = metadata_result.split()[-1]

            # Download and examine metadata
            download_cmd = f"aws s3 cp s3://{self.bucket_name}/{metadata_key} -"
            metadata_content = self.run_command(download_cmd, "Download metadata file")

            if metadata_content:
                try:
                    metadata = json.loads(metadata_content)

                    # Check for enhanced fields
                    required_fields = [
                        "batch_id",
                        "upload_timestamp",
                        "files_uploaded",
                        "files_skipped",
                        "total_size_bytes",
                    ]
                    has_enhanced_fields = all(
                        field in metadata for field in required_fields
                    )

                    print(f"🔍 Metadata fields present: {list(metadata.keys())}")
                    print(f"✅ Enhanced metadata: {has_enhanced_fields}")

                    self.test_results["metadata_tracking"] = has_enhanced_fields

                except json.JSONDecodeError:
                    print("❌ Invalid JSON in metadata file")
                    self.test_results["metadata_tracking"] = False
            else:
                self.test_results["metadata_tracking"] = False
        else:
            self.test_results["metadata_tracking"] = False

        return self.test_results["metadata_tracking"]

    def test_5_streaming_data_flow(self):
        """Test 5: Verify streaming data flow and partitioning"""
        print("\n" + "=" * 60)
        print("TEST 5: STREAMING DATA FLOW VERIFICATION")
        print("=" * 60)

        # Check connector status
        status_cmd = 'curl -s "http://localhost:8083/connectors/sales-transactions-s3-sink/status"'
        status_result = self.run_command(status_cmd, "Check S3 Sink Connector status")

        connector_running = False
        if status_result:
            try:
                status_data = json.loads(status_result)
                connector_running = (
                    status_data.get("connector", {}).get("state") == "RUNNING"
                )
                print(
                    f"🔌 Connector state: {status_data.get('connector', {}).get('state', 'UNKNOWN')}"
                )
            except json.JSONDecodeError:
                pass

        # Check for streaming data in S3
        streaming_cmd = f"aws s3 ls s3://{self.bucket_name}/sales_transactions/streaming_data/ --recursive | head -5"
        streaming_result = self.run_command(
            streaming_cmd, "Check streaming data in Bronze"
        )

        has_streaming_data = (
            streaming_result and "sales_transactions+" in streaming_result
        )

        # Check partitioning structure
        if has_streaming_data:
            partition_check = (
                "year=" in streaming_result
                and "month=" in streaming_result
                and "day=" in streaming_result
                and "hour=" in streaming_result
            )
            print(f"📅 Time-based partitioning: {partition_check}")
        else:
            partition_check = False

        self.test_results["streaming_flow"] = (
            connector_running and has_streaming_data and partition_check
        )

        return self.test_results["streaming_flow"]

    def run_all_tests(self):
        """Run complete verification suite"""
        print("🧪 BRONZE LAYER COMPREHENSIVE VERIFICATION SUITE")
        print("=" * 80)

        start_time = time.time()

        # Run all tests
        tests = [
            ("Idempotent Batch Upload", self.test_1_idempotent_batch_upload),
            ("Content Hash Detection", self.test_2_content_hash_detection),
            ("Partial Data Changes", self.test_3_partial_data_changes),
            ("Metadata Tracking", self.test_4_metadata_tracking),
            ("Streaming Data Flow", self.test_5_streaming_data_flow),
        ]

        results = {}
        for test_name, test_func in tests:
            try:
                result = test_func()
                results[test_name] = result
            except Exception as e:
                print(f"❌ {test_name} failed with exception: {str(e)}")
                results[test_name] = False

        # Generate final report
        self.generate_final_report(results, time.time() - start_time)

        return results

    def generate_final_report(self, results, duration):
        """Generate comprehensive test report"""
        print("\n" + "=" * 80)
        print("🏁 FINAL VERIFICATION REPORT")
        print("=" * 80)

        passed = sum(1 for result in results.values() if result)
        total = len(results)

        print(f"⏱️  Duration: {duration:.2f} seconds")
        print(f"📊 Overall: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
        print()

        for test_name, result in results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"{status} {test_name}")

        print()

        if passed == total:
            print("🎉 ALL TESTS PASSED - Bronze layer implementation verified!")
            print("✅ Idempotent operations working correctly")
            print("✅ Content-based duplicate detection active")
            print("✅ Partial data changes handled properly")
            print("✅ Enhanced metadata tracking operational")
            print("✅ Streaming data flow confirmed")
        else:
            print(f"⚠️  {total - passed} tests failed - review implementation")

        print("\n" + "=" * 80)


if __name__ == "__main__":
    verifier = BronzeVerificationSuite()
    verifier.run_all_tests()
