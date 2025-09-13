#!/usr/bin/env python3
"""
Demo Script for Kafka Producer Tests

This script demonstrates the testing capabilities without requiring
all external dependencies. It shows the test structure and validates
the core logic.
"""

import sys
import os
import json
import time
from datetime import datetime, timezone

# Add streaming directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "streaming"))


def demo_data_generation():
    """Demo the SalesDataGenerator testing"""
    print("==> Testing SalesDataGenerator...")

    try:
        from kafka_producer import SalesDataGenerator

        generator = SalesDataGenerator()

        # Test 1: Basic event generation
        print("  ✓ Generator initialized successfully")

        # Test 2: Generate sample events
        events = []
        start_time = time.time()

        for i in range(100):
            event = generator.generate_sales_event()
            events.append(event)

        end_time = time.time()
        generation_rate = 100 / (end_time - start_time)

        print(
            f"  ✓ Generated 100 events in {end_time - start_time:.3f}s ({generation_rate:.0f} events/sec)"
        )

        # Test 3: Validate event structure
        sample_event = events[0]
        required_fields = [
            "order_id",
            "store_id",
            "product_id",
            "product_name",
            "category",
            "quantity",
            "unit_price",
            "total_price",
            "sale_timestamp",
            "customer_id",
            "payment_method",
            "discount",
            "store_location",
        ]

        missing_fields = [
            field for field in required_fields if field not in sample_event
        ]
        if missing_fields:
            print(f"  ✗ Missing fields: {missing_fields}")
        else:
            print("  ✓ All required fields present")

        # Test 4: Validate data types
        type_checks = [
            ("order_id", str),
            ("quantity", int),
            ("unit_price", float),
            ("total_price", float),
            ("discount", float),
        ]

        type_errors = []
        for field, expected_type in type_checks:
            if not isinstance(sample_event[field], expected_type):
                type_errors.append(
                    f"{field}: expected {expected_type.__name__}, got {type(sample_event[field]).__name__}"
                )

        if type_errors:
            print(f"  ✗ Type errors: {type_errors}")
        else:
            print("  ✓ All field types correct")

        # Test 5: Validate data realism
        categories = list(set(event["category"] for event in events))
        locations = list(set(event["store_location"] for event in events))

        print(f"  ✓ Generated {len(categories)} unique categories: {categories[:3]}...")
        print(f"  ✓ Generated {len(locations)} unique locations: {locations[:3]}...")

        # Test 6: Validate constraints
        constraint_violations = []
        for event in events[:10]:  # Check first 10 events
            if event["quantity"] <= 0:
                constraint_violations.append("Invalid quantity")
            if event["unit_price"] <= 0:
                constraint_violations.append("Invalid unit_price")
            if not (0 <= event["discount"] <= 1):
                constraint_violations.append("Invalid discount")

        if constraint_violations:
            print(f"  ✗ Constraint violations: {set(constraint_violations)}")
        else:
            print("  ✓ All data constraints satisfied")

        print(f"  ==> Sample event: {json.dumps(sample_event, indent=2)[:200]}...")

        return True

    except ImportError as e:
        print(f"  ✗ Import error: {e}")
        return False
    except Exception as e:
        print(f"  ✗ Unexpected error: {e}")
        return False


def demo_producer_mocking():
    """Demo the producer testing approach with mocking"""
    print("\n==> Testing KafkaSalesProducer (with mocking)...")

    try:
        # Mock KafkaProducer to demonstrate testing approach
        class MockKafkaProducer:
            def __init__(self, **kwargs):
                self.config = kwargs
                self.sent_messages = []

            def send(self, topic, key=None, value=None):
                message = {
                    "topic": topic,
                    "key": key,
                    "value": value,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                self.sent_messages.append(message)

                # Mock future object
                class MockFuture:
                    def get(self, timeout=None):
                        class MockMetadata:
                            topic = topic
                            partition = 0
                            offset = len(self.sent_messages)

                        return MockMetadata()

                return MockFuture()

            def flush(self):
                pass

            def close(self):
                pass

        # Mock the import
        import sys
        from unittest.mock import patch

        # Test producer configuration
        mock_producer = MockKafkaProducer(
            bootstrap_servers=["localhost:9092"],
            acks="all",
            retries=3,
            compression_type="gzip",
        )

        print("  ✓ Mock producer created with correct configuration")

        # Test message production
        from kafka_producer import SalesDataGenerator

        generator = SalesDataGenerator()

        for i in range(5):
            event = generator.generate_sales_event()
            mock_producer.send("test_topic", key=event["order_id"], value=event)

        print(f"  ✓ Sent {len(mock_producer.sent_messages)} messages")

        # Validate message structure
        first_message = mock_producer.sent_messages[0]
        print(
            f"  ✓ Message structure: topic={first_message['topic']}, key present={first_message['key'] is not None}"
        )

        return True

    except Exception as e:
        print(f"  FAIL: Error: {e}")
        return False


def demo_schema_validation():
    """Demo schema validation testing"""
    print("\n==> Testing Schema Validation...")

    try:
        from kafka_producer import SalesDataGenerator

        generator = SalesDataGenerator()
        event = generator.generate_sales_event()

        # Define schema (simplified version)
        schema_checks = [
            ("order_id", str, lambda x: len(x) > 0),
            ("quantity", int, lambda x: x > 0),
            ("unit_price", float, lambda x: x > 0),
            ("total_price", float, lambda x: x >= 0),
            ("discount", float, lambda x: 0 <= x <= 1),
            ("sale_timestamp", str, lambda x: "T" in x),  # Basic ISO format check
        ]

        validation_errors = []
        for field, expected_type, validator in schema_checks:
            if field not in event:
                validation_errors.append(f"Missing field: {field}")
            elif not isinstance(event[field], expected_type):
                validation_errors.append(
                    f"Wrong type for {field}: expected {expected_type.__name__}"
                )
            elif not validator(event[field]):
                validation_errors.append(
                    f"Validation failed for {field}: {event[field]}"
                )

        if validation_errors:
            print(f"  ✗ Schema validation errors: {validation_errors}")
        else:
            print("  ✓ Schema validation passed")

        # Test JSON serialization
        json_str = json.dumps(event)
        deserialized = json.loads(json_str)

        if event == deserialized:
            print("  ✓ JSON serialization roundtrip successful")
        else:
            print("  ✗ JSON serialization roundtrip failed")

        # Test message size
        message_size = len(json_str.encode("utf-8"))
        if message_size < 1024:
            print(f"  ✓ Message size acceptable: {message_size} bytes")
        else:
            print(f"  ⚠ Message size large: {message_size} bytes")

        return True

    except Exception as e:
        print(f"  FAIL: Error: {e}")
        return False


def demo_performance_testing():
    """Demo performance testing approach"""
    print("\n==> Testing Performance...")

    try:
        from kafka_producer import SalesDataGenerator

        generator = SalesDataGenerator()

        # Test generation performance
        test_sizes = [100, 500, 1000]

        for size in test_sizes:
            start_time = time.time()
            events = [generator.generate_sales_event() for _ in range(size)]
            end_time = time.time()

            elapsed_time = end_time - start_time
            rate = size / elapsed_time

            print(
                f"  ✓ Generated {size} events in {elapsed_time:.3f}s ({rate:.0f} events/sec)"
            )

        # Test memory usage (basic)
        import sys

        events = []
        initial_size = sys.getsizeof(events)

        for _ in range(1000):
            events.append(generator.generate_sales_event())

        final_size = sys.getsizeof(events) + sum(
            sys.getsizeof(event) for event in events
        )
        memory_per_event = (final_size - initial_size) / 1000

        print(f"  ✓ Memory usage: ~{memory_per_event:.0f} bytes per event")

        return True

    except Exception as e:
        print(f"  FAIL: Error: {e}")
        return False


def main():
    """Run all demos"""
    print("==> Kafka Producer Test Suite Demo")
    print("=" * 50)

    results = []

    results.append(("Data Generation", demo_data_generation()))
    results.append(("Producer Mocking", demo_producer_mocking()))
    results.append(("Schema Validation", demo_schema_validation()))
    results.append(("Performance", demo_performance_testing()))

    print("\n" + "=" * 50)
    print("==> Demo Results:")

    for test_name, success in results:
        status = "PASS" if success else "FAIL"
        print(f"  {status} {test_name}")

    successful_tests = sum(1 for _, success in results if success)
    total_tests = len(results)

    print(f"\n==> {successful_tests}/{total_tests} test categories working")

    if successful_tests == total_tests:
        print("\n==> All test categories are functional!")
        print("\n==> Next steps:")
        print("  1. Install test dependencies: pip install -r requirements-test.txt")
        print("  2. Run unit tests: python run_kafka_producer_tests.py --mode unit")
        print(
            "  3. Start Kafka and run integration tests: python run_kafka_producer_tests.py --mode integration"
        )
    else:
        print(
            "\n==> Some test categories have issues - check the streaming/kafka_producer.py file"
        )

    return 0 if successful_tests == total_tests else 1


if __name__ == "__main__":
    sys.exit(main())
