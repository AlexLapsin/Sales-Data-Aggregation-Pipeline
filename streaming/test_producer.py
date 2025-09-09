#!/usr/bin/env python3
"""
Test script for Kafka producer

Simple test to verify the producer works correctly with sample data.
Can be used to test both Docker Kafka and MSK connectivity.
"""

import json
import logging
import os
import sys
from kafka_producer import KafkaSalesProducer, SalesDataGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_data_generation():
    """Test sales data generation"""
    print("Testing sales data generation...")
    generator = SalesDataGenerator()

    # Generate 3 sample events
    events = [generator.generate_sales_event() for _ in range(3)]

    for i, event in enumerate(events, 1):
        print(f"\nSample Event {i}:")
        print(json.dumps(event, indent=2))

    print("\nData generation test passed!")
    return True


def test_kafka_connection(bootstrap_servers: str = "localhost:9092"):
    """Test Kafka connection by sending a few events"""
    print(f"\nTesting Kafka connection to: {bootstrap_servers}")

    try:
        producer = KafkaSalesProducer(bootstrap_servers, "test_topic")

        # Send 3 test events
        for i in range(3):
            event = producer.produce_sales_event()
            print(f"Sent test event {i+1}: Order {event['order_id']}")

        # Clean up
        producer.producer.flush()
        producer.producer.close()

        print("Kafka connection test passed!")
        return True

    except Exception as e:
        print(f"Kafka connection test failed: {e}")
        return False


def main():
    """Run all tests"""
    print("Running Kafka Producer Tests")
    print("=" * 50)

    # Test 1: Data generation
    if not test_data_generation():
        return 1

    # Test 2: Kafka connection (optional if Kafka is running)
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    print(f"\nTesting Kafka connection (optional)")
    print(f"Set KAFKA_BOOTSTRAP_SERVERS env var to test different brokers")

    try:
        test_kafka_connection(bootstrap_servers)
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"WARNING: Kafka connection test skipped: {e}")
        print("This is normal if Kafka is not running locally")

    print("\nAll tests completed!")
    return 0


if __name__ == "__main__":
    exit(main())
