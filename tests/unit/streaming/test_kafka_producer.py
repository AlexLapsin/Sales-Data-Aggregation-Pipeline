#!/usr/bin/env python3
"""
Comprehensive Test Suite for Kafka Producer Components

This module provides extensive testing for:
1. SalesDataGenerator - data generation quality and realism
2. KafkaSalesProducer - Kafka connectivity and message production
3. Error handling and retry logic
4. Performance under different loads
5. Message serialization and schema validation
6. Configuration scenarios (Docker Kafka vs AWS MSK)

Test Categories:
- Unit Tests: Test individual components in isolation with mocking
- Integration Tests: Test with real Kafka instances
- Performance Tests: Load testing and throughput validation
- Schema Tests: Message format and data quality validation
"""

import json
import os
import time
import uuid
import pytest
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
from unittest.mock import Mock, patch, MagicMock, call
from collections import defaultdict
import threading
import statistics

# Test frameworks and utilities
from pytest_benchmark import BenchmarkFixture
from faker import Faker
import jsonschema
from memory_profiler import profile
import psutil

# Kafka imports
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable
from kafka.admin import NewTopic

# Import the classes we're testing
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "streaming"))
from kafka_producer import SalesDataGenerator, KafkaSalesProducer, load_config

# Configure logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test configuration
TEST_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TEST_TOPIC_PREFIX = "test_kafka_producer"


class TestSalesDataGenerator:
    """Test suite for SalesDataGenerator class focusing on data quality and realism"""

    @pytest.fixture
    def generator(self):
        """Fixture providing a SalesDataGenerator instance"""
        return SalesDataGenerator()

    def test_generator_initialization(self, generator):
        """Test that generator initializes with proper data structures"""
        assert hasattr(generator, "product_categories")
        assert hasattr(generator, "store_locations")
        assert hasattr(generator, "price_ranges")

        # Verify data structure integrity
        assert len(generator.product_categories) > 0
        assert len(generator.store_locations) > 0
        assert len(generator.price_ranges) > 0

        # Verify all categories have price ranges
        for category in generator.product_categories.keys():
            assert category in generator.price_ranges
            price_range = generator.price_ranges[category]
            assert len(price_range) == 2
            assert price_range[0] < price_range[1]  # min < max

    def test_single_event_generation(self, generator):
        """Test generation of a single sales event"""
        event = generator.generate_sales_event()

        # Test required fields exist
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

        for field in required_fields:
            assert field in event, f"Missing required field: {field}"

    def test_event_field_types(self, generator):
        """Test that generated events have correct field types"""
        event = generator.generate_sales_event()

        # String fields
        string_fields = [
            "order_id",
            "store_id",
            "product_id",
            "product_name",
            "category",
            "sale_timestamp",
            "customer_id",
            "payment_method",
            "store_location",
        ]
        for field in string_fields:
            assert isinstance(event[field], str), f"Field {field} should be string"

        # Numeric fields
        assert isinstance(event["quantity"], int)
        assert isinstance(event["unit_price"], float)
        assert isinstance(event["total_price"], float)
        assert isinstance(event["discount"], float)

        # Value constraints
        assert event["quantity"] > 0
        assert event["unit_price"] > 0
        assert event["total_price"] > 0
        assert 0 <= event["discount"] <= 1

    def test_event_data_realism(self, generator):
        """Test that generated data appears realistic"""
        events = [generator.generate_sales_event() for _ in range(100)]

        # Check category distribution
        categories = [event["category"] for event in events]
        unique_categories = set(categories)
        assert len(unique_categories) > 1, "Should generate multiple categories"

        # Check store location distribution
        locations = [event["store_location"] for event in events]
        unique_locations = set(locations)
        assert len(unique_locations) > 1, "Should generate multiple store locations"

        # Check quantity distribution (should favor lower quantities)
        quantities = [event["quantity"] for event in events]
        avg_quantity = sum(quantities) / len(quantities)
        assert (
            1 <= avg_quantity <= 5
        ), f"Average quantity {avg_quantity} seems unrealistic"

        # Check that most quantities are small (1-3)
        small_quantities = [q for q in quantities if q <= 3]
        assert (
            len(small_quantities) / len(quantities) > 0.6
        ), "Should favor smaller quantities"

    def test_price_ranges_by_category(self, generator):
        """Test that prices fall within expected ranges for each category"""
        events_by_category = defaultdict(list)

        # Generate enough events to cover all categories
        for _ in range(500):
            event = generator.generate_sales_event()
            events_by_category[event["category"]].append(event)

        # Verify each category has events
        for category in generator.product_categories.keys():
            assert (
                category in events_by_category
            ), f"No events generated for category {category}"

        # Verify prices are within expected ranges
        for category, events in events_by_category.items():
            price_range = generator.price_ranges[category]
            min_price, max_price = price_range

            for event in events:
                unit_price = event["unit_price"]
                assert (
                    min_price <= unit_price <= max_price
                ), f"Price {unit_price} outside range {price_range} for category {category}"

    def test_discount_application(self, generator):
        """Test that discounts are applied correctly"""
        events_with_discount = []
        events_without_discount = []

        # Generate events and separate by discount status
        for _ in range(200):
            event = generator.generate_sales_event()
            if event["discount"] > 0:
                events_with_discount.append(event)
            else:
                events_without_discount.append(event)

        # Verify discount frequency (should be around 30%)
        total_events = len(events_with_discount) + len(events_without_discount)
        discount_ratio = len(events_with_discount) / total_events
        assert (
            0.2 <= discount_ratio <= 0.4
        ), f"Discount ratio {discount_ratio} outside expected range"

        # Verify discount calculation for discounted events
        for event in events_with_discount:
            expected_total = (
                event["unit_price"] * event["quantity"] * (1 - event["discount"])
            )
            assert (
                abs(event["total_price"] - expected_total) < 0.01
            ), "Discount calculation incorrect"

    def test_unique_identifiers(self, generator):
        """Test that generated IDs are unique"""
        events = [generator.generate_sales_event() for _ in range(100)]

        # Test order_id uniqueness
        order_ids = [event["order_id"] for event in events]
        assert len(set(order_ids)) == len(order_ids), "Order IDs should be unique"

        # Test product_id format and variety
        product_ids = [event["product_id"] for event in events]
        assert len(set(product_ids)) > 50, "Should generate varied product IDs"

        # Test customer_id format
        customer_ids = [event["customer_id"] for event in events]
        for customer_id in customer_ids:
            assert customer_id.startswith(
                "CUST_"
            ), "Customer ID should start with CUST_"
            assert customer_id[5:].isdigit(), "Customer ID should have numeric suffix"

    def test_timestamp_format(self, generator):
        """Test that timestamps are properly formatted"""
        event = generator.generate_sales_event()
        timestamp_str = event["sale_timestamp"]

        # Parse the timestamp
        try:
            timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            assert timestamp.tzinfo is not None, "Timestamp should include timezone"
        except ValueError:
            pytest.fail(f"Invalid timestamp format: {timestamp_str}")

        # Verify timestamp is recent (within last minute)
        now = datetime.now(timezone.utc)
        time_diff = abs((now - timestamp).total_seconds())
        assert time_diff < 60, "Timestamp should be very recent"

    def test_payment_method_distribution(self, generator):
        """Test payment method distribution"""
        events = [generator.generate_sales_event() for _ in range(200)]
        payment_methods = [event["payment_method"] for event in events]

        expected_methods = ["Credit Card", "Debit Card", "Cash", "Digital Wallet"]
        unique_methods = set(payment_methods)

        # Should use all expected methods
        for method in expected_methods:
            assert method in unique_methods, f"Payment method {method} not found"

        # Should have reasonable distribution
        method_counts = {
            method: payment_methods.count(method) for method in expected_methods
        }
        for method, count in method_counts.items():
            ratio = count / len(events)
            assert (
                0.1 <= ratio <= 0.5
            ), f"Payment method {method} has unusual distribution: {ratio}"

    @pytest.mark.performance
    def test_generation_performance(self, generator, benchmark: BenchmarkFixture):
        """Test data generation performance"""
        # Benchmark single event generation
        result = benchmark(generator.generate_sales_event)
        assert isinstance(result, dict)

        # Test batch generation performance
        def generate_batch():
            return [generator.generate_sales_event() for _ in range(100)]

        start_time = time.time()
        events = generate_batch()
        end_time = time.time()

        generation_time = end_time - start_time
        events_per_second = len(events) / generation_time

        # Should be able to generate at least 1000 events per second
        assert (
            events_per_second > 1000
        ), f"Generation too slow: {events_per_second} events/sec"
        logger.info(f"Data generation performance: {events_per_second:.0f} events/sec")


class TestKafkaSalesProducerUnit:
    """Unit tests for KafkaSalesProducer with mocking (no real Kafka required)"""

    @pytest.fixture
    def mock_kafka_producer(self):
        """Fixture providing a mocked KafkaProducer"""
        with patch("kafka_producer.KafkaProducer") as mock_producer_class:
            mock_producer = Mock()
            mock_producer_class.return_value = mock_producer
            yield mock_producer

    @pytest.fixture
    def sales_producer(self, mock_kafka_producer):
        """Fixture providing a KafkaSalesProducer with mocked Kafka"""
        return KafkaSalesProducer("localhost:9092", "test_topic")

    def test_producer_initialization(self, mock_kafka_producer, sales_producer):
        """Test producer initialization with correct parameters"""
        # Verify KafkaProducer was called with correct parameters
        mock_producer_class = patch.object(sales_producer, "producer").start()

        # Check that producer has expected attributes
        assert sales_producer.bootstrap_servers == "localhost:9092"
        assert sales_producer.topic == "test_topic"
        assert hasattr(sales_producer, "data_generator")
        assert isinstance(sales_producer.data_generator, SalesDataGenerator)

    def test_producer_configuration_parameters(self):
        """Test that KafkaProducer is configured with correct parameters"""
        with patch("kafka_producer.KafkaProducer") as mock_producer_class:
            KafkaSalesProducer("localhost:9092", "test_topic")

            # Verify producer was called with expected configuration
            mock_producer_class.assert_called_once()
            call_kwargs = mock_producer_class.call_args[1]

            # Check key configuration parameters
            assert call_kwargs["bootstrap_servers"] == ["localhost:9092"]
            assert call_kwargs["acks"] == "all"
            assert call_kwargs["retries"] == 3
            assert call_kwargs["compression_type"] == "gzip"
            assert call_kwargs["max_in_flight_requests_per_connection"] == 1

    def test_produce_single_event_success(self, mock_kafka_producer, sales_producer):
        """Test successful production of a single event"""
        # Setup mock response
        mock_future = Mock()
        mock_metadata = Mock()
        mock_metadata.topic = "test_topic"
        mock_metadata.partition = 0
        mock_metadata.offset = 123
        mock_future.get.return_value = mock_metadata
        mock_kafka_producer.send.return_value = mock_future

        # Produce event
        event = sales_producer.produce_sales_event()

        # Verify calls
        mock_kafka_producer.send.assert_called_once()
        call_args = mock_kafka_producer.send.call_args
        assert call_args[0][0] == "test_topic"  # topic
        assert call_args[1]["key"] == event["order_id"]  # key
        assert call_args[1]["value"] == event  # value

        # Verify event structure
        assert isinstance(event, dict)
        assert "order_id" in event

    def test_produce_event_kafka_error(self, mock_kafka_producer, sales_producer):
        """Test handling of Kafka errors during event production"""
        # Setup mock to raise KafkaError
        mock_future = Mock()
        mock_future.get.side_effect = KafkaTimeoutError("Timeout occurred")
        mock_kafka_producer.send.return_value = mock_future

        # Should raise the KafkaError
        with pytest.raises(KafkaError):
            sales_producer.produce_sales_event()

    def test_produce_continuous_with_count(self, mock_kafka_producer, sales_producer):
        """Test continuous production with specified count"""
        # Setup successful mock responses
        mock_future = Mock()
        mock_metadata = Mock()
        mock_metadata.topic = "test_topic"
        mock_metadata.partition = 0
        mock_metadata.offset = 123
        mock_future.get.return_value = mock_metadata
        mock_kafka_producer.send.return_value = mock_future

        # Produce 5 events
        sales_producer.produce_continuous(count=5, interval=0)

        # Should have called send 5 times
        assert mock_kafka_producer.send.call_count == 5
        mock_kafka_producer.flush.assert_called_once()
        mock_kafka_producer.close.assert_called_once()

    def test_produce_continuous_keyboard_interrupt(
        self, mock_kafka_producer, sales_producer
    ):
        """Test graceful shutdown on KeyboardInterrupt"""
        # Setup mock to raise KeyboardInterrupt after first call
        mock_future = Mock()
        mock_metadata = Mock()
        mock_future.get.return_value = mock_metadata

        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise KeyboardInterrupt()
            return mock_future

        mock_kafka_producer.send.side_effect = side_effect

        # Should handle KeyboardInterrupt gracefully
        sales_producer.produce_continuous(count=10, interval=0)

        # Should still call cleanup methods
        mock_kafka_producer.flush.assert_called_once()
        mock_kafka_producer.close.assert_called_once()

    def test_produce_continuous_burst_mode(self, mock_kafka_producer, sales_producer):
        """Test burst mode production (no intervals)"""
        mock_future = Mock()
        mock_metadata = Mock()
        mock_future.get.return_value = mock_metadata
        mock_kafka_producer.send.return_value = mock_future

        start_time = time.time()
        sales_producer.produce_continuous(count=3, interval=1.0, burst_mode=True)
        end_time = time.time()

        # In burst mode, should complete very quickly even with interval=1.0
        elapsed_time = end_time - start_time
        assert elapsed_time < 1.0, f"Burst mode too slow: {elapsed_time}s"

        assert mock_kafka_producer.send.call_count == 3

    @patch("time.sleep")
    def test_produce_continuous_with_intervals(
        self, mock_sleep, mock_kafka_producer, sales_producer
    ):
        """Test that intervals are respected in non-burst mode"""
        mock_future = Mock()
        mock_metadata = Mock()
        mock_future.get.return_value = mock_metadata
        mock_kafka_producer.send.return_value = mock_future

        sales_producer.produce_continuous(count=3, interval=0.5, burst_mode=False)

        # Should have called sleep between events (3 events = 2 sleeps)
        assert mock_sleep.call_count == 2
        mock_sleep.assert_has_calls([call(0.5), call(0.5)])


class TestKafkaSalesProducerIntegration:
    """Integration tests requiring real Kafka instance"""

    @pytest.fixture(scope="class")
    def kafka_available(self):
        """Check if Kafka is available for integration tests"""
        try:
            from kafka import KafkaAdminClient

            admin = KafkaAdminClient(
                bootstrap_servers=[TEST_BOOTSTRAP_SERVERS], request_timeout_ms=5000
            )
            admin.list_topics()
            admin.close()
            return True
        except Exception as e:
            pytest.skip(f"Kafka not available for integration tests: {e}")

    @pytest.fixture
    def test_topic(self, kafka_available):
        """Create a test topic for integration tests"""
        topic_name = f"{TEST_TOPIC_PREFIX}_integration_{uuid.uuid4().hex[:8]}"

        # Create topic
        admin = KafkaAdminClient(bootstrap_servers=[TEST_BOOTSTRAP_SERVERS])
        try:
            topic = NewTopic(name=topic_name, num_partitions=3, replication_factor=1)
            admin.create_topics([topic])
            time.sleep(1)  # Allow topic creation to propagate
        except Exception as e:
            logger.warning(f"Topic creation failed (may already exist): {e}")
        finally:
            admin.close()

        yield topic_name

        # Cleanup
        admin = KafkaAdminClient(bootstrap_servers=[TEST_BOOTSTRAP_SERVERS])
        try:
            admin.delete_topics([topic_name])
        except Exception as e:
            logger.warning(f"Topic cleanup failed: {e}")
        finally:
            admin.close()

    def test_real_kafka_connection(self, test_topic):
        """Test actual connection to Kafka"""
        producer = KafkaSalesProducer(TEST_BOOTSTRAP_SERVERS, test_topic)

        # Should be able to produce an event
        event = producer.produce_sales_event()
        assert isinstance(event, dict)
        assert "order_id" in event

        # Cleanup
        producer.producer.close()

    def test_message_delivery_and_consumption(self, test_topic):
        """Test end-to-end message delivery"""
        producer = KafkaSalesProducer(TEST_BOOTSTRAP_SERVERS, test_topic)

        # Produce some events
        events_sent = []
        for _ in range(5):
            event = producer.produce_sales_event()
            events_sent.append(event)

        producer.producer.flush()
        producer.producer.close()

        # Consume and verify messages
        consumer = KafkaConsumer(
            test_topic,
            bootstrap_servers=[TEST_BOOTSTRAP_SERVERS],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            consumer_timeout_ms=10000,
        )

        events_received = []
        for message in consumer:
            events_received.append(message.value)
            if len(events_received) >= 5:
                break

        consumer.close()

        # Verify all events were received
        assert len(events_received) == 5

        # Verify event order_ids match (order may differ due to partitioning)
        sent_order_ids = {event["order_id"] for event in events_sent}
        received_order_ids = {event["order_id"] for event in events_received}
        assert sent_order_ids == received_order_ids

    def test_partitioning_with_keys(self, test_topic):
        """Test that events are properly partitioned by order_id"""
        producer = KafkaSalesProducer(TEST_BOOTSTRAP_SERVERS, test_topic)

        # Produce events and track partition assignment
        partition_assignments = {}
        for _ in range(20):
            event = producer.produce_sales_event()
            # Access the future to get partition info
            future = producer.producer.send(
                test_topic, key=event["order_id"], value=event
            )
            record_metadata = future.get(timeout=10)
            partition_assignments[event["order_id"]] = record_metadata.partition

        producer.producer.close()

        # Should have used multiple partitions
        unique_partitions = set(partition_assignments.values())
        assert (
            len(unique_partitions) > 1
        ), "Events should be distributed across partitions"

    @pytest.mark.performance
    def test_production_throughput(self, test_topic):
        """Test production throughput under load"""
        producer = KafkaSalesProducer(TEST_BOOTSTRAP_SERVERS, test_topic)

        start_time = time.time()
        event_count = 100

        for _ in range(event_count):
            producer.produce_sales_event()

        producer.producer.flush()
        end_time = time.time()
        producer.producer.close()

        elapsed_time = end_time - start_time
        throughput = event_count / elapsed_time

        # Should achieve reasonable throughput
        assert throughput > 50, f"Throughput too low: {throughput} events/sec"
        logger.info(f"Production throughput: {throughput:.1f} events/sec")


class TestMessageSchemaValidation:
    """Test message format and schema validation"""

    @pytest.fixture
    def sales_event_schema(self):
        """JSON schema for sales events"""
        return {
            "type": "object",
            "required": [
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
            ],
            "properties": {
                "order_id": {"type": "string", "pattern": "^[0-9a-f-]+$"},
                "store_id": {"type": "string", "pattern": "^STORE_[A-Z_]+$"},
                "product_id": {"type": "string"},
                "product_name": {"type": "string"},
                "category": {"type": "string"},
                "quantity": {"type": "integer", "minimum": 1, "maximum": 100},
                "unit_price": {"type": "number", "minimum": 0},
                "total_price": {"type": "number", "minimum": 0},
                "sale_timestamp": {"type": "string", "format": "date-time"},
                "customer_id": {"type": "string", "pattern": "^CUST_[0-9]+$"},
                "payment_method": {
                    "type": "string",
                    "enum": ["Credit Card", "Debit Card", "Cash", "Digital Wallet"],
                },
                "discount": {"type": "number", "minimum": 0, "maximum": 1},
                "store_location": {"type": "string"},
            },
        }

    def test_generated_events_match_schema(self, sales_event_schema):
        """Test that generated events conform to expected schema"""
        generator = SalesDataGenerator()

        for _ in range(20):
            event = generator.generate_sales_event()

            # Validate against schema
            try:
                jsonschema.validate(event, sales_event_schema)
            except jsonschema.ValidationError as e:
                pytest.fail(f"Event schema validation failed: {e}")

    def test_serialization_roundtrip(self):
        """Test JSON serialization/deserialization roundtrip"""
        generator = SalesDataGenerator()
        original_event = generator.generate_sales_event()

        # Serialize to JSON
        json_str = json.dumps(original_event)

        # Deserialize back
        deserialized_event = json.loads(json_str)

        # Should be identical
        assert original_event == deserialized_event

    def test_message_size_constraints(self):
        """Test that messages don't exceed reasonable size limits"""
        generator = SalesDataGenerator()

        for _ in range(50):
            event = generator.generate_sales_event()
            json_str = json.dumps(event)
            message_size = len(json_str.encode("utf-8"))

            # Should be under 1KB (reasonable for Kafka)
            assert message_size < 1024, f"Message too large: {message_size} bytes"


class TestErrorHandlingAndRetries:
    """Test error handling and retry logic"""

    def test_connection_failure_handling(self):
        """Test handling of connection failures"""
        # Try to connect to non-existent broker
        with pytest.raises((KafkaError, Exception)):
            producer = KafkaSalesProducer("invalid_host:9092", "test_topic")
            producer.produce_sales_event()

    @patch("kafka_producer.KafkaProducer")
    def test_retry_configuration(self, mock_producer_class):
        """Test that retry configuration is properly set"""
        KafkaSalesProducer("localhost:9092", "test_topic")

        call_kwargs = mock_producer_class.call_args[1]
        assert call_kwargs["retries"] == 3
        assert call_kwargs["retry_backoff_ms"] == 100

    @patch("kafka_producer.logger")
    def test_error_logging(self, mock_logger):
        """Test that errors are properly logged"""
        with patch("kafka_producer.KafkaProducer") as mock_producer_class:
            mock_producer = Mock()
            mock_producer_class.return_value = mock_producer

            # Setup send to raise exception
            mock_future = Mock()
            mock_future.get.side_effect = KafkaTimeoutError("Test timeout")
            mock_producer.send.return_value = mock_future

            producer = KafkaSalesProducer("localhost:9092", "test_topic")

            with pytest.raises(KafkaError):
                producer.produce_sales_event()

            # Should have logged the error
            mock_logger.error.assert_called()


class TestConfigurationScenarios:
    """Test different configuration scenarios"""

    def test_load_config_file_success(self, tmp_path):
        """Test successful configuration loading"""
        config_data = {
            "bootstrap_servers": "test_host:9092",
            "topic": "test_topic",
            "count": 100,
            "interval": 0.5,
        }

        config_file = tmp_path / "test_config.json"
        config_file.write_text(json.dumps(config_data))

        loaded_config = load_config(str(config_file))
        assert loaded_config == config_data

    def test_load_config_file_not_found(self):
        """Test handling of missing config file"""
        config = load_config("nonexistent_config.json")
        assert config == {}

    def test_load_config_invalid_json(self, tmp_path):
        """Test handling of invalid JSON in config file"""
        config_file = tmp_path / "invalid_config.json"
        config_file.write_text("{ invalid json }")

        with pytest.raises(json.JSONDecodeError):
            load_config(str(config_file))

    def test_environment_variable_configuration(self):
        """Test configuration via environment variables"""
        with patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": "env_host:9092"}):
            # Should use environment variable
            bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
            assert bootstrap_servers == "env_host:9092"

    def test_docker_kafka_configuration(self):
        """Test configuration for Docker Kafka setup"""
        # Typical Docker Kafka configuration
        bootstrap_servers = "localhost:9092"
        producer = KafkaSalesProducer(bootstrap_servers, "sales_events")

        assert producer.bootstrap_servers == bootstrap_servers
        assert producer.topic == "sales_events"

    def test_aws_msk_configuration(self):
        """Test configuration for AWS MSK setup"""
        # Typical MSK configuration
        msk_bootstrap_servers = "b-1.msk-cluster.kafka.us-west-2.amazonaws.com:9092,b-2.msk-cluster.kafka.us-west-2.amazonaws.com:9092"

        with patch("kafka_producer.KafkaProducer") as mock_producer_class:
            producer = KafkaSalesProducer(msk_bootstrap_servers, "sales_events")

            # Should split comma-separated servers
            call_kwargs = mock_producer_class.call_args[1]
            expected_servers = msk_bootstrap_servers.split(",")
            assert call_kwargs["bootstrap_servers"] == expected_servers


class TestPerformanceAndLoad:
    """Performance and load testing"""

    @pytest.mark.performance
    def test_memory_usage_during_generation(self):
        """Test memory usage during data generation"""
        generator = SalesDataGenerator()

        # Monitor memory usage
        process = psutil.Process()
        initial_memory = process.memory_info().rss

        # Generate many events
        events = []
        for _ in range(1000):
            events.append(generator.generate_sales_event())

        peak_memory = process.memory_info().rss
        memory_increase = peak_memory - initial_memory

        # Memory increase should be reasonable (less than 10MB for 1000 events)
        assert (
            memory_increase < 10 * 1024 * 1024
        ), f"Memory usage too high: {memory_increase} bytes"

    @pytest.mark.performance
    def test_concurrent_producers(self):
        """Test multiple producers running concurrently"""

        def produce_events(producer_id: int, results: dict):
            try:
                with patch("kafka_producer.KafkaProducer") as mock_producer_class:
                    mock_producer = Mock()
                    mock_producer_class.return_value = mock_producer

                    # Setup successful responses
                    mock_future = Mock()
                    mock_metadata = Mock()
                    mock_future.get.return_value = mock_metadata
                    mock_producer.send.return_value = mock_future

                    producer = KafkaSalesProducer(
                        "localhost:9092", f"test_topic_{producer_id}"
                    )

                    start_time = time.time()
                    for _ in range(100):
                        producer.produce_sales_event()
                    end_time = time.time()

                    results[producer_id] = {
                        "elapsed_time": end_time - start_time,
                        "events_produced": 100,
                    }
            except Exception as e:
                results[producer_id] = {"error": str(e)}

        # Run multiple producers concurrently
        results = {}
        threads = []

        for i in range(3):
            thread = threading.Thread(target=produce_events, args=[i, results])
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # All producers should complete successfully
        assert len(results) == 3
        for producer_id, result in results.items():
            assert (
                "error" not in result
            ), f"Producer {producer_id} failed: {result.get('error')}"
            assert result["events_produced"] == 100

    @pytest.mark.performance
    def test_batch_production_performance(self):
        """Test performance of batch event production"""
        with patch("kafka_producer.KafkaProducer") as mock_producer_class:
            mock_producer = Mock()
            mock_producer_class.return_value = mock_producer

            # Setup successful responses
            mock_future = Mock()
            mock_metadata = Mock()
            mock_future.get.return_value = mock_metadata
            mock_producer.send.return_value = mock_future

            producer = KafkaSalesProducer("localhost:9092", "test_topic")

            # Test different batch sizes
            batch_sizes = [10, 50, 100, 500]
            performance_results = {}

            for batch_size in batch_sizes:
                start_time = time.time()
                producer.produce_continuous(
                    count=batch_size, interval=0, burst_mode=True
                )
                end_time = time.time()

                elapsed_time = end_time - start_time
                throughput = batch_size / elapsed_time
                performance_results[batch_size] = throughput

            # Throughput should be consistent across batch sizes
            throughputs = list(performance_results.values())
            avg_throughput = statistics.mean(throughputs)

            for batch_size, throughput in performance_results.items():
                deviation = abs(throughput - avg_throughput) / avg_throughput
                assert (
                    deviation < 0.5
                ), f"Throughput inconsistent for batch size {batch_size}: {throughput}"


# Test runner configuration
if __name__ == "__main__":
    # Run tests with comprehensive output
    pytest.main(
        [
            __file__,
            "-v",
            "--tb=short",
            "--cov=kafka_producer",
            "--cov-report=html",
            "--cov-report=term-missing",
            "-m",
            "not performance",  # Skip performance tests by default
        ]
    )
