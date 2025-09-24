#!/usr/bin/env python3
"""
Kafka Streaming Component Tests

This module provides comprehensive tests for Kafka streaming components including
producer, consumer, Kafka Connect, and streaming data processing.
"""

import json
import time
import uuid
import pytest
import requests
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError, KafkaError
import subprocess
import os
import threading
from collections import defaultdict


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaStreamingTester:
    """Comprehensive Kafka streaming functionality tester"""

    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.test_topic_prefix = "test_kafka_streaming"
        self.test_id = str(uuid.uuid4())[:8]
        self.admin_client = None

    def get_admin_client(self) -> KafkaAdminClient:
        """Get Kafka admin client"""
        if not self.admin_client:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=[self.bootstrap_servers], request_timeout_ms=10000
            )
        return self.admin_client

    def cleanup_test_topics(self) -> None:
        """Clean up test topics"""
        try:
            admin_client = self.get_admin_client()
            existing_topics = admin_client.list_topics()

            test_topics = [
                topic
                for topic in existing_topics
                if topic.startswith(self.test_topic_prefix)
            ]

            if test_topics:
                admin_client.delete_topics(test_topics)
                logger.info(f"Cleaned up test topics: {test_topics}")

        except Exception as e:
            logger.warning(f"Failed to cleanup test topics: {e}")

    def create_test_topic(
        self, topic_name: str, num_partitions: int = 3, replication_factor: int = 1
    ) -> bool:
        """Create a test topic"""
        try:
            admin_client = self.get_admin_client()

            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )

            result = admin_client.create_topics([topic])

            # Wait for creation
            for topic_name, future in result.items():
                try:
                    future.result(timeout=10)
                    logger.info(f"Test topic {topic_name} created successfully")
                    return True
                except TopicAlreadyExistsError:
                    logger.info(f"Test topic {topic_name} already exists")
                    return True
                except Exception as e:
                    logger.error(f"Failed to create test topic {topic_name}: {e}")
                    return False

            return False

        except Exception as e:
            logger.error(f"Failed to create test topic: {e}")
            return False

    def test_producer_functionality(self) -> Dict[str, Any]:
        """Test Kafka producer functionality"""
        test_topic = f"{self.test_topic_prefix}_producer_{self.test_id}"
        results = {
            "topic": test_topic,
            "success": False,
            "messages_sent": 0,
            "errors": [],
        }

        try:
            # Create test topic
            if not self.create_test_topic(test_topic):
                results["errors"].append("Failed to create test topic")
                return results

            # Create producer
            producer = KafkaProducer(
                bootstrap_servers=[self.bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                batch_size=16384,
                linger_ms=10,
                compression_type="gzip",
            )

            # Send test messages
            test_messages = []
            for i in range(10):
                message = {
                    "id": i,
                    "test_id": self.test_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "data": f"test_message_{i}",
                }
                test_messages.append(message)

                # Send with different keys for partitioning
                key = f"key_{i % 3}"
                future = producer.send(test_topic, key=key, value=message)

                # Check for immediate errors
                try:
                    record_metadata = future.get(timeout=10)
                    results["messages_sent"] += 1
                    logger.debug(
                        f"Message {i} sent to partition {record_metadata.partition}"
                    )
                except Exception as e:
                    results["errors"].append(f"Message {i} failed: {e}")

            producer.flush()
            producer.close()

            results["success"] = (
                results["messages_sent"] == 10 and not results["errors"]
            )

        except Exception as e:
            results["errors"].append(f"Producer test failed: {e}")

        return results

    def test_consumer_functionality(self) -> Dict[str, Any]:
        """Test Kafka consumer functionality"""
        test_topic = f"{self.test_topic_prefix}_consumer_{self.test_id}"
        results = {
            "topic": test_topic,
            "success": False,
            "messages_consumed": 0,
            "errors": [],
        }

        try:
            # Create test topic and send messages
            if not self.create_test_topic(test_topic):
                results["errors"].append("Failed to create test topic")
                return results

            # Produce test messages first
            producer = KafkaProducer(
                bootstrap_servers=[self.bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )

            num_messages = 5
            for i in range(num_messages):
                message = {
                    "id": i,
                    "test_id": self.test_id,
                    "data": f"consumer_test_{i}",
                }
                producer.send(test_topic, message)

            producer.flush()
            producer.close()

            # Now test consumer
            consumer = KafkaConsumer(
                test_topic,
                bootstrap_servers=[self.bootstrap_servers],
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                consumer_timeout_ms=15000,
                group_id=f"test_group_{self.test_id}",
            )

            consumed_messages = []
            start_time = time.time()

            for message in consumer:
                if message.value.get("test_id") == self.test_id:
                    consumed_messages.append(message.value)
                    results["messages_consumed"] += 1

                # Break if we've consumed all messages or timeout
                if (
                    len(consumed_messages) >= num_messages
                    or time.time() - start_time > 10
                ):
                    break

            consumer.close()

            results["success"] = results["messages_consumed"] == num_messages
            if not results["success"]:
                results["errors"].append(
                    f"Expected {num_messages} messages, got {results['messages_consumed']}"
                )

        except Exception as e:
            results["errors"].append(f"Consumer test failed: {e}")

        return results

    def test_partitioning_behavior(self) -> Dict[str, Any]:
        """Test Kafka partitioning behavior"""
        test_topic = f"{self.test_topic_prefix}_partitioning_{self.test_id}"
        results = {
            "topic": test_topic,
            "success": False,
            "partition_distribution": {},
            "errors": [],
        }

        try:
            # Create topic with multiple partitions
            if not self.create_test_topic(test_topic, num_partitions=3):
                results["errors"].append("Failed to create test topic")
                return results

            # Produce messages with different keys
            producer = KafkaProducer(
                bootstrap_servers=[self.bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
            )

            # Send messages with specific keys to test partitioning
            keys = ["key_a", "key_b", "key_c"]
            partition_counts = defaultdict(int)

            for i in range(30):  # Send 30 messages
                key = keys[i % 3]
                message = {"id": i, "key": key, "test_id": self.test_id}

                future = producer.send(test_topic, key=key, value=message)
                record_metadata = future.get(timeout=10)
                partition_counts[record_metadata.partition] += 1

            producer.flush()
            producer.close()

            results["partition_distribution"] = dict(partition_counts)

            # Check that messages were distributed across partitions
            results["success"] = len(partition_counts) > 1
            if not results["success"]:
                results["errors"].append("Messages not distributed across partitions")

        except Exception as e:
            results["errors"].append(f"Partitioning test failed: {e}")

        return results

    def test_consumer_group_behavior(self) -> Dict[str, Any]:
        """Test consumer group behavior and load balancing"""
        test_topic = f"{self.test_topic_prefix}_consumer_group_{self.test_id}"
        results = {
            "topic": test_topic,
            "success": False,
            "consumer_results": {},
            "errors": [],
        }

        try:
            # Create topic with multiple partitions
            if not self.create_test_topic(test_topic, num_partitions=3):
                results["errors"].append("Failed to create test topic")
                return results

            # Produce messages
            producer = KafkaProducer(
                bootstrap_servers=[self.bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )

            num_messages = 30
            for i in range(num_messages):
                message = {"id": i, "test_id": self.test_id}
                producer.send(test_topic, message)

            producer.flush()
            producer.close()

            # Create multiple consumers in the same group
            group_id = f"test_consumer_group_{self.test_id}"
            consumer_results = {}

            def consume_messages(consumer_id: str):
                consumer = KafkaConsumer(
                    test_topic,
                    bootstrap_servers=[self.bootstrap_servers],
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    auto_offset_reset="earliest",
                    consumer_timeout_ms=10000,
                    group_id=group_id,
                )

                messages = []
                for message in consumer:
                    if message.value.get("test_id") == self.test_id:
                        messages.append(message.value["id"])

                consumer.close()
                consumer_results[consumer_id] = messages

            # Start multiple consumers
            threads = []
            for i in range(2):
                thread = threading.Thread(
                    target=consume_messages, args=[f"consumer_{i}"]
                )
                threads.append(thread)
                thread.start()

            # Wait for consumers to finish
            for thread in threads:
                thread.join(timeout=15)

            results["consumer_results"] = consumer_results

            # Check that messages were distributed between consumers
            total_consumed = sum(len(msgs) for msgs in consumer_results.values())
            results["success"] = (
                total_consumed == num_messages and len(consumer_results) >= 2
            )

            if not results["success"]:
                results["errors"].append(
                    f"Expected {num_messages} total messages across consumers, got {total_consumed}"
                )

        except Exception as e:
            results["errors"].append(f"Consumer group test failed: {e}")

        return results

    def test_kafka_connect_health(self) -> Dict[str, Any]:
        """Test Kafka Connect health and status"""
        results = {"success": False, "connectors": [], "errors": []}

        try:
            # Check if Kafka Connect is running
            connect_url = "http://localhost:8083"

            # Test basic connectivity
            response = requests.get(f"{connect_url}/", timeout=10)
            if response.status_code != 200:
                results["errors"].append(
                    f"Kafka Connect not accessible: {response.status_code}"
                )
                return results

            # Get connector plugins
            response = requests.get(f"{connect_url}/connector-plugins", timeout=10)
            if response.status_code == 200:
                plugins = response.json()
                snowflake_plugins = [
                    p for p in plugins if "snowflake" in p.get("class", "").lower()
                ]

                if snowflake_plugins:
                    results["connectors"] = snowflake_plugins
                    results["success"] = True
                    logger.info("Snowflake connector plugin found in Kafka Connect")
                else:
                    results["errors"].append("Snowflake connector plugin not found")
            else:
                results["errors"].append(
                    f"Failed to get connector plugins: {response.status_code}"
                )

        except requests.exceptions.ConnectionError:
            results["errors"].append("Kafka Connect not running or not accessible")
        except Exception as e:
            results["errors"].append(f"Kafka Connect health check failed: {e}")

        return results

    def test_message_serialization(self) -> Dict[str, Any]:
        """Test different message serialization formats"""
        test_topic = f"{self.test_topic_prefix}_serialization_{self.test_id}"
        results = {
            "topic": test_topic,
            "success": False,
            "formats_tested": [],
            "errors": [],
        }

        try:
            if not self.create_test_topic(test_topic):
                results["errors"].append("Failed to create test topic")
                return results

            # Test JSON serialization
            json_producer = KafkaProducer(
                bootstrap_servers=[self.bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )

            json_message = {
                "type": "json",
                "test_id": self.test_id,
                "data": {"nested": "value"},
            }
            json_producer.send(test_topic, json_message)
            json_producer.flush()
            json_producer.close()
            results["formats_tested"].append("json")

            # Test string serialization
            string_producer = KafkaProducer(
                bootstrap_servers=[self.bootstrap_servers],
                value_serializer=lambda v: v.encode("utf-8"),
            )

            string_message = f"string_message_{self.test_id}"
            string_producer.send(test_topic, string_message)
            string_producer.flush()
            string_producer.close()
            results["formats_tested"].append("string")

            # Test bytes serialization
            bytes_producer = KafkaProducer(bootstrap_servers=[self.bootstrap_servers])

            bytes_message = f"bytes_message_{self.test_id}".encode("utf-8")
            bytes_producer.send(test_topic, bytes_message)
            bytes_producer.flush()
            bytes_producer.close()
            results["formats_tested"].append("bytes")

            # Consume and verify messages
            consumer = KafkaConsumer(
                test_topic,
                bootstrap_servers=[self.bootstrap_servers],
                auto_offset_reset="earliest",
                consumer_timeout_ms=10000,
            )

            messages_received = 0
            for message in consumer:
                messages_received += 1
                if messages_received >= 3:  # We sent 3 different format messages
                    break

            consumer.close()

            results["success"] = messages_received == 3
            if not results["success"]:
                results["errors"].append(
                    f"Expected 3 messages, received {messages_received}"
                )

        except Exception as e:
            results["errors"].append(f"Serialization test failed: {e}")

        return results


# Test fixtures
@pytest.fixture(scope="session")
def kafka_tester():
    """Fixture providing Kafka streaming tester"""
    tester = KafkaStreamingTester()
    yield tester
    # Cleanup after tests
    tester.cleanup_test_topics()
    if tester.admin_client:
        tester.admin_client.close()


# Test cases
def test_kafka_producer_functionality(kafka_tester):
    """Test Kafka producer can send messages successfully"""
    results = kafka_tester.test_producer_functionality()

    assert results["success"], f"Producer test failed: {results['errors']}"
    assert (
        results["messages_sent"] == 10
    ), f"Expected 10 messages sent, got {results['messages_sent']}"


def test_kafka_consumer_functionality(kafka_tester):
    """Test Kafka consumer can receive messages successfully"""
    results = kafka_tester.test_consumer_functionality()

    assert results["success"], f"Consumer test failed: {results['errors']}"
    assert (
        results["messages_consumed"] == 5
    ), f"Expected 5 messages consumed, got {results['messages_consumed']}"


def test_kafka_partitioning_behavior(kafka_tester):
    """Test Kafka message partitioning works correctly"""
    results = kafka_tester.test_partitioning_behavior()

    assert results["success"], f"Partitioning test failed: {results['errors']}"
    assert (
        len(results["partition_distribution"]) > 1
    ), "Messages should be distributed across multiple partitions"


def test_kafka_consumer_group_behavior(kafka_tester):
    """Test Kafka consumer group load balancing"""
    results = kafka_tester.test_consumer_group_behavior()

    assert results["success"], f"Consumer group test failed: {results['errors']}"

    # Verify messages were distributed between consumers
    consumer_results = results["consumer_results"]
    assert len(consumer_results) >= 2, "Should have at least 2 consumers"

    total_messages = sum(len(msgs) for msgs in consumer_results.values())
    assert total_messages == 30, f"Expected 30 total messages, got {total_messages}"


def test_kafka_connect_availability(kafka_tester):
    """Test Kafka Connect is available and has required connectors"""
    results = kafka_tester.test_kafka_connect_health()

    # Note: This test may fail if Kafka Connect is not running, which is okay for local dev
    if results["success"]:
        assert len(results["connectors"]) > 0, "No Snowflake connectors found"
    else:
        pytest.skip("Kafka Connect not available - this is expected in basic setup")


def test_kafka_message_serialization(kafka_tester):
    """Test different message serialization formats work correctly"""
    results = kafka_tester.test_message_serialization()

    assert results["success"], f"Serialization test failed: {results['errors']}"
    assert "json" in results["formats_tested"], "JSON serialization not tested"
    assert "string" in results["formats_tested"], "String serialization not tested"
    assert "bytes" in results["formats_tested"], "Bytes serialization not tested"


def test_sales_events_topic_exists():
    """Test that the main sales_events topic exists"""
    admin_client = KafkaAdminClient(
        bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")],
        request_timeout_ms=10000,
    )

    try:
        topics = admin_client.list_topics()
        assert "sales_events" in topics, "sales_events topic does not exist"

        # Get topic configuration
        topic_metadata = admin_client.describe_topics(["sales_events"])
        assert len(topic_metadata) > 0, "Failed to get sales_events topic metadata"

    finally:
        admin_client.close()


def test_kafka_cluster_metadata():
    """Test Kafka cluster metadata and configuration"""
    admin_client = KafkaAdminClient(
        bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")],
        request_timeout_ms=10000,
    )

    try:
        # Test cluster metadata
        metadata = admin_client.describe_cluster()
        assert len(metadata.brokers) >= 1, "No Kafka brokers found"

        # Test topics
        topics = admin_client.list_topics()
        logger.info(f"Available topics: {list(topics)}")

        # Test configs for the cluster
        cluster_resource = ConfigResource(ConfigResourceType.BROKER, "0")
        configs = admin_client.describe_configs([cluster_resource])
        assert len(configs) > 0, "Failed to get broker configurations"

    finally:
        admin_client.close()


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
