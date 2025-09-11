#!/usr/bin/env python3
"""
Service Startup and Readiness Tests

This module provides comprehensive tests for validating service startup sequences,
readiness checks, and system initialization in the sales data pipeline.
"""

import time
import requests
import subprocess
import json
import psycopg2
import pytest
from typing import Dict, List, Optional, Tuple
import logging
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import docker
import os
from concurrent.futures import ThreadPoolExecutor, as_completed


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ServiceReadinessChecker:
    """Manages service readiness checks and startup validation"""

    def __init__(self):
        self.services = {
            "postgres": {"port": 5432, "health_check": self._check_postgres_ready},
            "airflow": {"port": 8080, "health_check": self._check_airflow_ready},
            "kafka": {"port": 9092, "health_check": self._check_kafka_ready},
            "kafka-ui": {"port": 8090, "health_check": self._check_kafka_ui_ready},
            "zookeeper": {"port": 2181, "health_check": self._check_zookeeper_ready},
        }
        self.docker_client = docker.from_env()

    def _check_postgres_ready(self) -> bool:
        """Check if PostgreSQL is ready to accept connections"""
        try:
            conn = psycopg2.connect(
                host="localhost",
                port="5432",
                database="airflow",
                user="airflow",
                password="airflow",
                connect_timeout=5,
            )

            # Test that we can execute a query
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()

            conn.close()
            return result[0] == 1

        except Exception as e:
            logger.debug(f"PostgreSQL not ready: {e}")
            return False

    def _check_airflow_ready(self) -> bool:
        """Check if Airflow webserver is ready"""
        try:
            # Check health endpoint
            response = requests.get("http://localhost:8080/health", timeout=5)
            if response.status_code != 200:
                return False

            # Check that database is initialized
            response = requests.get(
                "http://localhost:8080/api/v1/config",
                timeout=5,
                auth=("admin", "admin"),
            )
            return response.status_code == 200

        except Exception as e:
            logger.debug(f"Airflow not ready: {e}")
            return False

    def _check_kafka_ready(self) -> bool:
        """Check if Kafka broker is ready"""
        try:
            # Test creating a producer
            producer = KafkaProducer(
                bootstrap_servers=["localhost:9092"],
                request_timeout_ms=5000,
                api_version=(0, 10, 1),
            )

            # Test that we can get cluster metadata
            metadata = producer.list_topics(timeout=5)
            producer.close()

            return len(metadata.topics) >= 0  # Should return empty set or topics

        except Exception as e:
            logger.debug(f"Kafka not ready: {e}")
            return False

    def _check_kafka_ui_ready(self) -> bool:
        """Check if Kafka UI is ready"""
        try:
            response = requests.get("http://localhost:8090", timeout=5)
            return response.status_code == 200
        except Exception as e:
            logger.debug(f"Kafka UI not ready: {e}")
            return False

    def _check_zookeeper_ready(self) -> bool:
        """Check if Zookeeper is ready"""
        try:
            # Use Kafka admin client to check Zookeeper indirectly
            admin_client = KafkaAdminClient(
                bootstrap_servers=["localhost:9092"], request_timeout_ms=5000
            )

            # Try to list topics (requires Zookeeper)
            metadata = admin_client.list_topics(timeout=5)
            admin_client.close()

            return True

        except Exception as e:
            logger.debug(f"Zookeeper not ready: {e}")
            return False

    def wait_for_service(self, service_name: str, timeout: int = 120) -> bool:
        """Wait for a specific service to become ready"""
        if service_name not in self.services:
            logger.error(f"Unknown service: {service_name}")
            return False

        service_config = self.services[service_name]
        health_check = service_config["health_check"]

        start_time = time.time()
        logger.info(f"Waiting for {service_name} to become ready...")

        while time.time() - start_time < timeout:
            if health_check():
                logger.info(f"{service_name} is ready")
                return True

            time.sleep(5)

        logger.error(f"{service_name} not ready after {timeout} seconds")
        return False

    def wait_for_all_services(self, timeout: int = 300) -> Dict[str, bool]:
        """Wait for all services to become ready"""
        results = {}

        # Use thread pool to check services in parallel
        with ThreadPoolExecutor(max_workers=len(self.services)) as executor:
            future_to_service = {
                executor.submit(self.wait_for_service, service, timeout): service
                for service in self.services.keys()
            }

            for future in as_completed(future_to_service):
                service = future_to_service[future]
                try:
                    results[service] = future.result()
                except Exception as e:
                    logger.error(f"Error checking {service}: {e}")
                    results[service] = False

        return results

    def get_service_startup_order(self) -> List[str]:
        """Get the recommended service startup order"""
        # Order based on dependencies
        return ["postgres", "zookeeper", "kafka", "kafka-ui", "airflow"]

    def check_service_dependencies(self) -> Dict[str, List[str]]:
        """Check that service dependencies are met"""
        dependencies = {
            "airflow": ["postgres"],
            "kafka": ["zookeeper"],
            "kafka-ui": ["kafka"],
        }

        results = {}
        for service, deps in dependencies.items():
            results[service] = []
            for dep in deps:
                if not self.services[dep]["health_check"]():
                    results[service].append(dep)

        return results


class DataInitializationChecker:
    """Checks data initialization and sample data loading"""

    def __init__(self):
        self.kafka_admin = None
        self.required_topics = ["sales_events"]

    def create_kafka_topics(self) -> bool:
        """Create required Kafka topics"""
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=["localhost:9092"], request_timeout_ms=10000
            )

            topics = []
            for topic_name in self.required_topics:
                topic = NewTopic(
                    name=topic_name, num_partitions=3, replication_factor=1
                )
                topics.append(topic)

            # Create topics
            result = admin_client.create_topics(topics)

            # Wait for creation to complete
            for topic_name, future in result.items():
                try:
                    future.result(timeout=10)
                    logger.info(f"Topic {topic_name} created successfully")
                except TopicAlreadyExistsError:
                    logger.info(f"Topic {topic_name} already exists")
                except Exception as e:
                    logger.error(f"Failed to create topic {topic_name}: {e}")
                    admin_client.close()
                    return False

            admin_client.close()
            return True

        except Exception as e:
            logger.error(f"Failed to create Kafka topics: {e}")
            return False

    def check_sample_data_exists(self) -> bool:
        """Check if sample CSV data exists"""
        data_dir = os.getenv("HOST_DATA_DIR")
        if not data_dir:
            logger.error("HOST_DATA_DIR not configured")
            return False

        raw_dir = os.path.join(data_dir, "raw")
        if not os.path.exists(raw_dir):
            logger.error(f"Raw data directory {raw_dir} does not exist")
            return False

        csv_files = [f for f in os.listdir(raw_dir) if f.endswith(".csv")]
        if not csv_files:
            logger.error(f"No CSV files found in {raw_dir}")
            return False

        logger.info(f"Found {len(csv_files)} CSV files in {raw_dir}")
        return True

    def test_kafka_producer_consumer(self) -> bool:
        """Test Kafka producer and consumer functionality"""
        try:
            topic = "test_readiness"
            test_message = {"test": "readiness_check", "timestamp": time.time()}

            # Create producer
            producer = KafkaProducer(
                bootstrap_servers=["localhost:9092"],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )

            # Send test message
            producer.send(topic, test_message)
            producer.flush()
            producer.close()

            # Create consumer
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=["localhost:9092"],
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=10000,
                auto_offset_reset="earliest",
            )

            # Try to consume the message
            for message in consumer:
                if message.value.get("test") == "readiness_check":
                    consumer.close()
                    logger.info("Kafka producer/consumer test successful")
                    return True

            consumer.close()
            logger.error("Failed to consume test message")
            return False

        except Exception as e:
            logger.error(f"Kafka producer/consumer test failed: {e}")
            return False


# Test fixtures
@pytest.fixture(scope="session")
def readiness_checker():
    """Fixture providing service readiness checker"""
    return ServiceReadinessChecker()


@pytest.fixture(scope="session")
def data_checker():
    """Fixture providing data initialization checker"""
    return DataInitializationChecker()


# Test cases
def test_postgres_startup_and_readiness(readiness_checker):
    """Test PostgreSQL startup and readiness"""
    assert readiness_checker.wait_for_service(
        "postgres", timeout=60
    ), "PostgreSQL service not ready within timeout"


def test_zookeeper_startup_and_readiness(readiness_checker):
    """Test Zookeeper startup and readiness"""
    assert readiness_checker.wait_for_service(
        "zookeeper", timeout=60
    ), "Zookeeper service not ready within timeout"


def test_kafka_startup_and_readiness(readiness_checker):
    """Test Kafka startup and readiness"""
    assert readiness_checker.wait_for_service(
        "kafka", timeout=60
    ), "Kafka service not ready within timeout"


def test_kafka_ui_startup_and_readiness(readiness_checker):
    """Test Kafka UI startup and readiness"""
    assert readiness_checker.wait_for_service(
        "kafka-ui", timeout=60
    ), "Kafka UI service not ready within timeout"


def test_airflow_startup_and_readiness(readiness_checker):
    """Test Airflow startup and readiness"""
    assert readiness_checker.wait_for_service(
        "airflow", timeout=120
    ), "Airflow service not ready within timeout"


def test_all_services_ready(readiness_checker):
    """Test that all services become ready within reasonable time"""
    results = readiness_checker.wait_for_all_services(timeout=300)

    failed_services = [service for service, ready in results.items() if not ready]
    assert not failed_services, f"Services not ready: {failed_services}"


def test_service_dependencies_met(readiness_checker):
    """Test that service dependencies are properly satisfied"""
    unmet_deps = readiness_checker.check_service_dependencies()

    for service, missing_deps in unmet_deps.items():
        assert (
            not missing_deps
        ), f"Service {service} has unmet dependencies: {missing_deps}"


def test_kafka_topics_creation(data_checker):
    """Test Kafka topics can be created"""
    assert data_checker.create_kafka_topics(), "Failed to create required Kafka topics"


def test_sample_data_available(data_checker):
    """Test that sample CSV data is available"""
    assert data_checker.check_sample_data_exists(), "Sample CSV data not available"


def test_kafka_producer_consumer_functionality(data_checker):
    """Test Kafka producer and consumer functionality"""
    assert (
        data_checker.test_kafka_producer_consumer()
    ), "Kafka producer/consumer functionality test failed"


def test_airflow_dags_loaded():
    """Test that Airflow DAGs are loaded properly"""
    try:
        response = requests.get(
            "http://localhost:8080/api/v1/dags", auth=("admin", "admin"), timeout=10
        )

        assert (
            response.status_code == 200
        ), f"Failed to get DAGs: {response.status_code}"

        dags_data = response.json()
        dag_ids = [dag["dag_id"] for dag in dags_data.get("dags", [])]

        # Check for expected DAGs
        expected_dags = [
            "sales_data_pipeline",
            "maintenance_dag",
            "pipeline_monitoring_dag",
        ]
        missing_dags = [dag for dag in expected_dags if dag not in dag_ids]

        assert not missing_dags, f"Missing expected DAGs: {missing_dags}"

    except Exception as e:
        pytest.fail(f"Failed to check Airflow DAGs: {e}")


def test_docker_containers_healthy():
    """Test that all Docker containers are in healthy state"""
    client = docker.from_env()
    containers = client.containers.list()

    unhealthy_containers = []
    for container in containers:
        health = container.attrs.get("State", {}).get("Health", {})
        if health and health.get("Status") == "unhealthy":
            unhealthy_containers.append(container.name)

    assert (
        not unhealthy_containers
    ), f"Unhealthy containers found: {unhealthy_containers}"


def test_system_resources_adequate():
    """Test that system has adequate resources for all services"""
    import psutil

    # Check memory usage
    memory = psutil.virtual_memory()
    assert memory.percent < 90, f"System memory usage too high: {memory.percent}%"

    # Check CPU usage
    cpu_percent = psutil.cpu_percent(interval=1)
    assert cpu_percent < 95, f"System CPU usage too high: {cpu_percent}%"

    # Check disk usage
    disk = psutil.disk_usage("/")
    assert disk.percent < 90, f"System disk usage too high: {disk.percent}%"


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
