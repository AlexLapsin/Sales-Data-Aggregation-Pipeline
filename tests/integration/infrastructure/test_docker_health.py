#!/usr/bin/env python3
"""
Docker Container Health and Connectivity Tests

This module provides comprehensive tests for validating Docker container health,
service connectivity, and basic system readiness in the sales data pipeline.
"""

import subprocess
import time
import json
import requests
import psycopg2
import logging
from typing import Dict, List, Optional
import pytest
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import docker


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DockerHealthChecker:
    """Manages Docker container health checks and service validation"""

    def __init__(self):
        self.client = docker.from_env()
        self.expected_services = [
            "postgres",
            "etl-pipeline",
            "zookeeper",
            "kafka",
            "kafka-ui",
            "airflow",
        ]

    def get_container_status(self, service_name: str) -> Dict:
        """Get detailed status information for a specific container"""
        try:
            containers = self.client.containers.list(all=True)
            for container in containers:
                if service_name in container.name or any(
                    service_name in tag for tag in container.image.tags
                ):
                    return {
                        "name": container.name,
                        "status": container.status,
                        "health": getattr(
                            container.attrs.get("State", {}), "Health", {}
                        ).get("Status", "unknown"),
                        "ports": container.ports,
                        "image": container.image.tags[0]
                        if container.image.tags
                        else "unknown",
                    }
            return {"error": f"Container {service_name} not found"}
        except Exception as e:
            return {"error": str(e)}

    def wait_for_healthy_containers(self, timeout: int = 300) -> bool:
        """Wait for all expected containers to be healthy"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            all_healthy = True

            for service in self.expected_services:
                status = self.get_container_status(service)
                if "error" in status or status.get("status") != "running":
                    all_healthy = False
                    break

            if all_healthy:
                logger.info("All containers are running and healthy")
                return True

            time.sleep(10)

        logger.error(f"Containers not healthy after {timeout} seconds")
        return False


class ServiceConnectivityTester:
    """Tests connectivity between services and external dependencies"""

    @staticmethod
    def test_postgres_connection(
        host: str = "localhost",
        port: int = 5432,
        database: str = "airflow",
        user: str = "airflow",
        password: str = "airflow",
    ) -> bool:
        """Test PostgreSQL database connectivity"""
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                connect_timeout=10,
            )
            conn.close()
            logger.info("PostgreSQL connection successful")
            return True
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            return False

    @staticmethod
    def test_airflow_webserver(url: str = "http://localhost:8080") -> bool:
        """Test Airflow webserver availability"""
        try:
            response = requests.get(f"{url}/health", timeout=10)
            if response.status_code == 200:
                logger.info("Airflow webserver is accessible")
                return True
            else:
                logger.error(
                    f"Airflow webserver returned status {response.status_code}"
                )
                return False
        except Exception as e:
            logger.error(f"Airflow webserver connection failed: {e}")
            return False

    @staticmethod
    def test_kafka_connectivity(bootstrap_servers: str = "localhost:9092") -> bool:
        """Test Kafka broker connectivity"""
        try:
            # Test producer
            producer = KafkaProducer(
                bootstrap_servers=[bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=10000,
                api_version=(0, 10, 1),
            )

            # Test consumer
            consumer = KafkaConsumer(
                bootstrap_servers=[bootstrap_servers],
                consumer_timeout_ms=5000,
                api_version=(0, 10, 1),
            )

            producer.close()
            consumer.close()
            logger.info("Kafka connectivity test successful")
            return True
        except Exception as e:
            logger.error(f"Kafka connectivity test failed: {e}")
            return False

    @staticmethod
    def test_kafka_ui(url: str = "http://localhost:8090") -> bool:
        """Test Kafka UI accessibility"""
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                logger.info("Kafka UI is accessible")
                return True
            else:
                logger.error(f"Kafka UI returned status {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Kafka UI connection failed: {e}")
            return False


# Test fixtures and test cases
@pytest.fixture(scope="session")
def docker_health_checker():
    """Fixture providing Docker health checker instance"""
    return DockerHealthChecker()


@pytest.fixture(scope="session")
def connectivity_tester():
    """Fixture providing service connectivity tester instance"""
    return ServiceConnectivityTester()


def test_docker_compose_services_running(docker_health_checker):
    """Test that all expected Docker Compose services are running"""
    for service in docker_health_checker.expected_services:
        status = docker_health_checker.get_container_status(service)
        assert "error" not in status, f"Service {service} not found or error occurred"
        assert (
            status.get("status") == "running"
        ), f"Service {service} is not running: {status.get('status')}"


def test_postgres_service_health(connectivity_tester):
    """Test PostgreSQL service health and connectivity"""
    assert (
        connectivity_tester.test_postgres_connection()
    ), "PostgreSQL connection failed"


def test_airflow_service_health(connectivity_tester):
    """Test Airflow webserver health and accessibility"""
    assert (
        connectivity_tester.test_airflow_webserver()
    ), "Airflow webserver not accessible"


def test_kafka_service_health(connectivity_tester):
    """Test Kafka broker health and connectivity"""
    assert (
        connectivity_tester.test_kafka_connectivity()
    ), "Kafka connectivity test failed"


def test_kafka_ui_accessibility(connectivity_tester):
    """Test Kafka UI accessibility"""
    assert connectivity_tester.test_kafka_ui(), "Kafka UI not accessible"


def test_container_resource_usage(docker_health_checker):
    """Test that containers are not consuming excessive resources"""
    client = docker_health_checker.client

    for container in client.containers.list():
        stats = container.stats(stream=False)

        # Check memory usage (should be less than 1GB for most services)
        memory_usage = stats["memory_stats"].get("usage", 0)
        memory_limit = stats["memory_stats"].get("limit", float("inf"))
        memory_percent = (
            (memory_usage / memory_limit) * 100 if memory_limit != float("inf") else 0
        )

        assert (
            memory_percent < 90
        ), f"Container {container.name} using {memory_percent:.1f}% memory"

        # Check if container is responsive (not in zombie state)
        assert (
            container.status == "running"
        ), f"Container {container.name} not running: {container.status}"


def test_docker_network_connectivity(docker_health_checker):
    """Test inter-container network connectivity"""
    client = docker_health_checker.client
    networks = client.networks.list()

    # Find the project network
    project_network = None
    for network in networks:
        if "sales_data_aggregation_pipeline" in network.name:
            project_network = network
            break

    assert project_network is not None, "Project Docker network not found"

    # Check that containers are connected to the network
    connected_containers = [
        container["Name"] for container in project_network.attrs["Containers"].values()
    ]

    # At minimum, postgres and airflow should be connected
    expected_in_network = ["postgres", "airflow"]
    for expected in expected_in_network:
        assert any(
            expected in name for name in connected_containers
        ), f"Container {expected} not found in project network"


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
