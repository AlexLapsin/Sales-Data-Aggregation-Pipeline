#!/usr/bin/env python3
"""
Infrastructure Management for End-to-End Testing

This module manages the setup, configuration, and teardown of test infrastructure
including Docker containers, databases, message queues, and storage services.
"""

import asyncio
import docker
import json
import logging
import os
import subprocess
import tempfile
import time
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import boto3
from botocore.exceptions import ClientError
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import requests


class InfrastructureManager:
    """Manages test infrastructure setup and teardown"""

    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Docker client
        try:
            self.docker_client = docker.from_env()
        except Exception as e:
            self.logger.warning(f"Failed to initialize Docker client: {e}")
            self.docker_client = None

        # Track created resources for cleanup
        self.created_containers = []
        self.created_networks = []
        self.created_volumes = []
        self.created_databases = []
        self.created_topics = []
        self.created_buckets = []

        # Service status tracking
        self.service_status = {}

    async def start_services(self) -> None:
        """Start all required test infrastructure services"""

        self.logger.info("Starting test infrastructure services")

        # Create Docker network for services
        await self._create_docker_network()

        # Start services in dependency order
        await self._start_zookeeper()
        await self._start_kafka()
        await self._start_postgres()
        await self._start_minio()

        self.logger.info("All infrastructure services started")

    async def _create_docker_network(self) -> None:
        """Create Docker network for test services"""

        if not self.docker_client:
            self.logger.warning(
                "Docker client not available, skipping network creation"
            )
            return

        network_name = self.config.infrastructure.test_network_name

        try:
            # Check if network already exists
            existing_networks = self.docker_client.networks.list(names=[network_name])
            if existing_networks:
                self.logger.info(f"Docker network already exists: {network_name}")
                return

            # Create network
            network = self.docker_client.networks.create(
                name=network_name, driver="bridge"
            )

            self.created_networks.append(network_name)
            self.logger.info(f"Created Docker network: {network_name}")

        except Exception as e:
            self.logger.error(f"Failed to create Docker network: {e}")
            raise

    async def _start_zookeeper(self) -> None:
        """Start Zookeeper container"""

        if not self.docker_client:
            self.logger.warning("Docker client not available, skipping Zookeeper")
            return

        container_name = self.config.infrastructure.zookeeper_container_name

        try:
            # Check if container already running
            try:
                existing_container = self.docker_client.containers.get(container_name)
                if existing_container.status == "running":
                    self.logger.info(
                        f"Zookeeper container already running: {container_name}"
                    )
                    return
                else:
                    # Remove stopped container
                    existing_container.remove()
            except docker.errors.NotFound:
                pass

            # Start Zookeeper container
            container = self.docker_client.containers.run(
                image="confluentinc/cp-zookeeper:7.4.0",
                name=container_name,
                environment={
                    "ZOOKEEPER_CLIENT_PORT": "2181",
                    "ZOOKEEPER_TICK_TIME": "2000",
                },
                ports={"2181/tcp": "22181"},
                network=self.config.infrastructure.test_network_name,
                detach=True,
                remove=False,
            )

            self.created_containers.append(container_name)
            self.service_status["zookeeper"] = "starting"

            # Wait for Zookeeper to be ready
            await self._wait_for_port("localhost", 22181, timeout=60)
            self.service_status["zookeeper"] = "running"

            self.logger.info(f"Started Zookeeper container: {container_name}")

        except Exception as e:
            self.logger.error(f"Failed to start Zookeeper: {e}")
            raise

    async def _start_kafka(self) -> None:
        """Start Kafka container"""

        if not self.docker_client:
            self.logger.warning("Docker client not available, skipping Kafka")
            return

        container_name = self.config.infrastructure.kafka_container_name
        kafka_port = self.config.infrastructure.kafka_port

        try:
            # Check if container already running
            try:
                existing_container = self.docker_client.containers.get(container_name)
                if existing_container.status == "running":
                    self.logger.info(
                        f"Kafka container already running: {container_name}"
                    )
                    return
                else:
                    existing_container.remove()
            except docker.errors.NotFound:
                pass

            # Start Kafka container
            container = self.docker_client.containers.run(
                image="confluentinc/cp-kafka:7.4.0",
                name=container_name,
                environment={
                    "KAFKA_BROKER_ID": "1",
                    "KAFKA_ZOOKEEPER_CONNECT": f"{self.config.infrastructure.zookeeper_container_name}:2181",
                    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
                    "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://{container_name}:9092,PLAINTEXT_HOST://localhost:{kafka_port}",
                    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
                    "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
                    "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
                    "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "true",
                },
                ports={f"9092/tcp": str(kafka_port)},
                network=self.config.infrastructure.test_network_name,
                detach=True,
                remove=False,
            )

            self.created_containers.append(container_name)
            self.service_status["kafka"] = "starting"

            # Wait for Kafka to be ready
            await self._wait_for_kafka(f"localhost:{kafka_port}", timeout=120)
            self.service_status["kafka"] = "running"

            self.logger.info(f"Started Kafka container: {container_name}")

        except Exception as e:
            self.logger.error(f"Failed to start Kafka: {e}")
            raise

    async def _start_postgres(self) -> None:
        """Start PostgreSQL container"""

        if not self.docker_client:
            self.logger.warning("Docker client not available, skipping PostgreSQL")
            return

        container_name = self.config.infrastructure.postgres_container_name
        postgres_port = self.config.infrastructure.postgres_port

        try:
            # Check if container already running
            try:
                existing_container = self.docker_client.containers.get(container_name)
                if existing_container.status == "running":
                    self.logger.info(
                        f"PostgreSQL container already running: {container_name}"
                    )
                    return
                else:
                    existing_container.remove()
            except docker.errors.NotFound:
                pass

            # Start PostgreSQL container
            container = self.docker_client.containers.run(
                image="postgres:15",
                name=container_name,
                environment={
                    "POSTGRES_DB": self.config.infrastructure.test_db_name,
                    "POSTGRES_USER": self.config.infrastructure.test_db_user,
                    "POSTGRES_PASSWORD": self.config.infrastructure.test_db_password,
                },
                ports={f"5432/tcp": str(postgres_port)},
                network=self.config.infrastructure.test_network_name,
                detach=True,
                remove=False,
            )

            self.created_containers.append(container_name)
            self.service_status["postgres"] = "starting"

            # Wait for PostgreSQL to be ready
            await self._wait_for_postgres(
                host="localhost",
                port=postgres_port,
                database=self.config.infrastructure.test_db_name,
                user=self.config.infrastructure.test_db_user,
                password=self.config.infrastructure.test_db_password,
                timeout=60,
            )
            self.service_status["postgres"] = "running"

            self.logger.info(f"Started PostgreSQL container: {container_name}")

        except Exception as e:
            self.logger.error(f"Failed to start PostgreSQL: {e}")
            raise

    async def _start_minio(self) -> None:
        """Start MinIO container for S3-compatible storage"""

        if not self.docker_client:
            self.logger.warning("Docker client not available, skipping MinIO")
            return

        container_name = "test-minio"
        minio_port = self.config.infrastructure.minio_port

        try:
            # Check if container already running
            try:
                existing_container = self.docker_client.containers.get(container_name)
                if existing_container.status == "running":
                    self.logger.info(
                        f"MinIO container already running: {container_name}"
                    )
                    return
                else:
                    existing_container.remove()
            except docker.errors.NotFound:
                pass

            # Start MinIO container
            container = self.docker_client.containers.run(
                image="minio/minio:RELEASE.2023-07-07T07-13-57Z",
                name=container_name,
                environment={
                    "MINIO_ROOT_USER": "minioadmin",
                    "MINIO_ROOT_PASSWORD": "minioadmin",
                },
                ports={
                    f"9000/tcp": str(minio_port),
                    f"9001/tcp": str(minio_port + 1),
                },
                command="server /data --console-address ':9001'",
                network=self.config.infrastructure.test_network_name,
                detach=True,
                remove=False,
            )

            self.created_containers.append(container_name)
            self.service_status["minio"] = "starting"

            # Wait for MinIO to be ready
            await self._wait_for_minio(f"localhost:{minio_port}", timeout=60)
            self.service_status["minio"] = "running"

            self.logger.info(f"Started MinIO container: {container_name}")

        except Exception as e:
            self.logger.error(f"Failed to start MinIO: {e}")
            raise

    async def wait_for_readiness(self, timeout: float = 180) -> None:
        """Wait for all services to be ready"""

        self.logger.info("Waiting for services to be ready")

        start_time = time.time()

        while time.time() - start_time < timeout:
            all_ready = True

            for service, status in self.service_status.items():
                if status != "running":
                    all_ready = False
                    break

            if all_ready:
                self.logger.info("All services are ready")
                return

            await asyncio.sleep(2)

        # Check which services are not ready
        not_ready = [
            service
            for service, status in self.service_status.items()
            if status != "running"
        ]
        raise TimeoutError(f"Services not ready after {timeout}s: {not_ready}")

    async def setup_databases(self) -> None:
        """Setup test databases and schemas"""

        self.logger.info("Setting up test databases")

        # Setup PostgreSQL tables
        await self._setup_postgres_schema()

        self.logger.info("Test databases setup complete")

    async def _setup_postgres_schema(self) -> None:
        """Setup PostgreSQL test schema"""

        db_config = self.config.get_database_config()

        try:
            # Connect to PostgreSQL
            conn = psycopg2.connect(
                host=db_config["host"],
                port=db_config["port"],
                database=db_config["database"],
                user=db_config["user"],
                password=db_config["password"],
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            cursor = conn.cursor()

            # Create test tables for pipeline validation
            create_tables_sql = """
            -- Fact sales table
            CREATE TABLE IF NOT EXISTS fact_sales (
                order_id VARCHAR(255),
                store_id VARCHAR(255),
                product_id VARCHAR(255),
                product_name VARCHAR(255),
                category VARCHAR(255),
                quantity INTEGER,
                unit_price DECIMAL(10,2),
                total_price DECIMAL(10,2),
                order_date DATE,
                ship_date DATE,
                sales DECIMAL(10,2),
                profit DECIMAL(10,2),
                customer_segment VARCHAR(255),
                region VARCHAR(255),
                country VARCHAR(255),
                state VARCHAR(255),
                city VARCHAR(255),
                batch_id VARCHAR(255),
                ingestion_timestamp TIMESTAMP,
                source_system VARCHAR(255),
                partition_date DATE
            );

            -- Staging table for raw data
            CREATE TABLE IF NOT EXISTS sales_raw (
                id SERIAL PRIMARY KEY,
                data JSONB,
                ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                partition_date DATE DEFAULT CURRENT_DATE
            );

            -- Test data quality table
            CREATE TABLE IF NOT EXISTS data_quality_metrics (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(255),
                metric_name VARCHAR(255),
                metric_value DECIMAL(10,4),
                measurement_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                run_id VARCHAR(255)
            );
            """

            cursor.execute(create_tables_sql)

            cursor.close()
            conn.close()

            self.created_databases.append("postgres_schema")
            self.logger.info("PostgreSQL test schema created")

        except Exception as e:
            self.logger.error(f"Failed to setup PostgreSQL schema: {e}")
            raise

    async def setup_kafka_topics(self) -> None:
        """Setup Kafka topics for testing"""

        self.logger.info("Setting up Kafka topics")

        kafka_config = self.config.get_kafka_config()
        bootstrap_servers = kafka_config["bootstrap_servers"]
        topic_name = kafka_config["topic"]

        try:
            # Create Kafka admin client
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers, client_id="test_admin"
            )

            # Create test topic
            topic = NewTopic(name=topic_name, num_partitions=3, replication_factor=1)

            try:
                admin_client.create_topics([topic])
                self.created_topics.append(topic_name)
                self.logger.info(f"Created Kafka topic: {topic_name}")
            except TopicAlreadyExistsError:
                self.logger.info(f"Kafka topic already exists: {topic_name}")

            admin_client.close()

        except Exception as e:
            self.logger.error(f"Failed to setup Kafka topics: {e}")
            raise

    async def setup_s3_buckets(self) -> None:
        """Setup S3/MinIO buckets for testing"""

        self.logger.info("Setting up S3 buckets")

        s3_config = self.config.get_s3_config()

        try:
            # Create S3 client for MinIO
            s3_client = boto3.client(
                "s3",
                endpoint_url=s3_config["endpoint_url"],
                aws_access_key_id=s3_config["access_key"],
                aws_secret_access_key=s3_config["secret_key"],
                region_name="us-east-1",
            )

            # Create test buckets
            buckets = [s3_config["bucket_name"], s3_config["processed_bucket_name"]]

            for bucket_name in buckets:
                try:
                    s3_client.create_bucket(Bucket=bucket_name)
                    self.created_buckets.append(bucket_name)
                    self.logger.info(f"Created S3 bucket: {bucket_name}")
                except ClientError as e:
                    if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
                        self.logger.info(f"S3 bucket already exists: {bucket_name}")
                    else:
                        raise

        except Exception as e:
            self.logger.error(f"Failed to setup S3 buckets: {e}")
            raise

    async def upload_raw_data(self, file_path: str) -> str:
        """Upload raw data file to S3 bucket"""

        s3_config = self.config.get_s3_config()
        bucket_name = s3_config["bucket_name"]

        try:
            s3_client = boto3.client(
                "s3",
                endpoint_url=s3_config["endpoint_url"],
                aws_access_key_id=s3_config["access_key"],
                aws_secret_access_key=s3_config["secret_key"],
                region_name="us-east-1",
            )

            # Upload file
            file_name = Path(file_path).name
            s3_key = f"raw-data/{file_name}"

            s3_client.upload_file(file_path, bucket_name, s3_key)

            s3_url = f"s3://{bucket_name}/{s3_key}"
            self.logger.info(f"Uploaded raw data to: {s3_url}")

            return s3_url

        except Exception as e:
            self.logger.error(f"Failed to upload raw data: {e}")
            raise

    # Service health check methods

    async def _wait_for_port(self, host: str, port: int, timeout: float = 60) -> None:
        """Wait for a port to be available"""

        import socket

        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                result = sock.connect_ex((host, port))
                sock.close()

                if result == 0:
                    return

            except Exception:
                pass

            await asyncio.sleep(1)

        raise TimeoutError(f"Port {host}:{port} not available after {timeout}s")

    async def _wait_for_kafka(
        self, bootstrap_servers: str, timeout: float = 120
    ) -> None:
        """Wait for Kafka to be ready"""

        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                # Try to create a producer to test connectivity
                producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    request_timeout_ms=5000,
                    retries=0,
                )
                producer.close()
                return

            except Exception:
                await asyncio.sleep(2)

        raise TimeoutError(f"Kafka not ready after {timeout}s")

    async def _wait_for_postgres(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        timeout: float = 60,
    ) -> None:
        """Wait for PostgreSQL to be ready"""

        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                conn = psycopg2.connect(
                    host=host,
                    port=port,
                    database=database,
                    user=user,
                    password=password,
                    connect_timeout=5,
                )
                conn.close()
                return

            except Exception:
                await asyncio.sleep(2)

        raise TimeoutError(f"PostgreSQL not ready after {timeout}s")

    async def _wait_for_minio(self, endpoint_url: str, timeout: float = 60) -> None:
        """Wait for MinIO to be ready"""

        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                response = requests.get(
                    f"http://{endpoint_url}/minio/health/live", timeout=5
                )
                if response.status_code == 200:
                    return

            except Exception:
                await asyncio.sleep(2)

        raise TimeoutError(f"MinIO not ready after {timeout}s")

    # Cleanup methods

    async def cleanup(self) -> None:
        """Clean up all created infrastructure resources"""

        self.logger.info("Starting infrastructure cleanup")

        # Stop and remove containers
        await self._cleanup_containers()

        # Remove networks
        await self._cleanup_networks()

        # Clean up databases
        await self._cleanup_databases()

        # Clean up topics
        await self._cleanup_topics()

        # Clean up buckets
        await self._cleanup_buckets()

        self.logger.info("Infrastructure cleanup complete")

    async def _cleanup_containers(self) -> None:
        """Stop and remove created containers"""

        if not self.docker_client:
            return

        for container_name in self.created_containers:
            try:
                container = self.docker_client.containers.get(container_name)

                if container.status == "running":
                    container.stop(timeout=10)
                    self.logger.info(f"Stopped container: {container_name}")

                container.remove()
                self.logger.info(f"Removed container: {container_name}")

            except docker.errors.NotFound:
                self.logger.debug(f"Container not found: {container_name}")
            except Exception as e:
                self.logger.warning(
                    f"Failed to cleanup container {container_name}: {e}"
                )

        self.created_containers.clear()

    async def _cleanup_networks(self) -> None:
        """Remove created networks"""

        if not self.docker_client:
            return

        for network_name in self.created_networks:
            try:
                network = self.docker_client.networks.get(network_name)
                network.remove()
                self.logger.info(f"Removed network: {network_name}")

            except docker.errors.NotFound:
                self.logger.debug(f"Network not found: {network_name}")
            except Exception as e:
                self.logger.warning(f"Failed to cleanup network {network_name}: {e}")

        self.created_networks.clear()

    async def _cleanup_databases(self) -> None:
        """Clean up created databases and schemas"""

        for db_name in self.created_databases:
            try:
                if db_name == "postgres_schema":
                    # Drop test tables
                    db_config = self.config.get_database_config()

                    conn = psycopg2.connect(
                        host=db_config["host"],
                        port=db_config["port"],
                        database=db_config["database"],
                        user=db_config["user"],
                        password=db_config["password"],
                    )
                    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

                    cursor = conn.cursor()

                    drop_tables_sql = """
                    DROP TABLE IF EXISTS fact_sales;
                    DROP TABLE IF EXISTS sales_raw;
                    DROP TABLE IF EXISTS data_quality_metrics;
                    """

                    cursor.execute(drop_tables_sql)

                    cursor.close()
                    conn.close()

                    self.logger.info("Cleaned up PostgreSQL test schema")

            except Exception as e:
                self.logger.warning(f"Failed to cleanup database {db_name}: {e}")

        self.created_databases.clear()

    async def _cleanup_topics(self) -> None:
        """Clean up created Kafka topics"""

        if not self.created_topics:
            return

        try:
            kafka_config = self.config.get_kafka_config()
            bootstrap_servers = kafka_config["bootstrap_servers"]

            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers, client_id="test_admin"
            )

            # Delete topics
            admin_client.delete_topics(self.created_topics)
            admin_client.close()

            self.logger.info(f"Deleted Kafka topics: {self.created_topics}")

        except Exception as e:
            self.logger.warning(f"Failed to cleanup Kafka topics: {e}")

        self.created_topics.clear()

    async def _cleanup_buckets(self) -> None:
        """Clean up created S3 buckets"""

        if not self.created_buckets:
            return

        try:
            s3_config = self.config.get_s3_config()

            s3_client = boto3.client(
                "s3",
                endpoint_url=s3_config["endpoint_url"],
                aws_access_key_id=s3_config["access_key"],
                aws_secret_access_key=s3_config["secret_key"],
                region_name="us-east-1",
            )

            for bucket_name in self.created_buckets:
                try:
                    # Delete all objects in bucket first
                    response = s3_client.list_objects_v2(Bucket=bucket_name)
                    if "Contents" in response:
                        objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
                        s3_client.delete_objects(
                            Bucket=bucket_name, Delete={"Objects": objects}
                        )

                    # Delete bucket
                    s3_client.delete_bucket(Bucket=bucket_name)
                    self.logger.info(f"Deleted S3 bucket: {bucket_name}")

                except ClientError as e:
                    if e.response["Error"]["Code"] != "NoSuchBucket":
                        self.logger.warning(
                            f"Failed to delete bucket {bucket_name}: {e}"
                        )

        except Exception as e:
            self.logger.warning(f"Failed to cleanup S3 buckets: {e}")

        self.created_buckets.clear()

    def get_service_status(self) -> Dict[str, str]:
        """Get current status of all services"""
        return self.service_status.copy()

    def is_service_running(self, service_name: str) -> bool:
        """Check if a specific service is running"""
        return self.service_status.get(service_name) == "running"

    def are_all_services_running(self) -> bool:
        """Check if all services are running"""
        return all(status == "running" for status in self.service_status.values())


class DockerComposeManager:
    """Alternative infrastructure manager using Docker Compose"""

    def __init__(self, config, compose_file_path: Optional[str] = None):
        self.config = config
        self.logger = logging.getLogger(__name__)

        if compose_file_path:
            self.compose_file = Path(compose_file_path)
        else:
            # Generate compose file
            self.compose_file = self._generate_compose_file()

    def _generate_compose_file(self) -> Path:
        """Generate Docker Compose file for test infrastructure"""

        from .test_data_generator import MockComponentGenerator

        mock_generator = MockComponentGenerator()
        compose_config = mock_generator.generate_docker_compose_config()

        # Write compose file
        compose_path = Path("tests/integration/docker-compose.test.yml")
        compose_path.parent.mkdir(parents=True, exist_ok=True)

        with open(compose_path, "w") as f:
            yaml.dump(compose_config, f, default_flow_style=False)

        self.logger.info(f"Generated Docker Compose file: {compose_path}")
        return compose_path

    async def start_services(self) -> None:
        """Start services using Docker Compose"""

        self.logger.info("Starting services with Docker Compose")

        try:
            # Run docker-compose up
            cmd = ["docker-compose", "-f", str(self.compose_file), "up", "-d"]
            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                self.logger.info("Docker Compose services started successfully")
            else:
                raise Exception(f"Docker Compose failed: {stderr.decode()}")

        except Exception as e:
            self.logger.error(f"Failed to start Docker Compose services: {e}")
            raise

    async def stop_services(self) -> None:
        """Stop services using Docker Compose"""

        self.logger.info("Stopping services with Docker Compose")

        try:
            # Run docker-compose down
            cmd = ["docker-compose", "-f", str(self.compose_file), "down", "-v"]
            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                self.logger.info("Docker Compose services stopped successfully")
            else:
                self.logger.warning(f"Docker Compose stop warning: {stderr.decode()}")

        except Exception as e:
            self.logger.error(f"Failed to stop Docker Compose services: {e}")
            raise
