# tests/conftest.py
import sys
import os
import pytest
import logging
from pathlib import Path

# Configure logging for tests
logging.basicConfig(level=logging.INFO)

# Add the 'src' directory to sys.path so tests can import modules directly
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
sys.path.insert(0, SRC_PATH)

# Load environment variables for tests
from dotenv import load_dotenv

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))


@pytest.fixture(scope="session")
def project_root():
    """Provide project root directory path"""
    return Path(PROJECT_ROOT)


@pytest.fixture(scope="session")
def test_data_dir(project_root):
    """Provide test data directory path"""
    return project_root / "data" / "raw"


@pytest.fixture(scope="session")
def docker_compose_file(project_root):
    """Provide docker-compose file path"""
    return project_root / "docker-compose.yml"


@pytest.fixture(autouse=True)
def setup_test_environment():
    """Set up test environment before each test"""
    # Ensure we're using test-specific configurations
    os.environ["TESTING"] = "true"
    yield
    # Cleanup after test if needed


def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "unit: marks tests as unit tests")
    config.addinivalue_line("markers", "kafka: marks tests that require Kafka")
    config.addinivalue_line("markers", "aws: marks tests that require AWS access")
    config.addinivalue_line("markers", "docker: marks tests that require Docker")


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers automatically"""
    for item in items:
        # Add markers based on test file names
        if "kafka" in item.nodeid:
            item.add_marker(pytest.mark.kafka)
        if "docker" in item.nodeid:
            item.add_marker(pytest.mark.docker)
        if "config" in item.nodeid and "aws" in item.nodeid.lower():
            item.add_marker(pytest.mark.aws)
        if "communication" in item.nodeid or "readiness" in item.nodeid:
            item.add_marker(pytest.mark.integration)
        if "transform" in item.nodeid:
            item.add_marker(pytest.mark.unit)
