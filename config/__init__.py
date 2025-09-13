"""
Configuration Module

Centralized configuration management for all environments and platforms.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional

CONFIG_DIR = Path(__file__).parent
ENVIRONMENTS_FILE = CONFIG_DIR / "environments.yaml"
LOGGING_FILE = CONFIG_DIR / "logging.yaml"


def load_environment_config(env: Optional[str] = None) -> Dict[str, Any]:
    """Load environment-specific configuration."""
    if env is None:
        env = os.getenv("ENVIRONMENT", "development")

    with open(ENVIRONMENTS_FILE, "r") as f:
        configs = yaml.safe_load(f)

    return configs.get(env, configs["development"])


def load_logging_config() -> Dict[str, Any]:
    """Load logging configuration."""
    with open(LOGGING_FILE, "r") as f:
        return yaml.safe_load(f)


__all__ = ["load_environment_config", "load_logging_config"]
