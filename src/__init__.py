"""
Sales Data Aggregation Pipeline

A modern cloud-native ETL pipeline for aggregating regional sales data.
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

# Modern pipeline module exports
from . import spark
from . import streaming
from . import utils

# Import key classes and functions for convenience
from .streaming import KafkaSalesProducer, SalesDataGenerator
from .spark import SparkConfig
from .utils import ConfigValidator, SetupDoctor

__all__ = [
    # Version info
    "__version__",
    "__author__",
    # Modern submodules
    "spark",
    "streaming",
    "utils",
    # Streaming
    "KafkaSalesProducer",
    "SalesDataGenerator",
    # Spark
    "SparkConfig",
    # Utils
    "ConfigValidator",
    "SetupDoctor",
]
