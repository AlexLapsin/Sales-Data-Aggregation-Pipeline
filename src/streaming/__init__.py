"""
Streaming Module

Kafka-based streaming components for real-time data processing.
"""

from .producers import KafkaSalesProducer, SalesDataGenerator

__all__ = ["KafkaSalesProducer", "SalesDataGenerator"]
