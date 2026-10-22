"""
Clickstream Analytics Platform v2
Real-time streaming pipeline: Kafka → Spark SS → Delta Lake → Analytics
"""

__version__ = "2.0.0"
__author__ = "Data Engineering Team"

# Expose key components
from src.config import config
from src.spark_utils import build_spark_session, get_events_schema

__all__ = [
    "config",
    "build_spark_session",
    "get_events_schema",
]
