"""Shared pytest fixtures: a local Spark session and a raw-event sample."""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType

RAW_EVENT_SCHEMA = StructType([
    StructField("timestamp", LongType(), False),
    StructField("visitorid", LongType(), False),
    StructField("event", StringType(), False),
    StructField("itemid", LongType(), False),
    StructField("transactionid", LongType(), True),
])


@pytest.fixture(scope="session")
def spark():
    """Local Spark session shared by all tests."""
    session = (
        SparkSession.builder
        .appName("test-clickstream")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def raw_events_df(spark):
    """Five raw events (Kafka payload shape) for two visitors."""
    data = [
        (1000000, 1001, "view", 5001, None),
        (1001000, 1001, "view", 5002, None),
        (1002000, 1001, "addtocart", 5002, None),
        (1003000, 1001, "transaction", 5002, 12345),
        (2000000, 1002, "view", 5003, None),
    ]
    return spark.createDataFrame(data, schema=RAW_EVENT_SCHEMA)
