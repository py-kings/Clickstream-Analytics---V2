"""
Spark session initialization with Delta Lake, S3, and streaming support.
"""
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
)
from src.config import config


logger = logging.getLogger(__name__)


def build_spark_session(app_name: str = "ClickstreamAnalytics") -> SparkSession:
    """
    Build Spark session with Delta Lake, S3, and optimizations.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", 
                "io.delta:delta-spark_2.12:3.1.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.master", config.spark.master)
        .config("spark.sql.shuffle.partitions", config.spark.shuffle_partitions)
        .config("spark.sql.streaming.checkpointLocation", config.delta.checkpoint_path)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    # AWS S3 credentials
    if config.aws.access_key_id and config.aws.secret_access_key:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.access.key", config.aws.access_key_id)
            .config("spark.hadoop.fs.s3a.secret.key", config.aws.secret_access_key)
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        )
    else:
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )

    builder = builder.config("spark.hadoop.fs.s3a.endpoint.region", config.aws.region)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"✓ Spark session initialized: {app_name}")
    return spark


def get_events_schema() -> StructType:
    """Kafka clickstream events schema (Retail Rocket format)."""
    return StructType([
        StructField("timestamp", LongType(), nullable=False),
        StructField("visitorid", LongType(), nullable=False),
        StructField("event", StringType(), nullable=False),
        StructField("itemid", LongType(), nullable=False),
        StructField("transactionid", LongType(), nullable=True),
    ])


def ensure_delta_tables(spark: SparkSession):
    """Ensure Delta Lake paths exist with proper structure (local only)."""
    import os
    paths = [
        config.delta.bronze_path,
        config.delta.silver_path,
        config.delta.gold_path,
    ]
    
    # Only create local paths; S3 paths don't need mkdir
    for path in paths:
        if not path.startswith("s3a://") and not path.startswith("s3://"):
            os.makedirs(path, exist_ok=True)
    
    logger.info("✓ Delta Lake paths ensured")


if __name__ == "__main__":
    spark = build_spark_session("test")
    ensure_delta_tables(spark)
    print(f"Spark version: {spark.version}")
    spark.stop()

