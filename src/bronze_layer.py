"""
Bronze Layer: Raw event ingestion from Kafka with deduplication and schema enforcement.
Stores all events in Delta format.
"""
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json,
    col,
    current_timestamp,
    md5,
    concat_ws,
)
from src.config import config
from src.spark_utils import get_events_schema


logger = logging.getLogger(__name__)


class BronzeLayer:
    """Raw event ingestion from Kafka."""

    def __init__(self, spark: SparkSession, starting_offsets: str = "latest"):
        self.spark = spark
        self.kafka_config = config.kafka
        self.delta_config = config.delta
        self.schema = get_events_schema()
        # "latest" for a live stream; "earliest" for a bounded batch that must see
        # events produced before the job started. Ignored once a checkpoint exists.
        self.starting_offsets = starting_offsets

    def read_kafka_stream(self) -> DataFrame:
        """Read raw events from Kafka."""
        logger.info(f"Reading from Kafka: {self.kafka_config.topic_events}")

        df = (
            self.spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_config.bootstrap_servers)
            .option("subscribe", self.kafka_config.topic_events)
            .option("startingOffsets", self.starting_offsets)
            .option("failOnDataLoss", "false")
            .load()
        )

        # Parse JSON value
        df = df.select(
            from_json(col("value").cast("string"), self.schema).alias("event"),
            col("timestamp").alias("kafka_timestamp"),
        ).select("event.*", "kafka_timestamp")

        return df

    def add_bronze_metadata(self, df: DataFrame) -> DataFrame:
        """Add ingestion metadata."""
        df = (
            df
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn(
                "event_id",
                md5(concat_ws("_", "timestamp", "visitorid", "itemid", "event")),
            )
        )
        return df

    def deduplicate_events(self, df: DataFrame) -> DataFrame:
        """Remove duplicate events using streaming-compatible dropDuplicates."""
        # Apply watermark for late-data handling
        df = df.withWatermark("ingestion_timestamp", "1 hour")
        
        # dropDuplicates on event_id (streaming-supported)
        df = df.dropDuplicates(["event_id"])
        
        logger.info("Applied streaming deduplication via dropDuplicates")
        return df

    def write_bronze(self, df: DataFrame, checkpoint_id: str = "bronze"):
        """Write deduplicated events to Delta Bronze table."""
        checkpoint_path = f"{self.delta_config.checkpoint_path}/{checkpoint_id}"

        query = (
            df
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .option("mergeSchema", "true")
            .start(self.delta_config.bronze_path)
        )

        logger.info(f"✓ Bronze streaming query started: {self.delta_config.bronze_path}")
        return query

    def run(self, checkpoint_id: str = "bronze"):
        """Full Bronze pipeline."""
        df = self.read_kafka_stream()
        df = self.add_bronze_metadata(df)
        df = self.deduplicate_events(df)
        query = self.write_bronze(df, checkpoint_id)
        return query


def main():
    from src.spark_utils import build_spark_session

    spark = build_spark_session("BronzeLayer")
    bronze = BronzeLayer(spark)
    query = bronze.run()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Bronze layer stopped")
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
