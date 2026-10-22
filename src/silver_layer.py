"""
Silver Layer: streaming data cleaning, watermarking, data quality and enrichment.
Creates business-ready datasets with event-time processing.
"""
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    window,
    when,
    sum as spark_sum,
    
)
from src.config import config


logger = logging.getLogger(__name__)


class SilverLayer:
    """Streaming data cleaning, watermarking, enrichment, and windowing helpers."""

    def __init__(self, spark: SparkSession, starting_version: str = None):
        self.spark = spark
        self.delta_config = config.delta
        # Delta streaming source starts at the latest version by default. A bounded
        # batch that must process data written earlier in the same run passes "0".
        self.starting_version = starting_version

    def read_bronze(self) -> DataFrame:
        """Read Bronze Delta table."""
        logger.info("Reading Bronze layer...")
        reader = self.spark.readStream.format("delta")
        if self.starting_version is not None:
            reader = reader.option("startingVersion", str(self.starting_version))
        df = reader.load(self.delta_config.bronze_path)
        return df

    def convert_timestamps(self, df: DataFrame) -> DataFrame:
        """Convert millisecond timestamps to proper datetime."""
        df = (
            df
            .withColumn("event_timestamp", (col("timestamp") / 1000).cast("timestamp"))
            .drop("kafka_timestamp")
        )
        return df

    def enrich_event_metadata(self, df: DataFrame) -> DataFrame:
        """Add event metadata for downstream processing.
        
        Note: Gap-based sessionization is computed in Gold layer (batch) to avoid
        stateful streaming complexity. Silver focuses on streaming-native operations:
        cleaning, deduplication, event-time, and watermarking.
        """
        from pyspark.sql.functions import to_date
        
        df = (
            df
            .withColumn("event_date", to_date(col("event_timestamp")))
            .withColumn("event_hour", col("event_timestamp").cast("timestamp"))
        )
        
        logger.info("Added event metadata")
        return df

    def apply_watermark(self, df: DataFrame, watermark_delay: str = "10 minutes") -> DataFrame:
        """Apply event-time watermarking for late data handling."""
        df = df.withWatermark("event_timestamp", watermark_delay)
        logger.info(f"Applied watermark: {watermark_delay}")
        return df

    def window_aggregations(self, df: DataFrame) -> DataFrame:
        """Create windowed event counts (streaming-compatible)."""
        # Note: session_id is computed in Gold layer (batch); Silver uses time-based windowing only
        windowed_df = (
            df
            .groupBy(
                col("visitorid"),
                window(col("event_timestamp"), "5 minutes", "1 minute"),
            )
            .agg(
                spark_sum(when(col("event") == "view", 1).otherwise(0)).alias("view_count"),
                spark_sum(when(col("event") == "addtocart", 1).otherwise(0)).alias("cart_count"),
                spark_sum(when(col("event") == "transaction", 1).otherwise(0)).alias("txn_count"),
            )
        )

        logger.info("Applied 5-min windowing with 1-min slide")
        return windowed_df

    def data_quality_checks(self, df: DataFrame) -> DataFrame:
        """Basic data quality validation (streaming-compatible)."""
        # Remove invalid rows (no .count() on streaming DataFrames)
        df = df.filter(col("visitorid").isNotNull() & col("event").isNotNull())
        logger.info("Applied streaming data quality filters")
        return df

    def write_silver(self, df: DataFrame, checkpoint_id: str = "silver"):
        """Write sessionized events to Delta Silver table."""
        checkpoint_path = f"{self.delta_config.checkpoint_path}/{checkpoint_id}"

        query = (
            df
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .option("mergeSchema", "true")
            .trigger(processingTime="10 seconds")
            .start(self.delta_config.silver_path)
        )

        logger.info(f"✓ Silver streaming query started: {self.delta_config.silver_path}")
        return query

    def run(self, checkpoint_id: str = "silver"):
        """Full Silver pipeline (streaming-native operations only)."""
        df = self.read_bronze()
        df = self.convert_timestamps(df)
        df = self.apply_watermark(df)  # Apply watermark for late-data handling
        df = self.enrich_event_metadata(df)  # Add event date/hour
        df = self.data_quality_checks(df)
        query = self.write_silver(df, checkpoint_id)
        return query


def main():
    from src.spark_utils import build_spark_session

    spark = build_spark_session("SilverLayer")
    silver = SilverLayer(spark)
    query = silver.run()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Silver layer stopped")
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
