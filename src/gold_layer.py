
"""
Gold Layer (batch): sessionization and daily funnel built from Silver.

Batch-only by design. Streaming Gold is not implemented in this repository.
Outputs (overwritten on each run):
    {gold_path}/sessions      one row per (visitorid, session_id)
    {gold_path}/daily_funnel  one row per event_date

Run directly with: python -m src.gold_layer
"""

import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    countDistinct,
    approx_count_distinct,
    sum as spark_sum,
    count,
    when,
    to_date,
    round as spark_round,
    concat_ws,
    lag,
)
from pyspark.sql.window import Window

from src.config import config
from src.spark_utils import build_spark_session

logger = logging.getLogger(__name__)

SESSION_GAP_MINUTES = 30


def sessionize(silver_df: DataFrame) -> DataFrame:
    """Add session_id using a 30-minute inactivity gap per visitor."""
    w = Window.partitionBy("visitorid").orderBy("event_timestamp")

    df = (
        silver_df.withColumn("prev_ts", lag("event_timestamp").over(w))
        .withColumn(
            "gap_min",
            (
                col("event_timestamp").cast("long")
                - col("prev_ts").cast("long")
            ) / 60,
        )
        .withColumn(
            "is_session_start",
            when(col("gap_min").isNull(), 1)
            .when(col("gap_min") > SESSION_GAP_MINUTES, 1)
            .otherwise(0),
        )
    )

    w2 = Window.partitionBy("visitorid").orderBy("event_timestamp")

    df = (
        df.withColumn(
            "session_num",
            spark_sum("is_session_start").over(w2),
        )
        .withColumn(
            "session_id",
            concat_ws(
                "_",
                col("visitorid").cast("string"),
                col("session_num").cast("string"),
            ),
        )
    )

    return df


def build_sessions(sessionized_df: DataFrame) -> DataFrame:
    """Session-level table: one row per (visitorid, session_id)."""
    return (
        sessionized_df.groupBy("visitorid", "session_id")
        .agg(
            count("*").alias("event_count"),
            count("itemid").alias("item_count"),
            spark_sum(
                when(col("event") == "view", 1).otherwise(0)
            ).alias("views"),
            spark_sum(
                when(col("event") == "addtocart", 1).otherwise(0)
            ).alias("addtocarts"),
            spark_sum(
                when(col("event") == "transaction", 1).otherwise(0)
            ).alias("transactions"),
            countDistinct("itemid").alias("distinct_items"),
        )
        .withColumn(
            "converted",
            (col("transactions") > 0).cast("int"),
        )
    )


def build_daily_funnel(sessionized_df: DataFrame) -> DataFrame:
    """Daily funnel: viewers -> carters -> buyers, with conversion rates."""
    return (
        sessionized_df.withColumn(
            "event_date", to_date("event_timestamp")
        )
        .groupBy("event_date")
        .agg(
            countDistinct(
                when(col("event") == "view", col("visitorid"))
            ).alias("viewers"),
            approx_count_distinct(
                when(col("event") == "addtocart", col("visitorid"))
            ).alias("carters"),
            approx_count_distinct(
                when(col("event") == "transaction", col("visitorid"))
            ).alias("buyers"),
            count(when(col("event") == "view", 1)).alias("total_views"),
            count(
                when(col("event") == "addtocart", 1)
            ).alias("total_addtocarts"),
            count(
                when(col("event") == "transaction", 1)
            ).alias("total_transactions"),
        )
        .withColumn(
            "view_to_cart_rate",
            spark_round(col("carters") / col("viewers"), 4),
        )
        .withColumn(
            "cart_to_purchase_rate",
            spark_round(col("buyers") / col("carters"), 4),
        )
    )


def ensure_silver_has_rows(silver_df: DataFrame) -> DataFrame:
    """Refuse to overwrite Gold tables when Silver is empty."""
    if silver_df.limit(1).count() == 0:
        raise RuntimeError(
            "Silver table is empty; refusing to overwrite Gold tables. "
            "Run Bronze/Silver first."
        )

    return silver_df


def run_gold_batch(
    spark: SparkSession,
    silver_path: str = None,
    gold_path: str = None,
):
    """Read Silver, build sessions and funnel, and overwrite Gold tables."""
    silver_path = silver_path or config.delta.silver_path
    gold_path = gold_path or config.delta.gold_path

    silver_df = ensure_silver_has_rows(
        spark.read.format("delta").load(silver_path)
    )
    logger.info("Silver data loaded successfully")

    sessionized = sessionize(silver_df)

    sessions = build_sessions(sessionized)
    (
        sessions.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{gold_path}/sessions")
    )
    logger.info("Gold sessions table written successfully")

    funnel = build_daily_funnel(sessionized)
    (
        funnel.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{gold_path}/daily_funnel")
    )
    logger.info("Gold daily funnel table written successfully")

    return sessions, funnel


def main():
    logging.basicConfig(level=logging.INFO)

    # Uses the shared Spark configuration, including Delta Lake support.
    spark = build_spark_session("GoldBatchValidation")

    try:
        sessions, funnel = run_gold_batch(spark)
        print("GOLD BATCH COMPLETE")
        print("GOLD SESSIONS:", sessions.count())
        print("GOLD FUNNEL:", funnel.count())
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
