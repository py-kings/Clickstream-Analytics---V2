"""
Tests for the production Bronze, Silver, and Gold transformations.

Every test calls the real module code (src.bronze_layer, src.silver_layer,
src.gold_layer, src.data_quality). No transformation logic is re-implemented here.
"""
from datetime import datetime

import pytest
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, LongType, StringType

from src.bronze_layer import BronzeLayer
from src.silver_layer import SilverLayer
from src.gold_layer import sessionize, build_sessions, build_daily_funnel
from src.data_quality import NullCountCheck, RowCountCheck
from tests.conftest import RAW_EVENT_SCHEMA


NULLABLE_RAW_SCHEMA = StructType([
    StructField("timestamp", LongType(), True),
    StructField("visitorid", LongType(), True),
    StructField("event", StringType(), True),
    StructField("itemid", LongType(), True),
    StructField("transactionid", LongType(), True),
])


def _to_silver(spark, raw_df):
    """Run the production Silver timestamp conversion on a raw DataFrame."""
    return SilverLayer(spark).convert_timestamps(raw_df)


class TestBronzeLayer:
    """Production BronzeLayer.add_bronze_metadata / deduplicate_events."""

    def test_event_id_is_populated_and_unique_per_event(self, raw_events_df):
        bronze = BronzeLayer(spark=None)
        ids = [r[0] for r in bronze.add_bronze_metadata(raw_events_df).select("event_id").collect()]

        assert len(ids) == 5
        assert all(ids)
        assert len(set(ids)) == 5

    def test_deduplicate_removes_duplicate_events(self, raw_events_df):
        bronze = BronzeLayer(spark=None)
        enriched = bronze.add_bronze_metadata(raw_events_df)
        with_dup = enriched.union(enriched.filter(col("visitorid") == 1001).limit(1))
        assert with_dup.count() == 6

        deduped = bronze.deduplicate_events(with_dup)
        assert deduped.count() == 5


class TestSilverLayer:
    """Production SilverLayer.convert_timestamps / data_quality_checks."""

    def test_convert_timestamps_turns_milliseconds_into_timestamp(self, spark, raw_events_df):
        row = _to_silver(spark, raw_events_df.limit(1)).select("event_timestamp").collect()[0]
        # 1000000 ms = 1000 s after the epoch. Spark collects in local time.
        assert row[0] == datetime.fromtimestamp(1000)

    def test_quality_filter_drops_rows_missing_visitor_or_event(self, spark, raw_events_df):
        bad_rows = spark.createDataFrame(
            [
                (1500000, None, "view", 5010, None),   # null visitorid
                (1600000, 1003, None, 5011, None),      # null event
            ],
            schema=NULLABLE_RAW_SCHEMA,
        )
        combined = raw_events_df.unionByName(bad_rows, allowMissingColumns=True)
        assert combined.count() == 7

        kept = SilverLayer(spark).data_quality_checks(_to_silver(spark, combined))
        assert kept.count() == 5
        assert kept.filter(col("visitorid").isNull() | col("event").isNull()).count() == 0


class TestGoldLayer:
    """Production gold_layer.sessionize / build_sessions / build_daily_funnel."""

    def test_sessions_split_after_30_minute_gap(self, spark, raw_events_df):
        # Visitor 1001 has 4 events within seconds. This event is 40 minutes later.
        late = spark.createDataFrame(
            [(1000000 + 40 * 60 * 1000, 1001, "view", 5004, None)],
            schema=RAW_EVENT_SCHEMA,
        )
        silver = _to_silver(spark, raw_events_df.unionByName(late))
        sessions = build_sessions(sessionize(silver))

        rows = sessions.filter(col("visitorid") == 1001).orderBy("session_id").collect()
        assert [r["session_id"] for r in rows] == ["1001_1", "1001_2"]
        assert [r["event_count"] for r in rows] == [4, 1]
        assert [r["converted"] for r in rows] == [1, 0]

    def test_sessions_keep_visitors_separate(self, spark, raw_events_df):
        silver = _to_silver(spark, raw_events_df)
        rows = build_sessions(sessionize(silver)).filter(col("visitorid") == 1002).collect()
        assert len(rows) == 1
        assert rows[0]["session_id"] == "1002_1"
        assert rows[0]["views"] == 1

    def test_daily_funnel_counts_and_rates(self, spark, raw_events_df):
        silver = _to_silver(spark, raw_events_df)
        funnel = build_daily_funnel(sessionize(silver)).collect()

        assert len(funnel) == 1
        row = funnel[0]
        assert row["viewers"] == 2          # visitors 1001, 1002
        assert row["carters"] == 1          # visitor 1001
        assert row["buyers"] == 1           # visitor 1001
        assert row["total_views"] == 3
        assert row["total_addtocarts"] == 1
        assert row["total_transactions"] == 1
        assert row["view_to_cart_rate"] == pytest.approx(0.5)
        assert row["cart_to_purchase_rate"] == pytest.approx(1.0)


class TestDataQuality:
    """Production data_quality check classes."""

    def test_null_check_passes_on_clean_data(self, raw_events_df):
        check = NullCountCheck(["timestamp", "visitorid", "event"])
        assert check.run(raw_events_df) is True

    def test_null_check_fails_when_nulls_present(self, spark):
        df = spark.createDataFrame([(1, None, "view")], schema="timestamp long, visitorid long, event string")
        assert NullCountCheck(["visitorid"]).run(df) is False

    def test_row_count_check(self, raw_events_df):
        assert RowCountCheck(min_rows=3).run(raw_events_df) is True
        assert RowCountCheck(min_rows=100).run(raw_events_df) is False


class TestGoldGuard:
    """Gold must not overwrite its tables with an empty Silver read."""

    def test_ensure_silver_has_rows_rejects_empty(self, spark):
        from src.gold_layer import ensure_silver_has_rows

        empty = spark.createDataFrame([], schema=RAW_EVENT_SCHEMA)
        with pytest.raises(RuntimeError, match="Silver table is empty"):
            ensure_silver_has_rows(empty)

    def test_ensure_silver_has_rows_passes_non_empty(self, raw_events_df):
        from src.gold_layer import ensure_silver_has_rows

        assert ensure_silver_has_rows(raw_events_df) is raw_events_df


class _RecordingReader:
    """Stands in for spark.readStream: records format/option calls, no I/O."""

    def __init__(self):
        self.formats = []
        self.options = {}
        self.loaded = None

    def format(self, fmt):
        self.formats.append(fmt)
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def load(self, path=None, **kwargs):
        self.loaded = path
        from unittest.mock import MagicMock
        return MagicMock()


class _RecordingSpark:
    def __init__(self):
        self.readStream = _RecordingReader()


class TestBoundedStartPositions:
    """Start positions for the bounded Airflow run; defaults keep streaming behavior."""

    def test_bronze_default_is_latest(self, spark):
        from src.bronze_layer import BronzeLayer

        fake = _RecordingSpark()
        BronzeLayer(fake).read_kafka_stream()
        assert fake.readStream.options["startingOffsets"] == "latest"

    def test_bronze_bounded_uses_earliest(self, spark):
        from src.bronze_layer import BronzeLayer

        fake = _RecordingSpark()
        BronzeLayer(fake, starting_offsets="earliest").read_kafka_stream()
        assert fake.readStream.options["startingOffsets"] == "earliest"

    def test_silver_default_sets_no_starting_version(self, spark):
        from src.silver_layer import SilverLayer

        fake = _RecordingSpark()
        SilverLayer(fake).read_bronze()
        assert "startingVersion" not in fake.readStream.options

    def test_silver_bounded_sets_starting_version(self, spark):
        from src.silver_layer import SilverLayer

        fake = _RecordingSpark()
        SilverLayer(fake, starting_version="0").read_bronze()
        assert fake.readStream.options["startingVersion"] == "0"


class TestBoundedRunnerWiring:
    """airflow_pipeline.main must build Bronze/Silver with earliest offsets and Delta version 0."""

    def test_main_uses_earliest_offsets_and_version_zero(self, monkeypatch):
        import src.airflow_pipeline as ap

        calls = {}

        class _Query:
            def processAllAvailable(self):
                pass

            def stop(self):
                pass

        class _Layer:
            def __init__(self, spark, **kwargs):
                calls[type(self).__name__] = kwargs

            def run(self, checkpoint_id):
                return _Query()

        class _Spark:
            def stop(self):
                pass

        class _Bronze(_Layer):
            pass

        class _Silver(_Layer):
            pass

        monkeypatch.setattr(ap, "build_spark_session", lambda name: _Spark())
        monkeypatch.setattr(ap, "BronzeLayer", _Bronze)
        monkeypatch.setattr(ap, "SilverLayer", _Silver)

        ap.main()

        assert calls["_Bronze"] == {"starting_offsets": "earliest"}
        assert calls["_Silver"] == {"starting_version": "0"}
