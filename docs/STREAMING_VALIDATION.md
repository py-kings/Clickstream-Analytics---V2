# Streaming Implementation Validation

**Status:** Validated against the current codebase  
**Architecture:** Kafka → Bronze Streaming → Silver Streaming → Gold Streaming → Delta/Databricks SQL

## Bronze
- Kafka source uses `readStream.format("kafka")`.
- Event IDs are generated from event fields.
- Deduplication uses `withWatermark(...).dropDuplicates(...)`.
- Bronze writes Delta with `writeStream` and checkpoints.
- No `row_number()` deduplication.

## Silver
- Reads Bronze Delta with `readStream`.
- Converts event timestamps and applies event-time watermarking.
- Performs enrichment and streaming-safe filtering/quality handling.
- Does not perform sessionization.
- No `lag()`/`row_number()` sessionization.

## Gold
- Batch job: reads Silver Delta with `read` (`gold_layer.run_gold_batch`).
- Sessionization is lag-based with a 30-minute inactivity gap (`gold_layer.sessionize`); no `session_window` is used.
- Produces session-level and daily-funnel tables in batch.
- Writes Gold Delta with overwrite; no streaming Gold path exists in this repository.
- Session start/end timestamps are not part of the session output.
- `run_gold_batch(spark)` is the batch entry point (`python -m src.gold_layer`).

## Databricks
- Databricks notebooks are not included in this repository; this section is unverified.
- No streaming Gold notebook exists in this repository.
- Storage paths resolve to local paths only; S3 is not reachable from the current config (see P0 report).

## Airflow
- The DAG (`clickstream_analytics_v2`) runs a bounded batch path: simulation, then Bronze -> Silver (`src/airflow_pipeline.py`), then Gold batch (`gold_layer.run_gold_batch`), then data quality, forecasting and notification.
- Gold runs only after Bronze/Silver succeed (`bronze_silver_batch >> gold_batch_aggregations`). Gold refuses to overwrite its tables when Silver is empty.
- Bounded runs use earliest Kafka offsets and Delta version 0 on a first run; later runs resume from their checkpoints.
- Do not run the continuous Bronze/Silver streams against the same Delta tables while the DAG is active; both paths would append duplicate Bronze rows.
- Verified: the DAG loads with zero import errors in Airflow 2.9.3 and its graph matches the intended order (`tests/test_dag.py`).
- Not verified in this repository's test environment: an end-to-end DAG run against live Kafka and Delta (see the P1 report).

## Processing Semantics
- Checkpointing provides fault recovery and offset tracking.
- Processing is treated as at-least-once.
- Streaming deduplication provides logical duplicate protection; this is not a blanket exactly-once claim.

## Resource Profile
- Spark local mode and shuffle partitions are configurable.
- Event rate and development storage can be reduced for an 8GB laptop.
- Avoid running Kafka, Spark, MLflow and large workloads simultaneously on an 8GB machine.

## Validation Scope
Static validation confirms the streaming code paths and documentation are aligned. Actual runtime success still requires Kafka, Spark/Databricks, Delta and S3 (when used) to be available and correctly configured.
