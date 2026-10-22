> **Historical status document.** Written before the P0/P1 audits. Statements about features, tests, tables, and infrastructure in this file were not re-verified and may be out of date. For current setup and behavior see README.md and SETUP.md.

# Architecture: Real-Time Clickstream Analytics v2

## System Design Overview

This document details the design decisions, data flow, and technical implementation of the Real-Time Clickstream Analytics Platform.

---

## 1. Streaming Architecture Pattern

### Medallion (Bronze-Silver-Gold) Architecture

```
        Raw Data
           ↓
    ┌─────────────┐
    │   Bronze    │  Raw Ingestion
    │  (Transient)│  • Kafka → Spark SS
    └─────────────┘  • Deduplication
           ↓         • Event ID hashing
    ┌─────────────┐
    │   Silver    │  Data Preparation
    │ (Processed) │  • Timestamp conversion
    └─────────────┘  • Watermarking
           ↓         • Data quality
    ┌─────────────┐
    │    Gold     │  Analytics-Ready
    │  (Curated)  │  • Session windows (30-min)
    └─────────────┘  • Business metrics
           ↓         • Aggregations
           ↓
      Dashboards &
      Forecasting
```

### Why This Pattern?

| Layer | Purpose | Retention | Reprocessing |
|-------|---------|-----------|--------------|
| Bronze | Audit trail | Long-term | Easy (raw data) |
| Silver | Intermediate | Medium-term | Moderate (derived) |
| Gold | Analytics | Short-term | Rare (final metrics) |

**Benefits**:
- ✓ Data lineage (can replay from Bronze)
- ✓ Schema evolution (Silver/Gold adapt independently)
- ✓ Separation of concerns (each layer has one job)
- ✓ Cost optimization (compression at each layer)

---

## 2. Bronze Layer: Raw Ingestion

### Design

```python
Kafka Topic
  ↓ (JSON events)
┌──────────────────────────────────────┐
│ Spark Structured Streaming           │
│ readStream.format("kafka")           │
│   .option("subscribe", "...")        │
│   .load()                            │
└──────────────────────────────────────┘
  ↓ (Parse JSON)
┌──────────────────────────────────────┐
│ Schema Validation & Event ID Gen     │
│ • from_json(schema)                  │
│ • MD5(timestamp+visitorid+itemid)    │
└──────────────────────────────────────┘
  ↓ (Deduplicate)
┌──────────────────────────────────────┐
│ Deduplication (Streaming-Native)     │
│ • withWatermark("...", "1 hour")     │
│ • dropDuplicates(["event_id"])       │
│ • Checkpoint-backed recovery         │
└──────────────────────────────────────┘
  ↓
┌──────────────────────────────────────┐
│ Delta Lake (Bronze)                  │
│ writeStream.format("delta")          │
│   .mode("append")                    │
│   .option("mergeSchema", true)       │
│   .start()                           │
└──────────────────────────────────────┘
```

### Implementation Details

**Deduplication Logic**:
- Uses MD5 hash of `(timestamp, visitorid, itemid, event)`
- Streaming-compatible: `withWatermark(...).dropDuplicates(["event_id"])`
- Keeps first occurrence (streaming append mode)
- Checkpoint-backed for fault recovery

**Checkpointing**:
- Location: `/tmp/spark-checkpoints/bronze`
- Stores offsets for fault recovery
- Enables at-least-once processing with deduplication

**Metadata Enrichment**:
- `ingestion_timestamp`: CURRENT_TIMESTAMP() when received
- `event_id`: Deduplication key
- Kafka timestamp preserved for audit

---

## 3. Silver Layer: Data Preparation & Watermarking

### Operations

**Silver layer performs streaming-native data preparation**:
- Timestamp conversion: milliseconds → event-time (timestamp column)
- Event-time watermarking for late-data handling
- Metadata enrichment (event_date, event_hour)
- Data quality filtering (remove nulls)
- No stateful operations; no sessionization

**Note**: Sessionization is performed in Gold layer using Spark Structured Streaming `session_window()`, not in Silver.

### Watermarking Strategy

```python
df.withWatermark("event_timestamp", "10 minutes")
```

**Why 10 minutes?**
- Kafka → Spark latency: ~1-2s
- Late data tolerance: 10 min buffer
- State cleanup: Watermark triggers allowed lateness window

**Trade-off**:
- ↑ Watermark delay → captures more late data (more state)
- ↓ Watermark delay → faster cleanup (less memory)

### Windowing

**5-minute sliding window** (slide 1 minute):

```python
window(col("event_timestamp"), "5 minutes", "1 minute")
```

produces overlapping windows:

```
00:00-00:05
    00:01-00:06
        00:02-00:07
            ...
```

**Purpose**: Smooth out per-second spikes, show 5-min rolling metrics.

---

## 4. Silver Layer: Data Quality Checks

### Validation Scheme

```
Input DataFrame (from Bronze)
       ↓
┌─────────────────────────┐
│ Schema Check            │ ← Verify columns exist
│ • timestamp, visitorid  │
│ • event, itemid         │
└─────────────────────────┘
       ↓ (fail → log & continue)
┌─────────────────────────┐
│ Null Check              │ ← Count NULLs
│ • Critical: visitorid   │
│ • Warning: transactionid│
└─────────────────────────┘
       ↓
┌─────────────────────────┐
│ Value Range Check       │ ← Event type enum
│ • event IN (view, ...)  │
└─────────────────────────┘
       ↓
Clean Data → Write to Silver
```

**Non-blocking Design**: Checks log but don't halt pipeline.

---

## 5. Gold Layer: Business Aggregations

### Daily Funnel

```python
SELECT
  event_date,
  COUNTDISTINCT(IF event='view' THEN visitorid) as viewers,
  COUNTDISTINCT(IF event='addtocart' THEN visitorid) as carters,
  COUNTDISTINCT(IF event='transaction' THEN visitorid) as buyers
FROM silver.events
GROUP BY event_date
```

**Insight**: Track conversion at each stage.

```
1000 viewers
  ↓ 20% conversion
200 cart adds
  ↓ 50% conversion
100 purchases
```

### Top Items

```python
SELECT
  itemid,
  COUNTDISTINCT(IF event='view' THEN visitorid) as viewers,
  COUNTDISTINCT(IF event='transaction' THEN visitorid) as buyers,
  (buyers / viewers * 100) as conversion_rate_pct
FROM silver.events
GROUP BY itemid
ORDER BY buyers DESC
```

**Use Case**: Identify best-performing products.

### Session Windows (30-minute inactivity gap)

**Streaming-native implementation**:

```python
session_window(
  col("event_timestamp"),
  "30 minutes"   # Gap duration
)
```

Groups events into sessions based on 30-minute inactivity breaks. When a visitor has no activity for > 30 minutes, a new session starts.

**Streaming aggregation**:
```python
df.withWatermark("event_timestamp", "1 day")
  .groupBy(
      session_window(col("event_timestamp"), "30 minutes"),
      col("visitorid")
  )
  .agg(
      count("*").alias("event_count"),
      countDistinct("itemid").alias("distinct_items"),
      sum(when(col("event") == "view", 1)).alias("views"),
      ...
  )
```

Result: Sessions with metrics (event_count, views, addtocarts, transactions, converted).

### Cart Abandonment

```python
WITH session_events AS (
  SELECT session_id, visitorid,
    MAX(IF event='addtocart' THEN 1 ELSE 0) as has_cart,
    MAX(IF event='transaction' THEN 1 ELSE 0) as has_purchase
  FROM silver.events
  GROUP BY session_id, visitorid
)
SELECT
  event_date,
  COUNTIF(has_cart=1 AND has_purchase=0) as abandoned,
  COUNTIF(has_cart=1) as total_cart_sessions
FROM session_events
```

**Metric**: Abandoned / Total Carts

### Session Behavior

```python
SELECT
  session_id, visitorid,
  COUNT(*) as event_count,
  COUNTDISTINCT(itemid) as distinct_items,
  SUM(IF event='view' THEN 1 ELSE 0) as views,
  SUM(IF event='addtocart' THEN 1 ELSE 0) as addtocarts,
  MAX(IF event='transaction' THEN 1 ELSE 0) as converted
FROM silver.events
GROUP BY session_id, visitorid
```

**Insight**: Converted sessions have more views/items browsed.

---

## 6. Forecasting: Prophet + MLflow

### Time-Series Approach

```
Historical Daily Viewers (60+ days)
  ↓
┌────────────────────────────┐
│ Prophet Model              │
│ • Weekly seasonality: YES  │
│ • Daily seasonality: NO    │
│ • Yearly seasonality: NO   │
│ • Trend: Auto (linear)     │
│ • Interval: 95%            │
└────────────────────────────┘
  ↓
14-Day Forecast
  ↓
┌────────────────────────────┐
│ MLflow Tracking            │
│ • Experiment: ...          │
│ • Parameters logged        │
│ • Metrics (MAE, ...)       │
│ • Model artifact           │
└────────────────────────────┘
  ↓
Export to Parquet / S3
```

### Why Prophet?

- ✓ Fast training (seconds, not hours)
- ✓ Handles missing data
- ✓ Seasonal decomposition
- ✓ Built-in confidence intervals
- ✗ Not suitable for: High-frequency (minute-level)

### MLflow Integration

```python
with mlflow.start_run(run_name="prophet_forecast"):
  mlflow.log_param("forecast_days", 14)
  mlflow.log_param("seasonality", "weekly")
  
  model.fit(df)
  forecast = model.predict(future)
  
  mlflow.log_metric("rmse", rmse_value)
  mlflow.log_model(model, "model")
```

**Benefits**:
- Experiment tracking (A/B test models)
- Reproducibility (parameter versioning)
- Model registry (production deployment)

---

## 7. Checkpoint & State Management

### Spark Streaming Checkpoints

```
/tmp/spark-checkpoints/
├── bronze/
│   ├── metadata
│   ├── 0/
│   ├── 1/
│   └── ...
├── silver/
│   └── ...
└── gold/
    └── ...
```

**Metadata contains**:
- Current offset range processed
- Watermark progress
- State store snapshots

**Recovery**: On restart, Spark reads metadata and resumes from last offset.

### End-to-End Semantics (Checkpoint + Delta ACID)

**Semantic Guarantee**: At-least-once by default (may reprocess events on failure).
Implementation with streaming deduplication:

```python
df.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", path)  # Enables offset tracking
  .start()
```

**Mechanism**:
1. Kafka offsets tracked in checkpoint (offset = {topic, partition, latest_processed})
2. Spark writes micro-batch to Delta (transaction log atomicity)
3. On failure: next run resumes from last checkpoint
4. Deduplication via event_id ensures logical idempotency (same event ≠ duplicate in Gold)

**Limitation**: True exactly-once requires end-to-end transactional isolation across Kafka → Spark → Delta,
which is achieved only if Kafka source, Spark processing, and Delta sink are all transactional (they are).
However, network failures or crashes between checkpoint writes can cause at-least-once behavior.

---

## 8. AWS S3 Integration (Optional)

### Credential Handling

**Approach 1: IAM Role** (Recommended)
```python
# No credentials in code; Spark reads from EC2 metadata
spark.hadoop.fs.s3a.aws.credentials.provider=
  com.amazonaws.auth.DefaultAWSCredentialsProviderChain
```

**Approach 2: Access Keys** (Dev only)
```python
spark.hadoop.fs.s3a.access.key = AWS_ACCESS_KEY_ID
spark.hadoop.fs.s3a.secret.key = AWS_SECRET_ACCESS_KEY
```

### Export Flow

```python
df.write.format("parquet").mode("overwrite").save(
  "s3a://bucket/predicted/viewers_forecast/"
)
```

**Path**: `s3a://<your-bucket>/predicted/viewers_forecast/viewers_forecast.parquet`

---

## 9. Airflow Orchestration

### DAG Structure

```
clickstream_analytics_v2 (daily, 00:00 UTC)
  ├── [TaskGroup] kafka_ingestion
  │   ├── check_kafka_connectivity
  │   └── simulate_events
  ├── [TaskGroup] streaming_boundary
  │   └── Bronze/Silver/Gold run as external long-running services
  ├── data_quality_validation
  ├── prophet_forecasting
  └── notify_completion
```

Airflow does not treat long-running Structured Streaming jobs as finite tasks.
The streaming services are deployed separately (for example, Databricks jobs).
The existing batch Gold method remains available for explicit historical backfills.

### Dependencies

```
kafka_ingestion
  ↓
delta_processing
  ↓
data_quality_validation
  ↓
prophet_forecasting
  ↓
notify_completion
```

**Why this order?**
1. Ensure Kafka has data
2. Process through medallion layers
3. Validate output
4. Train forecast model
5. Notify operators

### Retries & Alerting

```python
default_args = {
  "retries": 2,
  "retry_delay": timedelta(minutes=5),
}
```

On failure → Airflow retries task 2x with 5-min delay.

---

## 10. Performance Characteristics

### Throughput Estimates (8GB RAM)

| Layer | Throughput | Latency | Notes |
|-------|-----------|---------|-------|
| Bronze | 1,000-5,000 events/sec | 1-2s | Kafka + dedup |
| Silver | 500-2,000 events/sec | 10-30s | Sessionization + window |
| Gold | Streaming | Micro-batch dependent | Session/funnel aggregation with event-time state |
| Forecast | Batch | 10-30s | Prophet training |

### Memory Usage

```
Spark Driver: 1-2 GB
Spark Executor: 1-2 GB
Kafka: 512 MB
MLflow: 256 MB
OS/System: 2-3 GB
─────────────────
Total: ~7 GB (fits in 8GB)
```

**Scaling Strategy** (for larger datasets):
1. ↑ SPARK_MASTER to local[8] (if 16GB RAM)
2. ↑ KAFKA_PARTITIONS to 4+ (parallel consumption)
3. ↑ SPARK_SHUFFLE_PARTITIONS to 16+ (more parallelism)
4. ↑ Event rate to 1,000+ events/sec

---

## 11. Data Quality & Monitoring

### Quality Gates

| Check | Layer | Impact | Action |
|-------|-------|--------|--------|
| Null count | Silver | Warn | Log & continue |
| Duplicate rate | Bronze | Warn | Log deduplicated count |
| Row count < min | Gold | Fail | Retry, then alert |
| Schema mismatch | Bronze | Fail | Reject batch |

### Observability

**Logging**:
```python
logger.info("✓ Processed 1000 events")
logger.warning("⚠ High null rate: 5%")
logger.error("✗ Schema validation failed")
```

**Metrics** (MLflow):
- Event latency (ms)
- Dedup rate (%)
- Forecast RMSE (viewers)

**Dashboards**:
- Spark UI (streaming progress)
- MLflow (experiment results)
- Kafdrop (topic lag)

---

## 12. Error Handling & Recovery

### Streaming Failure Scenarios

| Failure | Impact | Recovery |
|---------|--------|----------|
| Kafka broker down | No new data ingested | Auto-reconnect (5 sec retry) |
| Spark executor dies | Batch failed | Checkpoint replay on restart |
| Delta write fails | Transaction aborted | Retry (idempotent write) |
| OOM in Silver layer | Stream halted | Reduce partitions; restart |

### Idempotency

**Key**: Event ID-based deduplication ensures:
- Retrying Bronze write → same result
- Reprocessing Silver → same sessions
- Rerunning Gold → same metrics

---

## 13. Cost Optimization (Cloud Deployment)

### Storage

**Delta Lake Compaction**:
```python
df.write.format("delta").mode("overwrite").save(path)
```

Consolidates small files → fewer objects → lower S3 cost.

**Retention Policy**:
- Bronze: 30 days (audit trail)
- Silver: 7 days (reprocessing buffer)
- Gold: Unlimited (analytics reference)

### Compute

**Spot Instances**: Use 80% spot + 20% on-demand (Databricks)
**Autoscaling**: Scale down after DAG completion

---

## 14. Security Considerations

### Secrets Management

```bash
# ✓ Use environment variables (CI/CD)
export AWS_ACCESS_KEY_ID=...

# ✓ Use IAM roles (AWS, GCP)
# ✓ Use Airflow Secrets backend
# ✓ Use Databricks secrets scope

# ✗ Never hardcode credentials
# ✗ Never commit .env to git
```

### Data Access Control

**Delta Lake Isolation**:
```sql
-- Grant Gold layer access
GRANT SELECT ON TABLE gold.daily_funnel TO analyst_group
```

---

## 15. Future Enhancements

### Planned

1. **Real-Time Dashboard**: Streamlit/Dash with live Gold metrics
2. **A/B Testing**: Multivariate Prophet models
3. **Advanced Forecasting**: ARIMA, LightGBM ensembles
4. **Spark SQL Warehouse**: Interactive queries on Delta
5. **CI/CD Pipeline**: GitHub Actions for automated testing

### Not in Scope (v2)

- Kubernetes orchestration (use Spark on k8s if needed)
- Streaming joins (Kafka + Spark SQL join)
- Real-time ML scoring (Spark MLlib pipelines)
- Snowflake/BigQuery integration

---

## References

- [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Delta Lake Guide](https://docs.delta.io/latest/index.html)
- [Kafka Architecture](https://kafka.apache.org/documentation/#architecture)
- [Prophet Methodology](https://facebook.github.io/prophet/docs/methodology.html)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)

---

**Last Updated**: January 2024  
**Version**: 2.0
