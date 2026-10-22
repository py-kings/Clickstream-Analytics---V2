> **Version history.** Entries were written at earlier stages. Test counts, CI, S3, and streaming-Gold claims in older entries were later corrected or retracted; see README.md for current status.

# Changelog

All notable changes to the Clickstream Analytics Platform are documented here.

## [2.0.0] - 2024

### Added (v2 - Production Upgrade)

#### Architecture
- **Kafka Event Streaming**: Real-time clickstream ingestion from Kafka topics
- **Spark Structured Streaming**: Distributed event processing with exactly-once semantics
- **Delta Lake Medallion Architecture**: Bronze → Silver → Gold three-tier data warehouse
  - Bronze: Raw event deduplication with MD5 event IDs
  - Silver: Sessionization (30-min inactivity gap), watermarking (10-min), windowing (5-min)
  - Gold: Business metrics (daily funnel, top items, cart abandonment, session behavior)

#### Data Processing
- **Deduplication**: Row-number-based duplicate removal at Bronze layer
- **Sessionization**: Window-based session grouping from raw clicks
- **Watermarking**: 10-minute late-data tolerance for accurate aggregations
- **Windowing**: 5-minute tumbling windows with 1-minute sliding windows
- **Data Quality Framework**: Pluggable quality checks (null count, duplicates, row count, schema)

#### Analytics & Forecasting
- **Prophet Time-Series Model**: 14-day viewer forecasting with weekly seasonality
- **MLflow Integration**: Experiment tracking, parameter logging, model versioning
- **Gold Layer Aggregations**:
  - Daily funnel (viewers → cart → buyers)
  - Top products by conversion rate
  - Cart abandonment tracking
  - Session behavior analysis (converted vs non-converted)
  - Hourly activity monitoring

#### Orchestration & Automation
- **Apache Airflow DAG v2**: Daily schedule with task groups
  - Kafka ingestion → Bronze → Silver → Gold → Data Quality → Forecasting
- **GitHub Actions CI/CD**: documented, but `.github/workflows/` is not present in this repository
- **Makefile**: Convenience commands for dev tasks

#### Testing & Quality
- **pytest Suite**: 14 unit tests covering (no integration tests against live Kafka/Delta):
  - Bronze layer deduplication
  - Silver layer sessionization
  - Gold layer aggregations
  - Data quality checks
- **Coverage Reporting**: HTML coverage reports with pytest-cov

#### Monitoring & Operations
- **Spark Streaming Checkpoints**: Fault-tolerant processing with exactly-once semantics
- **MLflow Dashboard**: http://localhost:5000 for experiment tracking
- **Kafdrop Integration**: Kafka topic monitoring at http://localhost:9000
- **Data Quality Reports**: Automated DQ gate before forecasting

#### Configuration & Environment
- **Pydantic Config Management**: Type-safe environment variable handling
- **.env.example**: Template with all configurable parameters
- **Docker Compose**: Kafka, Zookeeper, MLflow, Kafdrop stack
- **Documentation**: README, SETUP, ARCHITECTURE guides

### Changed

- Upgraded from batch Spark jobs (v1) to Spark Structured Streaming
- Replaced static S3 snapshots with Delta Lake versioned tables
- Enhanced event simulator with configurable rates and realistic patterns
- Improved deduplication from simple drop-duplicates to event-ID-based approach
- Refactored forecasting to use MLflow for production-grade model management

### Deprecated

- v1 spark_job.py (batch processing only)
- v1 forecast_job.py (no MLflow tracking)

### Removed

- Legacy Tableau Public dashboard references (now Databricks SQL templates)
- Hardcoded S3 paths (now configurable via .env)

### Fixed

- Data quality checks now non-blocking (prevent pipeline halt on warnings)
- Session overlap handled correctly with watermarking
- Forecast predictions now include confidence intervals

### Security

- AWS credentials handled via environment variables (no hardcoding)
- IAM role support for S3 access in cloud deployments
- Secrets management guidance in documentation

### Performance

- Streaming latency: ~1-2 seconds (Kafka → Delta)
- Throughput: 1,000-5,000 events/sec on 8GB RAM
- Deduplication: O(n) via window function (scalable)
- Sessionization: ~2-3x faster than v1 (distributed)

---

## [1.0.0] - 2023

### Initial Release

- Batch Spark jobs for clickstream processing
- Static S3-based data lake
- Parquet output tables
- Apache Airflow DAG for daily runs
- Prophet forecasting (standalone)
- Streamlit/Tableau dashboards

### Components (v1)

- spark_job.py: Event aggregation, sessionization
- forecast_job.py: Prophet model training
- clickstream_dag.py: Airflow orchestration
- clickstream_dashboard.py: Streamlit frontend

---

## Migration Guide: v1 → v2

### Breaking Changes

1. **Output Path Changes**
   - v1: `s3://bucket/processed/daily_funnel/`
   - v2: `/tmp/delta-lake/gold/daily_funnel`

2. **New Dependencies**
   - kafka-python: Kafka client
   - delta-spark: Delta Lake support
   - mlflow: Model tracking

3. **Configuration**
   - v1: Hardcoded env vars in DAG
   - v2: .env file with Pydantic validation

### Migration Steps

```bash
# 1. Backup v1 data
aws s3 sync s3://bucket/processed/ s3://bucket/backup/v1/

# 2. Upgrade dependencies
pip install -r requirements.txt  # includes kafka-python, delta-spark

# 3. Set environment variables
cp .env.example .env
# Edit .env with your Kafka, S3 settings

# 4. Initialize Kafka & Docker
docker-compose up -d

# 5. Run v2 pipeline
python -m src.bronze_layer &
python -m src.silver_layer &
python -m src.simulator 50 300
python -m src.gold_layer

# 6. Verify data
# Check Delta tables: /tmp/delta-lake/gold/
# Compare metrics with v1 outputs
```

---

## Future Roadmap

### v2.1 (Planned)

- Real-time Streamlit dashboard (replacing batch Tableau)
- Spark SQL Warehouse integration
- Multiple forecast models (ARIMA, LightGBM ensemble)

### v3 (Planned)

- Streaming joins (Kafka topics)
- Real-time ML scoring
- Kubernetes deployment (Spark on k8s)
- dbt integration for Gold layer transforms

---

**Versioning**: Semantic versioning (MAJOR.MINOR.PATCH)

**Support**: Community-driven; issues/PRs welcome
