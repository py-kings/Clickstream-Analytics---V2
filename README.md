# Clickstream Analytics v2

A batch-and-streaming data pipeline for e-commerce clickstream events:
Kafka → Bronze → Silver (Delta Lake, PySpark) → Gold (batch) → data quality → Prophet forecasting (MLflow),
orchestrated by an Airflow DAG. Built on the Retail Rocket event schema.

**Status:** unit tests pass in a local environment. The Kafka, Delta Lake, Spark-with-jars, Prophet-fit,
and Airflow task-execution paths have **not** been verified end-to-end. See
[Verification status](#verification-status). This is a portfolio prototype, not a production system.

For step-by-step setup see **[SETUP.md](SETUP.md)**.

## Architecture

```
src/simulator.py          → Kafka topic  (KAFKA_TOPIC_EVENTS, default "clickstream-events")
src/bronze_layer.py       → Bronze Delta  ({LOCAL_DELTA_BASE}/bronze)   parse, event_id, dedup
src/silver_layer.py       → Silver Delta  ({LOCAL_DELTA_BASE}/silver)   timestamps, quality filter
src/gold_layer.py         → Gold Delta    ({LOCAL_DELTA_BASE}/gold/sessions, /gold/daily_funnel)
                            batch: 30-minute-gap sessions, daily funnel; overwrites both tables
src/data_quality.py       → row-count and null checks used by the DAG on the Gold funnel
src/forecasting.py        → Prophet on gold/daily_funnel, metrics to MLflow
airflow/clickstream_dag_v2.py → orchestration (see below)
```

Bronze and Silver are available in two modes. Use one mode per set of Delta tables at a time:

- **Streaming (manual):** `src/bronze_layer.py` and `src/silver_layer.py` run as long-lived Spark streams.
- **Bounded batch (Airflow):** `src/airflow_pipeline.py` processes everything produced so far, then exits.

Running both against the same tables appends duplicate Bronze rows, because the two paths keep separate dedup state.

Gold is always a batch job (`python -m src.gold_layer`, or the DAG task).

## Airflow DAG

DAG id `clickstream_analytics_v2`, schedule `@daily`, `catchup=False`. Task order (verified by `tests/test_dag.py`):

```
kafka_ingestion.check_kafka_connectivity
  → kafka_ingestion.simulate_events                 (50 events/s for 300 s)
  → delta_processing.bronze_silver_batch            (bounded Bronze → Silver)
  → delta_processing.gold_batch_aggregations        (Gold overwrite; runs only after Bronze/Silver succeed)
  → data_quality_validation                         (row-count check on gold/daily_funnel)
  → prophet_forecasting
  → notify_completion                               (echo only)
```

The DAG loads `src/` from `CLICKSTREAM_PROJECT_DIR` (default `/opt/airflow/dags`). When you run the DAG
from a repository checkout, set this variable to the repository root.

## Quick start (local, streaming mode)

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env              # optional; defaults work for local use

docker-compose up -d kafka zookeeper mlflow   # Kafka on localhost:9092

python -m src.bronze_layer        # terminal 1 (long-running)
python -m src.silver_layer        # terminal 2 (long-running)
python -m src.simulator 50 300    # terminal 3: 50 events/s for 5 minutes
python -m src.gold_layer          # after Silver has data: one batch run
python -m src.forecasting         # needs ≥14 days in gold/daily_funnel
```

Local Delta data goes to `./delta-lake` (`LOCAL_DELTA_BASE`). Checkpoints go to `./delta-lake/checkpoints`.

## Outputs

| Path (under `LOCAL_DELTA_BASE`) | Contents |
|---|---|
| `bronze/` | Parsed events with `event_id` and `ingestion_timestamp` |
| `silver/` | Events with `event_timestamp`, `event_date` |
| `gold/sessions/` | One row per (`visitorid`, `session_id`): counts, `converted` |
| `gold/daily_funnel/` | One row per `event_date`: viewers, carters, buyers, rates |

Forecast metrics (`last_actual_viewers`, `forecast_mean`, `forecast_std`) are logged to the MLflow experiment
`clickstream_forecasting`. The forecast is also written to `s3://$CLICKSTREAM_BUCKET/predicted/...` when AWS
credentials are set.

Example queries are in `src/databricks_sql.py` (`gold.sessions` and `gold.daily_funnel` only). They assume
the Delta paths are registered as tables and have not been run against Databricks.

## Configuration

Settings are read from environment variables and from `.env` at the repository root (real environment
variables take precedence). See `.env.example`. Relevant variables:

| Variable | Default | Notes |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | |
| `KAFKA_TOPIC_EVENTS` | `clickstream-events` | Simulator and Bronze must agree |
| `LOCAL_DELTA_BASE` | `./delta-lake` | Local Delta root |
| `DEV_MODE` | `true` | `false` selects the S3 branch, which is **not wired** (see Limitations) |
| `SPARK_MASTER` / `SPARK_SHUFFLE_PARTITIONS` | `local[2]` / `2` | Sized for an 8 GB laptop |
| `EVENT_RATE_PER_SEC` | `10` | Used when the simulator is run without arguments |
| `FORECAST_DAYS` | `14` | |
| `CLICKSTREAM_PROJECT_DIR` | `/opt/airflow/dags` | Airflow only |
| `CLICKSTREAM_BUCKET` | `your-bucket` | Forecast output bucket when AWS credentials are set |
| `POWERBI_EXPORT_DIR` | `./powerbi_exports` | Output folder for `export_*.py` CSVs |
| `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_WAREHOUSE_ID` | empty | Only for `DatabricksSQLHelper` |

## Testing

```bash
pip install -r requirements.txt
pytest tests/ -v          # or: make test
```

The suite has 65 tests (17 transformation, 4 forecasting, 6 DAG, 38 hygiene; the hygiene checks are parametrized over 19 files):

- `tests/test_transformations.py` calls the real Bronze, Silver, Gold, and data-quality code on a small local
  Spark session.
- `tests/test_forecasting.py` runs `train_forecast_model` with a stubbed Prophet class, because CmdStan is
  needed for a real fit.
- `tests/test_dag.py` loads the DAG through Airflow's DagBag and checks the graph. It is **skipped** unless
  Airflow is installed. Install it with `pip install -r requirements-airflow.txt` (see SETUP.md).
- `tests/test_repo_hygiene.py` guards against machine-specific paths and default credentials.

The tests need Java for Spark. They do not need Kafka, Delta jars, or Docker.

## Verification status

**Passed in the local test environment** (Python 3.12.3, pinned requirements, Airflow 2.9.3 installed for the DAG tests):

- All 65 tests (`pytest tests/`): transformations, forecasting, DAG graph, and repository hygiene. Verified
  in a fresh venv from `requirements.txt` plus `requirements-airflow.txt`.
- Fresh venv from `requirements.txt` only: 59 pass, 6 DAG tests skip (Airflow not installed).
- Airflow DagBag loads the DAG with zero import errors. The task graph matches the order above.
- The Gold batch refactor gives the same output as the original code on identical synthetic Silver input
  (Parquet storage substituted for Delta).
- Forecast metric regression: `train_forecast_model` no longer raises on the metrics step (stubbed Prophet).

**Not verified** (services or jars unavailable in the test environment):

- Kafka: producing and consuming events, `earliest`/`latest` offset behavior against a real topic.
- Spark with Delta: `spark.jars.packages` (needs Maven access), streaming Bronze/Silver writes, bounded runs,
  Delta streaming source starting version, Delta reads/writes.
- Prophet: a real model fit (needs CmdStan).
- MLflow: logging to a running tracking server.
- Airflow: running the Bronze/Silver, Gold, and simulator tasks end-to-end.
- Docker Compose services and the Makefile docker targets.
- S3 writes, Databricks SQL execution, and the Power BI CSV exports.

## Limitations

- **S3 storage is not supported** in the current config. `DEV_MODE=false` raises an error because the S3 branch
  is never enabled.
- **Snowflake** is not implemented.
- **Bronze dedup** is keyed on `event_id` (MD5 of timestamp, visitor, item, event type) and only covers one hour
  of ingestion time. Later duplicates pass through.
- **Malformed events are dropped silently** in Silver. There is no quarantine table.
- **Data quality is non-blocking.** The DAG logs failures and continues.
- **Gold recomputes all history** on every run and overwrites `sessions` and `daily_funnel` as two separate
  writes, so a failure between them can leave the two tables inconsistent.
- **Simulated timestamps run ahead of wall-clock time.** The simulator starts at "now" and advances 100 ms per
  event, so a 300-second run at 50 events/s covers about 25 minutes of event time.
- `silver_layer.py` contains a windowing helper (`window_aggregations`) that the pipeline does not call.
- `web/app.py` is a static demo. Its metrics and statuses are hardcoded.
- The `export_*.py` scripts recompute some metrics outside Gold. Their definitions are not guaranteed to match
  `gold_layer.py`.
- Python 3.9–3.11 are not tested. Only 3.12.3 was used.

## Project structure

```
src/
  config.py              settings (pydantic), loads repo-root .env
  spark_utils.py         Spark session with Delta and S3 options, Kafka event schema
  simulator.py           Kafka event generator
  bronze_layer.py        Kafka → Bronze Delta
  silver_layer.py        Bronze → Silver Delta
  airflow_pipeline.py    bounded Bronze → Silver run used by the DAG
  gold_layer.py          Silver → Gold (batch)
  data_quality.py        check classes and per-layer check sets
  forecasting.py         Prophet + MLflow
  databricks_sql.py      query templates for gold.sessions / gold.daily_funnel
airflow/clickstream_dag_v2.py
tests/                   pytest suite (see Testing)
web/                     static Flask demo (not connected to the pipeline; Flask is installed with mlflow)
export_*.py              CSV exports for Power BI (paths from POWERBI_EXPORT_DIR)
docker-compose.yml       Zookeeper, Kafka, MLflow
Makefile                 setup, test, run, and docker targets (`make help`)
requirements.txt         pinned Python dependencies
```

Historical design and status documents (`ARCHITECTURE.md`, `DELIVERY.md`, `VALIDATION.md`,
`FINAL_VALIDATION.md`, `STREAMING_VALIDATION.md`, `CHANGELOG.md`) were written at earlier stages. Their
claims are not current unless this README or SETUP.md repeats them.

## License

MIT. See `LICENSE`.
