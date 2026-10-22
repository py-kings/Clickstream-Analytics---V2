# Setup Guide

This guide covers local setup, the streaming mode, and the Airflow mode. Everything marked **unverified**
has not been run end-to-end in the test environment (see README → Verification status).

## 1. Prerequisites

| Tool | Notes |
|---|---|
| Python | Tested on 3.12.3. 3.9–3.11 are not tested. |
| Java | Required by PySpark 3.5, which documents Java 8, 11, and 17. The JDK version used here was not recorded. |
| Docker and Docker Compose | For Kafka, Zookeeper, and MLflow. Not verified here. |
| Internet access on first Spark run | Spark downloads Delta and Hadoop-AWS jars from Maven (`spark.jars.packages`). **Unverified**: the test environment could not reach Maven. |
| RAM | 8 GB is the target. Kafka, Spark, and MLflow together are tight on 8 GB. |

## 2. Install

```bash
git clone <your-repo-url> clickstream-v2
cd clickstream-v2
python -m venv venv
source venv/bin/activate            # Windows PowerShell: .\venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -r requirements.txt
```

Or use the Makefile: `make setup` (creates `venv/` and installs the requirements).

Check the install:

```bash
pytest tests/ -q                    # unit tests; the DAG tests skip unless Airflow is installed
```

## 3. Configure (optional)

```bash
cp .env.example .env
```

The defaults run locally without AWS. `.env` is loaded from the repository root no matter which directory
you start from. Real environment variables override `.env`. Full list: README → Configuration.

## 4. Start Kafka

```bash
docker-compose up -d zookeeper kafka mlflow
docker-compose ps
```

Host clients connect to `localhost:9092`. The topic is auto-created on first produce
(`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`).

**Unverified:** these services were not started in the test environment.

Optional, to list topics from inside the broker container (**unverified**; the container name is set in
`docker-compose.yml`):

```bash
docker exec clickstream-kafka kafka-topics --list --bootstrap-server kafka:29092
```

## 5. Streaming mode (manual)

Run each command in its own terminal. Bronze and Silver run until you press Ctrl+C.

```bash
python -m src.bronze_layer          # Kafka → ./delta-lake/bronze   (reads from the latest offset)
python -m src.silver_layer          # ./delta-lake/bronze → ./delta-lake/silver
python -m src.simulator 50 300      # 50 events/s for 300 s into Kafka
python -m src.gold_layer            # batch: ./delta-lake/silver → gold/sessions, gold/daily_funnel
python -m src.forecasting           # optional: needs ≥14 days in gold/daily_funnel
```

Run `python -m src.gold_layer` after Silver has data. It is a one-shot batch job, not a stream.

Bronze starts at the latest Kafka offset. Start it before the simulator, or events produced earlier are not
ingested.

Stop the streams with Ctrl+C and check that no Java processes remain (`ps aux | grep java`).

**Unverified:** the streaming Bronze and Silver jobs have not been run against a live broker or Delta tables
in this repository's test environment.

## 6. Airflow mode

The DAG runs simulation, a bounded Bronze → Silver batch, Gold, data quality, forecasting, and a notification.

### 6.1 Install Airflow separately

Airflow is not in `requirements.txt`. Install it from `requirements-airflow.txt`, into the same environment
as the pipeline:

```bash
pip install -r requirements.txt
pip install -r requirements-airflow.txt
```

The `protobuf<5` pin in `requirements-airflow.txt` keeps `mlflow==2.10.2` working. Without it, Airflow's
dependencies upgrade protobuf and break mlflow. **Verified:** this combined install in a fresh venv gives a
clean `pip check`, and all 65 tests pass. **Unverified:** running the DAG's Spark, Kafka, and Prophet tasks.

The Airflow worker needs this project's dependencies too, because the tasks import `src/`. Use the same
environment, or a container image with both.

### 6.2 Initialize Airflow

```bash
export AIRFLOW_HOME=$HOME/airflow
airflow db init
airflow users create --username admin --use-random-password \
  --firstname Admin --lastname User --role Admin --email admin@example.com
```

`--use-random-password` prints a generated password once. Save it. There are no default credentials.

### 6.3 Point Airflow at the project

```bash
export CLICKSTREAM_PROJECT_DIR=/absolute/path/to/clickstream-v2
cp airflow/clickstream_dag_v2.py $AIRFLOW_HOME/dags/
```

`CLICKSTREAM_PROJECT_DIR` must be set in the environment of the **scheduler and worker**, not only in your
shell. The DAG imports `src/` from this directory. The default `/opt/airflow/dags` only works if `src/` is
copied there.

### 6.4 Run

Run each command in its own terminal, or as a service:

```bash
airflow scheduler
airflow webserver --port 8080       # http://localhost:8080
```

Or with the Makefile: `make airflow-init`, then `make airflow-scheduler` and `make airflow-webserver` in two
terminals.

Enable `clickstream_analytics_v2` in the UI and trigger a run.

Task order:

1. `kafka_ingestion.check_kafka_connectivity` checks `KAFKA_BOOTSTRAP_SERVERS`.
2. `kafka_ingestion.simulate_events` produces 300 s of events at 50/s (blocks the worker for 5 minutes).
3. `delta_processing.bronze_silver_batch` runs the bounded Bronze → Silver job. On the first run it reads
   from the earliest Kafka offset and from Delta version 0. Later runs resume from their checkpoints.
4. `delta_processing.gold_batch_aggregations` rebuilds Gold. It refuses to overwrite Gold if Silver is empty.
5. `data_quality_validation` checks row counts on `gold/daily_funnel`. A failure is logged and the run continues.
6. `prophet_forecasting` trains Prophet on `gold/daily_funnel` and logs metrics to MLflow.
7. `notify_completion` echoes a message. It does not send a notification.

**Do not run the streaming Bronze and Silver jobs (section 5) while the DAG is active.** Both paths append to
the same Bronze table, which creates duplicate rows.

**Unverified:** the DAG has been loaded and its graph checked, but no task has run end-to-end with Kafka and
Delta available. Tasks 1–3 and 6 need Kafka, Spark with Delta jars, and/or Prophet with CmdStan.

## 7. Verify the output

```bash
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('verify').master('local[1]').getOrCreate()
for p in ['bronze', 'silver', 'gold/sessions', 'gold/daily_funnel']:
    try:
        print(p, spark.read.format('delta').load(f'./delta-lake/{p}').count())
    except Exception as e:
        print(p, 'not available:', type(e).__name__)
spark.stop()
"
```

Or `make check-data`. It uses the same local path.

## 8. Tests

```bash
make test                           # or: pytest tests/ -v
pytest tests/test_dag.py -v         # needs Airflow installed
```

## 9. Troubleshooting

| Symptom | Check |
|---|---|
| `Kafka connectivity failed` | `docker ps` shows `clickstream-kafka`; `KAFKA_BOOTSTRAP_SERVERS` matches the host listener (`localhost:9092`). |
| `Path does not exist: ./delta-lake/silver` | Run Bronze and Silver first. Gold stops with "Silver table is empty" if there is no data. |
| `ModuleNotFoundError: No module named 'src'` in Airflow | Set `CLICKSTREAM_PROJECT_DIR` in the scheduler and worker environment. |
| `DAG import errors` in the Airflow UI | Run `airflow dags list-import-errors`. Confirm `src/` and the Airflow version. |
| Spark cannot resolve `io.delta` or `hadoop-aws` | Spark needs Maven access on first run. Check network or proxy settings. |
| `ValueError: Invalid Delta config` | `DEV_MODE=false` selects S3, which is not supported in the current config. Use `DEV_MODE=true`. |
| Forecast skipped | Fewer than 14 days of data in `gold/daily_funnel`. |
| Port in use | `lsof -i :9092` (Linux/macOS) or `netstat -ano \| findstr :9092` (Windows). |

## 10. Cleanup

```bash
docker-compose down                 # add -v to remove volumes
rm -rf ./delta-lake ./mlruns        # local data and MLflow runs (not in git)
make clean                          # also removes the venv/ directory
```
