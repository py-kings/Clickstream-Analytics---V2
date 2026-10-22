"""
Airflow DAG for Real-Time Clickstream Analytics Platform v2.

Pipeline:
1. Validate Kafka connectivity
2. Simulate clickstream events
3. Process bounded Bronze -> Silver data
4. Run Gold batch aggregations
5. Validate Gold data quality
6. Run forecasting with MLflow
7. Export Gold data and load it into Snowflake

Do not run continuous Bronze/Silver streams against the same Delta tables
while this DAG is active; they may append duplicate Bronze rows.
"""

import os
import sys
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


# Configuration
PROJECT_DIR = os.environ.get(
    "CLICKSTREAM_PROJECT_DIR",
    "/opt/airflow/project",
)
PYTHON_BIN = sys.executable

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}


# ------------------------------------------------------------
# Task functions
# ------------------------------------------------------------

def check_kafka_connectivity():
    """Verify Kafka connectivity."""
    sys.path.insert(0, PROJECT_DIR)

    from kafka import KafkaProducer
    from src.config import config

    producer = KafkaProducer(
        bootstrap_servers=config.kafka.bootstrap_servers.split(","),
        request_timeout_ms=5000,
        max_block_ms=5000,
    )

    producer.close()
    print("Kafka connectivity verified.")


def run_event_simulator(event_rate=50, duration=300):
    """Simulate clickstream events."""
    sys.path.insert(0, PROJECT_DIR)

    from src.simulator import KafkaEventProducer

    producer = KafkaEventProducer()
    producer.produce_events(
        rate_per_sec=event_rate,
        duration_sec=duration,
    )

    print("Event simulation completed.")


def run_bronze_silver_batch():
    """Run bounded Bronze -> Silver processing."""
    sys.path.insert(0, PROJECT_DIR)

    from src.airflow_pipeline import main

    main()
    print("Bronze and Silver processing completed.")


def run_gold_aggregations():
    """Run Gold batch aggregations after Silver succeeds."""
    sys.path.insert(0, PROJECT_DIR)

    from src.spark_utils import build_spark_session
    from src.gold_layer import run_gold_batch

    spark = build_spark_session("GoldBatch")

    try:
        run_gold_batch(spark)
    finally:
        spark.stop()

    print("Gold aggregations completed.")


def validate_data_quality():
    """Run Gold-layer data quality checks."""
    sys.path.insert(0, PROJECT_DIR)

    from src.spark_utils import build_spark_session
    from src.data_quality import gold_quality_checks
    from src.config import config

    spark = build_spark_session("DQCheck")

    try:
        gold_path = f"{config.delta.gold_path}/daily_funnel"
        df = spark.read.format("delta").load(gold_path)

        checks = gold_quality_checks()
        all_passed = checks.run_all(df)

        if not all_passed:
            raise ValueError("Gold data quality checks failed.")

        print("Gold data quality checks passed.")
    finally:
        spark.stop()


def run_forecasting():
    """Train the forecasting model and generate predictions."""
    sys.path.insert(0, PROJECT_DIR)

    import mlflow
    from src.spark_utils import build_spark_session
    from src.forecasting import ForecastingPipeline

    mlflow.set_experiment("clickstream_forecasting")
    spark = build_spark_session("Forecasting")

    try:
        pipeline = ForecastingPipeline(spark)
        forecast_df = pipeline.run()

        if forecast_df is not None:
            print("Forecasting completed.")
        else:
            print("Forecasting skipped: insufficient data.")
    finally:
        spark.stop()


def export_gold_for_snowflake():
    """Export Gold Delta tables to CSV files for Snowflake loading."""
    script_path = os.path.join(
        PROJECT_DIR,
        "export_gold_for_snowflake.py",
    )

    if not os.path.isfile(script_path):
        raise FileNotFoundError(
            f"Gold export script not found: {script_path}"
        )

    subprocess.run(
        [PYTHON_BIN, script_path],
        cwd=PROJECT_DIR,
        check=True,
    )

    print("Gold data exported for Snowflake.")


def load_gold_to_snowflake():
    """Load exported Gold CSV snapshots into Snowflake."""
    sys.path.insert(0, PROJECT_DIR)

    from src.snowflake_loader import main

    main()
    print("Gold data loaded into Snowflake.")


# ------------------------------------------------------------
# DAG and tasks
# ------------------------------------------------------------

with DAG(
    dag_id="clickstream_analytics_v2",
    default_args=default_args,
    description=(
        "Kafka ingestion -> Bronze/Silver -> Gold -> "
        "quality checks -> forecasting -> Snowflake"
    ),
    schedule_interval="@daily",
    catchup=False,
    tags=["clickstream", "streaming", "v2", "snowflake"],
) as dag:

    with TaskGroup(
        "kafka_ingestion",
        tooltip="Validate Kafka and simulate events",
    ) as kafka_tg:

        check_kafka = PythonOperator(
            task_id="check_kafka_connectivity",
            python_callable=check_kafka_connectivity,
        )

        simulate_events = PythonOperator(
            task_id="simulate_events",
            python_callable=run_event_simulator,
            op_kwargs={
                "event_rate": 50,
                "duration": 300,
            },
        )

        check_kafka >> simulate_events

    with TaskGroup(
        "delta_processing",
        tooltip="Bounded Bronze/Silver processing and Gold aggregations",
    ) as delta_tg:

        bronze_silver_batch = PythonOperator(
            task_id="bronze_silver_batch",
            python_callable=run_bronze_silver_batch,
        )

        gold_batch = PythonOperator(
            task_id="gold_batch_aggregations",
            python_callable=run_gold_aggregations,
        )

        bronze_silver_batch >> gold_batch

    quality_check = PythonOperator(
        task_id="data_quality_validation",
        python_callable=validate_data_quality,
    )

    forecasting = PythonOperator(
        task_id="prophet_forecasting",
        python_callable=run_forecasting,
    )

    export_gold = PythonOperator(
        task_id="export_gold_for_snowflake",
        python_callable=export_gold_for_snowflake,
    )

    snowflake_load = PythonOperator(
        task_id="load_gold_to_snowflake",
        python_callable=load_gold_to_snowflake,
    )

    notify_completion = BashOperator(
        task_id="notify_completion",
        bash_command='echo "Clickstream Analytics Pipeline v2 completed."',
    )

    (
        kafka_tg
        >> delta_tg
        >> quality_check
        >> forecasting
        >> export_gold
        >> snowflake_load
        >> notify_completion
    )
