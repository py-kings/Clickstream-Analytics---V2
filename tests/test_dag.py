"""
Airflow DAG tests: the DAG must load with zero import errors and the task
graph must run Gold only after bounded Bronze/Silver processing.

Skipped when Airflow is not installed (it is not in requirements.txt).
"""
import os
import tempfile
from pathlib import Path

import pytest

DAG_FOLDER = Path(__file__).resolve().parent.parent / "airflow"
DAG_ID = "clickstream_analytics_v2"

EXPECTED_TASKS = {
    "kafka_ingestion.check_kafka_connectivity",
    "kafka_ingestion.simulate_events",
    "delta_processing.bronze_silver_batch",
    "delta_processing.gold_batch_aggregations",
    "data_quality_validation",
    "prophet_forecasting",
    "notify_completion",
}

EXPECTED_EDGES = {
    "kafka_ingestion.check_kafka_connectivity": {"kafka_ingestion.simulate_events"},
    "kafka_ingestion.simulate_events": {"delta_processing.bronze_silver_batch"},
    "delta_processing.bronze_silver_batch": {"delta_processing.gold_batch_aggregations"},
    "delta_processing.gold_batch_aggregations": {"data_quality_validation"},
    "data_quality_validation": {"prophet_forecasting"},
    "prophet_forecasting": {"notify_completion"},
    "notify_completion": set(),
}


@pytest.fixture(scope="module")
def dag():
    # Guard on airflow.models, not "airflow": the repo's airflow/ folder is a namespace
    # package that shadows the name when Airflow is not installed.
    pytest.importorskip("airflow.models", reason="Airflow not installed")
    os.environ["AIRFLOW_HOME"] = tempfile.mkdtemp(prefix="airflow_home_")
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    from airflow.models import DagBag
    from airflow.utils.db import initdb

    initdb()  # DagBag reads DAG metadata from the Airflow DB; use a fresh temp DB

    bag = DagBag(dag_folder=str(DAG_FOLDER), include_examples=False)
    assert bag.import_errors == {}, f"DAG import errors: {bag.import_errors}"
    dag = bag.get_dag(DAG_ID)
    assert dag is not None
    return dag


def test_dag_loads_with_zero_import_errors(dag):
    assert dag.dag_id == DAG_ID


def test_dag_has_expected_tasks(dag):
    assert set(dag.task_ids) == EXPECTED_TASKS


def test_dag_has_expected_direct_edges(dag):
    actual = {t.task_id: {d.task_id for d in t.downstream_list} for t in dag.tasks}
    for task_id, downstream in EXPECTED_EDGES.items():
        assert actual[task_id] == downstream, task_id


def test_gold_runs_only_after_bronze_silver(dag):
    gold = dag.get_task("delta_processing.gold_batch_aggregations")
    upstream = {t.task_id for t in gold.get_flat_relatives(upstream=True)}
    assert "delta_processing.bronze_silver_batch" in upstream
    assert "kafka_ingestion.simulate_events" in upstream


def test_bronze_silver_does_not_depend_on_gold(dag):
    bronze_silver = dag.get_task("delta_processing.bronze_silver_batch")
    upstream = {t.task_id for t in bronze_silver.get_flat_relatives(upstream=True)}
    assert "delta_processing.gold_batch_aggregations" not in upstream


def test_quality_and_forecasting_run_after_gold(dag):
    quality = dag.get_task("data_quality_validation")
    upstream = {t.task_id for t in quality.get_flat_relatives(upstream=True)}
    assert "delta_processing.gold_batch_aggregations" in upstream
