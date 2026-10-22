from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Shared Airflow logs volume: persists between task attempts
COUNTER_FILE = Path(
    "/opt/airflow/logs/self_healing_retry_test_counter.txt"
)


def fail_once_then_succeed():
    COUNTER_FILE.parent.mkdir(parents=True, exist_ok=True)

    try:
        count = int(COUNTER_FILE.read_text().strip())
    except (FileNotFoundError, ValueError):
        count = 0

    count += 1
    COUNTER_FILE.write_text(str(count))

    print(f"Self-healing test attempt: {count}")

    if count == 1:
        raise RuntimeError(
            "Intentional first-attempt failure to test Airflow retries"
        )

    print("RECOVERY SUCCESSFUL: task passed on retry!")


with DAG(
    dag_id="self_healing_retry_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["test", "self-healing"],
) as dag:

    retry_test = PythonOperator(
        task_id="fail_once_then_succeed",
        python_callable=fail_once_then_succeed,
    )
