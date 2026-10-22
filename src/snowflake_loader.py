import os
import csv
import logging
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
EXPORTS = {
    "DAILY_FUNNEL": ROOT / "powerbi_data" / "daily_funnel",
    "SESSIONS": ROOT / "powerbi_data" / "sessions",
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )


def load_csv_table(conn, table_name, folder):
    files = list(folder.glob("part-*.csv"))
    if len(files) != 1:
        raise RuntimeError(
            f"Expected exactly one CSV in {folder}, found {len(files)}"
        )

    with files[0].open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        rows = list(reader)

    if not rows:
        raise RuntimeError(f"Refusing to replace {table_name} with empty data")

    columns = ", ".join(f'"{name.upper()}"' for name in headers)
    placeholders = ", ".join(["%s"] * len(headers))
    insert_sql = (
        f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    )

    cur = conn.cursor()
    try:
        cur.execute("BEGIN")
        cur.execute(f"DELETE FROM {table_name}")
        cur.executemany(insert_sql, rows)
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        actual = cur.fetchone()[0]

        if actual != len(rows):
            raise RuntimeError(
                f"{table_name}: expected {len(rows)} rows, found {actual}"
            )

        conn.commit()
        logger.info("%s refreshed successfully: %d rows", table_name, actual)

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def main():
    conn = connect()
    try:
        for table_name, folder in EXPORTS.items():
            load_csv_table(conn, table_name, folder)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
