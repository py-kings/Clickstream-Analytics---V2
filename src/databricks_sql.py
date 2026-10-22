"""
Databricks SQL helper and query templates for the Gold tables.

Only tables the batch Gold job actually writes are referenced:
    gold.sessions      one row per (visitorid, session_id)
    gold.daily_funnel  one row per event_date

Register the Delta paths as tables (for example, CREATE TABLE gold.sessions
USING DELTA LOCATION '<gold_path>/sessions') before running these queries.
"""
import logging
import os
from typing import Dict

logger = logging.getLogger(__name__)


class DatabricksSQLHelper:
    """Helper to execute queries on a Databricks SQL warehouse.

    Credentials come from DATABRICKS_HOST, DATABRICKS_TOKEN and
    DATABRICKS_WAREHOUSE_ID. Nothing is hardcoded.
    """

    QUERIES: Dict[str, str] = {
        "daily_funnel": """
            SELECT event_date, viewers, carters, buyers,
                   view_to_cart_rate, cart_to_purchase_rate
            FROM gold.daily_funnel
            ORDER BY event_date DESC
            LIMIT 30
        """,
        "cart_abandonment": """
            SELECT COUNT(*) AS cart_sessions,
                   SUM(CASE WHEN transactions = 0 THEN 1 ELSE 0 END) AS abandoned_sessions
            FROM gold.sessions
            WHERE addtocarts > 0
        """,
        "session_behavior": """
            SELECT converted,
                   COUNT(*) AS session_count,
                   AVG(event_count) AS avg_events,
                   AVG(distinct_items) AS avg_distinct_items
            FROM gold.sessions
            GROUP BY converted
        """,
    }

    def __init__(self, workspace_url: str = None, token: str = None, warehouse_id: str = None):
        self.workspace_url = workspace_url or os.getenv("DATABRICKS_HOST", "")
        self.token = token or os.getenv("DATABRICKS_TOKEN", "")
        self.warehouse_id = warehouse_id or os.getenv("DATABRICKS_WAREHOUSE_ID", "")
        self.conn = None

    def connect(self) -> bool:
        """Connect to the Databricks SQL warehouse. Returns False if not configured."""
        if not all([self.workspace_url, self.token, self.warehouse_id]):
            logger.warning(
                "Databricks credentials not configured. "
                "Set DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID"
            )
            return False

        try:
            from databricks import sql as dbsql

            self.conn = dbsql.connect(
                server_hostname=self.workspace_url.replace("https://", "").split("/")[0],
                http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
                auth_type="pat",
                personal_access_token=self.token,
            )
            logger.info("Connected to Databricks SQL warehouse")
            return True
        except ImportError:
            logger.error("databricks-sql-connector not installed: pip install databricks-sql-connector")
            return False
        except Exception as e:
            logger.error(f"Failed to connect to Databricks: {e}")
            return False

    def execute_query(self, query: str) -> list:
        """Execute a query. Returns [] (and logs) on failure."""
        if not self.conn and not self.connect():
            return []

        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            logger.info(f"Query returned {len(result)} rows")
            return result
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return []

    def execute_named_query(self, query_name: str) -> list:
        """Execute a predefined query by name."""
        query = self.QUERIES.get(query_name)
        if not query:
            logger.error(f"Query not found: {query_name}")
            return []
        return self.execute_query(query)

    @staticmethod
    def get_all_queries() -> Dict[str, str]:
        """Return all predefined queries."""
        return DatabricksSQLHelper.QUERIES


if __name__ == "__main__":
    print("Gold SQL queries defined:")
    for name, query in DatabricksSQLHelper.get_all_queries().items():
        print(f"\n{name}:\n{' '.join(query.split())[:120]}...")
