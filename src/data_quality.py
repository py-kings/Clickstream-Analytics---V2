"""
Data Quality Checks: Validate data at each layer.
"""
import logging
from typing import Dict, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


logger = logging.getLogger(__name__)


class DataQualityCheck:
    """Base quality check."""

    def __init__(self, name: str):
        self.name = name
        self.passed = False
        self.message = ""

    def run(self, df: DataFrame) -> bool:
        """Run check. Override in subclasses."""
        raise NotImplementedError

    def __str__(self):
        status = "✓ PASS" if self.passed else "✗ FAIL"
        return f"{status} | {self.name}: {self.message}"


class NullCountCheck(DataQualityCheck):
    """Check for null values in critical columns."""

    def __init__(self, columns: List[str]):
        super().__init__(f"Null Check: {', '.join(columns)}")
        self.columns = columns

    def run(self, df: DataFrame) -> bool:
        null_counts = {}
        for col_name in self.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_counts[col_name] = null_count

        total_nulls = sum(null_counts.values())
        self.passed = total_nulls == 0
        self.message = f"{total_nulls} nulls found: {null_counts}"
        return self.passed


class DuplicateCheck(DataQualityCheck):
    """Check for duplicate rows."""

    def __init__(self, partition_cols: List[str]):
        super().__init__(f"Duplicate Check: {', '.join(partition_cols)}")
        self.partition_cols = partition_cols

    def run(self, df: DataFrame) -> bool:
        original_count = df.count()
        deduplicated_count = df.dropDuplicates(self.partition_cols).count()

        duplicates = original_count - deduplicated_count
        self.passed = duplicates == 0
        self.message = f"{duplicates} duplicate(s) found"
        return self.passed


class RowCountCheck(DataQualityCheck):
    """Check minimum row count."""

    def __init__(self, min_rows: int = 100):
        super().__init__(f"Row Count Check (min={min_rows})")
        self.min_rows = min_rows

    def run(self, df: DataFrame) -> bool:
        row_count = df.count()
        self.passed = row_count >= self.min_rows
        self.message = f"{row_count} rows (min={self.min_rows})"
        return self.passed


class SchemaValidationCheck(DataQualityCheck):
    """Validate required columns exist."""

    def __init__(self, required_columns: List[str]):
        super().__init__(f"Schema Check: {', '.join(required_columns)}")
        self.required_columns = required_columns

    def run(self, df: DataFrame) -> bool:
        existing = set(df.columns)
        required = set(self.required_columns)
        missing = required - existing

        self.passed = len(missing) == 0
        self.message = f"Missing columns: {missing}" if missing else "All required columns present"
        return self.passed


class DataQualityFramework:
    """Run and report DQ checks."""

    def __init__(self):
        self.checks: List[DataQualityCheck] = []
        self.results: Dict[str, bool] = {}

    def add_check(self, check: DataQualityCheck):
        """Add a quality check."""
        self.checks.append(check)

    def run_all(self, df: DataFrame) -> bool:
        """Run all checks against dataframe."""
        logger.info(f"Running {len(self.checks)} quality checks...")

        for check in self.checks:
            check.run(df)
            self.results[check.name] = check.passed
            logger.info(str(check))

        all_passed = all(self.results.values())
        logger.info(f"\n📊 Quality Summary: {sum(self.results.values())}/{len(self.results)} checks passed")
        return all_passed


def bronze_quality_checks() -> DataQualityFramework:
    """DQ checks for Bronze layer."""
    fw = DataQualityFramework()
    fw.add_check(SchemaValidationCheck(["timestamp", "visitorid", "event", "itemid"]))
    fw.add_check(NullCountCheck(["timestamp", "visitorid", "event", "itemid"]))
    fw.add_check(RowCountCheck(min_rows=100))
    fw.add_check(DuplicateCheck(["timestamp", "visitorid", "itemid", "event"]))
    return fw


def silver_quality_checks() -> DataQualityFramework:
    """DQ checks for Silver layer."""
    fw = DataQualityFramework()
    fw.add_check(SchemaValidationCheck(["visitorid", "session_id", "event_timestamp", "event"]))
    fw.add_check(NullCountCheck(["session_id", "event_timestamp"]))
    fw.add_check(RowCountCheck(min_rows=50))
    return fw


def gold_quality_checks() -> DataQualityFramework:
    """DQ checks for Gold layer."""
    fw = DataQualityFramework()
    fw.add_check(RowCountCheck(min_rows=1))
    return fw
