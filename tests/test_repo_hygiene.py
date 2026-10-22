"""
Repository hygiene guards: keep machine-specific paths, personal defaults,
and default credentials out of the code, Makefile, and env template.
"""
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

CODE_FILES = (
    list((REPO / "src").glob("*.py"))
    + list((REPO / "airflow").glob("*.py"))
    + list(REPO.glob("export_*.py"))
    + [REPO / "Makefile", REPO / ".env.example"]
)

FORBIDDEN_PATHS = ["/mnt/d/", "/home/claude/", "C:\\", "/Users/"]
FORBIDDEN_DEFAULTS = ["project-bda-data-123", "--password admin", "admin/admin"]


@pytest.mark.parametrize("path", CODE_FILES, ids=lambda p: str(p.relative_to(REPO)))
def test_no_machine_specific_paths(path):
    text = path.read_text(encoding="utf-8", errors="replace")
    hits = [f for f in FORBIDDEN_PATHS if f in text]
    assert not hits, f"{path.name} contains machine-specific paths: {hits}"


@pytest.mark.parametrize("path", CODE_FILES, ids=lambda p: str(p.relative_to(REPO)))
def test_no_personal_or_default_credentials(path):
    text = path.read_text(encoding="utf-8", errors="replace")
    hits = [f for f in FORBIDDEN_DEFAULTS if f in text]
    assert not hits, f"{path.name} contains personal or default credentials: {hits}"


def test_no_aws_or_databricks_secrets_in_code():
    pattern = re.compile(r"AKIA[0-9A-Z]{16}|dapi[0-9a-f]{32}|ghp_[A-Za-z0-9]{36}")
    for path in CODE_FILES:
        assert not pattern.search(path.read_text(encoding="utf-8", errors="replace")), path.name


def test_databricks_queries_reference_only_gold_tables():
    from src.databricks_sql import DatabricksSQLHelper

    allowed = {"gold.sessions", "gold.daily_funnel"}
    for name, query in DatabricksSQLHelper.QUERIES.items():
        tables = set(re.findall(r"FROM\s+([\w.]+)", query, flags=re.IGNORECASE))
        assert tables and tables <= allowed, f"{name} references {tables - allowed}"
