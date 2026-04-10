from __future__ import annotations

from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_DIR.parent
SQL_DIR = PACKAGE_DIR / "sql"

DEFAULT_DATA_DIR = PROJECT_ROOT
DEFAULT_DB_PATH = PROJECT_ROOT / "output" / "reederei_mart.db"
DEFAULT_DUCKDB_PATH = PROJECT_ROOT / "output" / "reederei_mart.duckdb"

STATIC_FX_TO_USD = {
    "USD": 1.0,
    "EUR": 1.09,
    "AED": 0.27,
    "SGD": 0.74,
}
