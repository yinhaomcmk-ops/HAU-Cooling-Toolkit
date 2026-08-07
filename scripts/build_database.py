from __future__ import annotations

"""Build the runtime SQLite database from the external Excel database folder.

Run locally after Excel source files are updated:

    python scripts/build_database.py

Default source folder:
    C:\\Users\\yinhao.chen\\OneDrive - Hisense\\Documents - YinhaoChen\\Database

The website should then run only from data/app_data.db. Excel files do not need to be kept
inside the website package.
"""

import argparse
import sqlite3
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import services.sales_data_loader as sales_loader  # noqa: E402

from services.sales_data_loader import (  # noqa: E402
    DB_PATH,
    init_all_shared_db,
    normalize_product_master_df,
    normalize_exw_cost_df,
    normalize_landed_cost_df,
    normalize_store_master_df,
    normalize_highlight_store_df,
    normalize_sales_by_stores_df,
    normalize_sales_agent_df,
    save_product_master_records,
    save_exw_cost_records,
    save_landed_cost_records,
    save_store_master_records,
    save_highlight_store_records,
    save_sales_by_stores_records,
    save_sales_agent_records,
)

DEFAULT_SOURCE_DIR = Path(r"C:\Users\yinhao.chen\OneDrive - Hisense\Documents - YinhaoChen\Database")

SOURCE_MAP = {
    "Product Master": {
        "folder": "Product Master",
        "stem": "product_model_master",
        "normalizer": normalize_product_master_df,
        "saver": save_product_master_records,
    },
    "EXW Cost": {
        "folder": "Cost",
        "stem": "exw_cost",
        "normalizer": normalize_exw_cost_df,
        "saver": save_exw_cost_records,
    },
    "Landed Cost": {
        "folder": "Cost",
        "stem": "landed_cost",
        "normalizer": normalize_landed_cost_df,
        "saver": save_landed_cost_records,
    },
    "Store Master": {
        "folder": "Store",
        "stem": "store_master",
        "normalizer": normalize_store_master_df,
        "saver": save_store_master_records,
    },
    "Highlight Store": {
        "folder": "Store",
        "stem": "highlight_store",
        "normalizer": normalize_highlight_store_df,
        "saver": save_highlight_store_records,
    },
    "Sales Summary Store": {
        "folder": "Sellout",
        "stem": "sales_summary_store",
        "normalizer": normalize_sales_by_stores_df,
        "saver": save_sales_by_stores_records,
    },
    "Sellout vs Price": {
        "folder": "Sellout",
        "stem": "Sellout vs Price",
        "normalizer": normalize_sales_agent_df,
        "saver": save_sales_agent_records,
    },
}

KPI_MAP = {
    "DN": {"folder": "DN", "stem": "dn_summary", "table": "kpi_dn_records"},
    "POD": {"folder": "POD", "stem": "pod_summary", "table": "kpi_pod_records"},
}


def find_source_file(source_dir: Path, folder: str, stem: str) -> Path | None:
    folder_path = source_dir / folder
    for suffix in (".xlsx", ".xlsm", ".xls", ".csv"):
        p = folder_path / f"{stem}{suffix}"
        if p.exists():
            return p
    if folder_path.exists():
        matches = [p for p in folder_path.iterdir() if p.is_file() and p.stem.lower() == stem.lower() and p.suffix.lower() in {".xlsx", ".xlsm", ".xls", ".csv"}]
        if matches:
            return matches[0]
    return None


def read_file(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    try:
        return pd.read_excel(path, sheet_name="Export")
    except Exception:
        return pd.read_excel(path)


def recreate_db(db_path: Path) -> None:
    """Create a clean database at *db_path*.

    The save/initialisation helpers in ``sales_data_loader`` read DB_PATH from
    their module globals. Temporarily redirect that global so a requested
    temporary database is actually populated instead of silently writing to
    the live ``data/app_data.db``.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        p = Path(str(db_path) + suffix)
        if p.exists():
            p.unlink()
    init_all_shared_db()


def save_kpi_table(df: pd.DataFrame, table: str) -> int:
    with sqlite3.connect(sales_loader.DB_PATH) as conn:
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        df.to_sql(table, conn, if_exists="replace", index=False)
        conn.commit()
    return len(df)


def create_indexes() -> None:
    with sqlite3.connect(sales_loader.DB_PATH) as conn:
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_model_master_status ON model_master(status)",
            "CREATE INDEX IF NOT EXISTS idx_model_master_year ON model_master(year)",
            "CREATE INDEX IF NOT EXISTS idx_sales_agent_date_model ON sales_agent_records(sales_date, model)",
            "CREATE INDEX IF NOT EXISTS idx_sales_agent_channel_model ON sales_agent_records(channel, model)",
            "CREATE INDEX IF NOT EXISTS idx_sales_records_date_model ON sales_records(sales_date, model)",
            "CREATE INDEX IF NOT EXISTS idx_exw_cost_model_month ON exw_cost(model_id, cost_month)",
            "CREATE INDEX IF NOT EXISTS idx_landed_cost_model_month ON landed_cost(model_id, cost_month)",
            "CREATE INDEX IF NOT EXISTS idx_store_locations_retailer_region ON store_locations(retailer, region)",
        ]
        for sql in indexes:
            conn.execute(sql)
        for table in ["kpi_dn_records", "kpi_pod_records"]:
            exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
            if exists:
                cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{table}")').fetchall()]
                for col in cols:
                    if str(col).lower() in {"date", "created on", "created_on", "month", "year", "customer model", "客户型号"}:
                        idx_name = f"idx_{table}_{str(col).replace(' ', '_').replace('[','').replace(']','')}"
                        conn.execute(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{table}"("{col}")')
        conn.execute("VACUUM")
        conn.commit()


def build_database(source: str | Path = DEFAULT_SOURCE_DIR, db: str | Path = DB_PATH, verbose: bool = True) -> list[str]:
    """Rebuild SQLite from the external Excel database folder and return log lines."""
    source_dir = Path(source)
    db_path = Path(db)
    logs: list[str] = []

    def log(message: str) -> None:
        logs.append(message)
        if verbose:
            print(message)

    if not source_dir.exists():
        raise FileNotFoundError(f"Source folder not found: {source_dir}")

    previous_db_path = sales_loader.DB_PATH
    sales_loader.DB_PATH = str(db_path)
    try:
        recreate_db(db_path)
        log(f"Building DB: {db_path}")
        log(f"Source: {source_dir}")

        # Product Master must load first because later saves validate model list.
        for label, spec in SOURCE_MAP.items():
            file_path = find_source_file(source_dir, spec["folder"], spec["stem"])
            if not file_path:
                log(f"[SKIP] {label}: file not found")
                continue
            try:
                raw = read_file(file_path)
                df = spec["normalizer"](raw)
                result = spec["saver"](df, replace_all=True)
                if isinstance(result, tuple):
                    saved, ignored = result
                else:
                    saved, ignored = len(df), 0
                log(f"[OK] {label}: {saved:,} rows saved, {ignored:,} ignored -> {file_path.name}")
            except Exception as exc:
                log(f"[ERROR] {label}: {exc}")

        for label, spec in KPI_MAP.items():
            file_path = find_source_file(source_dir, spec["folder"], spec["stem"])
            if not file_path:
                log(f"[SKIP] {label}: file not found")
                continue
            try:
                raw = read_file(file_path)
                saved = save_kpi_table(raw, spec["table"])
                log(f"[OK] KPI {label}: {saved:,} rows saved -> {file_path.name}")
            except Exception as exc:
                log(f"[ERROR] KPI {label}: {exc}")

        create_indexes()
        log("Done. Website can now run from SQLite only.")
        return logs
    finally:
        sales_loader.DB_PATH = previous_db_path



def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default=str(DEFAULT_SOURCE_DIR), help="External Database folder")
    parser.add_argument("--db", default=DB_PATH, help="Output SQLite DB path")
    args = parser.parse_args()
    build_database(args.source, args.db, verbose=True)


if __name__ == "__main__":
    main()
