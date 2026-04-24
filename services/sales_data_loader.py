from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = str(BASE_DIR / "data" / "app_data.db")

# ============================================================
# Shared DB connection / schema
# ============================================================

def get_conn():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _columns(conn: sqlite3.Connection, table: str) -> list[str]:
    if not _table_exists(conn, table):
        return []
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    cols = _columns(conn, table)
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def init_product_master_db() -> None:
    """Universal product table. HAU Model is the key; internally model_id = HAU Model for legacy modules."""
    with get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS model_master (
                model_id TEXT PRIMARY KEY,
                product_line TEXT,
                category TEXT,
                hau_model TEXT,
                hq_model TEXT,
                series TEXT,
                series_name TEXT,
                updated_at TEXT
            )
            """
        )
        # Safe migration from older variants.
        cols = _columns(conn, "model_master")
        if "model_id" not in cols and "model" in cols:
            # Migrate older table that used `model` as the key.
            conn.execute("ALTER TABLE model_master RENAME TO model_master_old")
            conn.execute(
                """
                CREATE TABLE model_master (
                    model_id TEXT PRIMARY KEY,
                    product_line TEXT,
                    category TEXT,
                    hau_model TEXT,
                    hq_model TEXT,
                    series TEXT,
                    series_name TEXT,
                    updated_at TEXT
                )
                """
            )
            old_cols = _columns(conn, "model_master_old")
            series_expr = "series_name" if "series_name" in old_cols else "''"
            conn.execute(
                f"""
                INSERT OR REPLACE INTO model_master
                    (model_id, product_line, category, hau_model, hq_model, series, series_name, updated_at)
                SELECT UPPER(TRIM(model)), product_line, category, UPPER(TRIM(model)), '', {series_expr}, {series_expr},
                       COALESCE(updated_at, datetime('now'))
                FROM model_master_old
                WHERE model IS NOT NULL AND TRIM(model) <> ''
                """
            )
            conn.execute("DROP TABLE model_master_old")
        else:
            for col, definition in {
                "product_line": "TEXT",
                "category": "TEXT",
                "hau_model": "TEXT",
                "hq_model": "TEXT",
                "series": "TEXT",
                "series_name": "TEXT",
                "updated_at": "TEXT",
            }.items():
                _ensure_column(conn, "model_master", col, definition)
            conn.execute("UPDATE model_master SET hau_model = model_id WHERE hau_model IS NULL OR TRIM(hau_model) = ''")
            conn.execute("UPDATE model_master SET series = series_name WHERE (series IS NULL OR TRIM(series) = '') AND series_name IS NOT NULL")
            conn.execute("UPDATE model_master SET series_name = series WHERE (series_name IS NULL OR TRIM(series_name) = '') AND series IS NOT NULL")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_model_master_hau ON model_master(hau_model)")
        conn.commit()


def init_cost_db() -> None:
    with get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS exw_cost (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_id TEXT NOT NULL,
                exw_cost REAL NOT NULL,
                currency TEXT,
                cost_month TEXT NOT NULL,
                uploaded_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS landed_cost (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_id TEXT NOT NULL,
                landed_cost REAL NOT NULL,
                currency TEXT,
                cost_month TEXT NOT NULL,
                uploaded_at TEXT
            )
            """
        )
        for table in ["exw_cost", "landed_cost"]:
            _ensure_column(conn, table, "currency", "TEXT")
            _ensure_column(conn, table, "uploaded_at", "TEXT")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_model ON {table}(model_id)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_month ON {table}(cost_month)")
        conn.commit()


def init_store_db() -> None:
    with get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS store_locations (
                business_name TEXT PRIMARY KEY,
                retailer TEXT,
                region TEXT,
                latitude REAL,
                longitude REAL,
                updated_at TEXT
            )
            """
        )
        for col, definition in {
            "retailer": "TEXT",
            "region": "TEXT",
            "latitude": "REAL",
            "longitude": "REAL",
            "updated_at": "TEXT",
        }.items():
            _ensure_column(conn, "store_locations", col, definition)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_store_locations_retailer ON store_locations(retailer)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_store_locations_region ON store_locations(region)")
        conn.commit()


def init_sales_by_stores_db() -> None:
    with get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sales_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sales_date TEXT NOT NULL,
                business_name TEXT NOT NULL,
                model TEXT NOT NULL,
                sales REAL NOT NULL,
                uploaded_at TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_records_date ON sales_records(sales_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_records_store ON sales_records(business_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_records_model ON sales_records(model)")
        conn.commit()


def init_sales_agent_db() -> None:
    with get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sales_agent_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sales_date TEXT NOT NULL,
                channel TEXT,
                model TEXT NOT NULL,
                avl_soh_amt REAL,
                soo_amt REAL,
                daily_sales_amt REAL,
                price REAL,
                sum_avl_soh REAL,
                sum_soo REAL,
                sales_qty REAL,
                uploaded_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_agent_date ON sales_agent_records(sales_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_agent_channel ON sales_agent_records(channel)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_agent_model ON sales_agent_records(model)")
        conn.commit()


def init_all_shared_db() -> None:
    init_product_master_db()
    init_cost_db()
    init_store_db()
    init_sales_by_stores_db()
    init_sales_agent_db()

# Backwards-compatible names
init_model_master_db = init_product_master_db

# ============================================================
# Generic helpers
# ============================================================

def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip().lower() for c in out.columns]
    return out


def _rename_by_aliases(df: pd.DataFrame, aliases_map: dict[str, list[str]]) -> pd.DataFrame:
    out = df.copy()
    current = set(out.columns)
    rename_map = {}
    for target, aliases in aliases_map.items():
        for alias in aliases:
            a = alias.lower().strip()
            if a in current:
                rename_map[a] = target
                break
    return out.rename(columns=rename_map)


def _load_raw_file(file_obj_or_path) -> pd.DataFrame:
    if hasattr(file_obj_or_path, "name"):
        name = str(file_obj_or_path.name).lower()
        file_obj_or_path.seek(0)
        if name.endswith(".csv"):
            return pd.read_csv(file_obj_or_path)
        try:
            return pd.read_excel(file_obj_or_path, sheet_name="Export")
        except Exception:
            file_obj_or_path.seek(0)
            return pd.read_excel(file_obj_or_path)

    path = Path(file_obj_or_path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    try:
        return pd.read_excel(path, sheet_name="Export")
    except Exception:
        return pd.read_excel(path)


def _norm_model(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip().upper()


def _norm_text(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


def _norm_store(x) -> str:
    return _norm_text(x).upper()


def normalize_month_str(v) -> str:
    if pd.isna(v):
        return ""
    text = str(v).strip()
    if not text:
        return ""
    dt = pd.to_datetime(text, errors="coerce")
    if pd.notna(dt):
        return dt.strftime("%Y/%m")
    return text


def _valid_models_set() -> set[str]:
    init_product_master_db()
    with get_conn() as conn:
        df = pd.read_sql_query("SELECT model_id FROM model_master", conn)
    return set(df["model_id"].dropna().astype(str).str.strip().str.upper())


def _filter_valid_models(df: pd.DataFrame, model_col: str) -> tuple[pd.DataFrame, int]:
    valid = _valid_models_set()
    if not valid:
        return df.iloc[0:0].copy(), int(len(df))
    out = df.copy()
    out[model_col] = out[model_col].astype(str).str.strip().str.upper()
    mask = out[model_col].isin(valid)
    return out[mask].copy(), int((~mask).sum())

# ============================================================
# Product Master
# ============================================================

PRODUCT_MASTER_ALIASES = {
    "product_line": ["product line", "product_line", "line", "产品线", "productline"],
    "category": ["category", "product category", "product_category", "品类", "类别"],
    "hau_model": ["hau model", "hau_model", "model", "model_id", "model id", "sku", "客户型号", "hisense model"],
    "hq_model": ["hq model", "hq_model", "总部型号", "factory model", "factory_model"],
    "series": ["series", "series name", "series_name", "系列", "系列名"],
}
PRODUCT_MASTER_COLUMNS = ["product_line", "category", "hau_model", "hq_model", "series"]


def normalize_product_master_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = _rename_by_aliases(_standardize_columns(df_raw), PRODUCT_MASTER_ALIASES)
    missing = [c for c in ["product_line", "category", "hau_model"] if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required Product Master columns: {missing}")
    for c in PRODUCT_MASTER_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    out = df[PRODUCT_MASTER_COLUMNS].copy()
    out["hau_model"] = out["hau_model"].apply(_norm_model)
    for c in ["product_line", "category", "hq_model", "series"]:
        out[c] = out[c].apply(_norm_text)
    out = out[(out["hau_model"] != "") & (out["hau_model"].str.lower() != "nan")]
    return out.drop_duplicates(subset=["hau_model"], keep="last").reset_index(drop=True)


def save_product_master_records(df: pd.DataFrame, replace_all: bool = True) -> None:
    init_product_master_db()
    to_save = normalize_product_master_df(df)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_conn() as conn:
        if replace_all:
            conn.execute("DELETE FROM model_master")
        for _, r in to_save.iterrows():
            conn.execute(
                """
                INSERT INTO model_master
                    (model_id, product_line, category, hau_model, hq_model, series, series_name, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(model_id) DO UPDATE SET
                    product_line=excluded.product_line,
                    category=excluded.category,
                    hau_model=excluded.hau_model,
                    hq_model=excluded.hq_model,
                    series=excluded.series,
                    series_name=excluded.series_name,
                    updated_at=excluded.updated_at
                """,
                (r["hau_model"], r["product_line"], r["category"], r["hau_model"], r["hq_model"], r["series"], r["series"], now),
            )
        conn.commit()
    clear_all_caches()


def read_product_master_records() -> pd.DataFrame:
    init_product_master_db()
    with get_conn() as conn:
        df = pd.read_sql_query(
            """
            SELECT product_line, category, model_id AS hau_model, hq_model, COALESCE(series, series_name, '') AS series, updated_at
            FROM model_master
            ORDER BY product_line, category, model_id
            """,
            conn,
        )
    return df


def clear_product_master_records() -> None:
    init_product_master_db()
    with get_conn() as conn:
        conn.execute("DELETE FROM model_master")
        conn.commit()
    clear_all_caches()

# Backwards-compatible names for existing Sales Agent code.
MODEL_MASTER_EXPECTED = ["model", "product_line", "category", "series_name"]

def normalize_model_master_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    pm = normalize_product_master_df(df_raw)
    return pm.rename(columns={"hau_model": "model", "series": "series_name"})[["model", "product_line", "category", "series_name"]]


def save_model_master_records(df: pd.DataFrame, replace_all: bool = True) -> None:
    if "model" in df.columns and "hau_model" not in df.columns:
        df = df.rename(columns={"model": "hau_model", "series_name": "series"})
    save_product_master_records(df, replace_all=replace_all)


def read_model_master_records() -> pd.DataFrame:
    pm = read_product_master_records()
    return pm.rename(columns={"hau_model": "model", "series": "series_name"})[["model", "product_line", "category", "series_name"]]


def clear_model_master_records() -> None:
    clear_product_master_records()


def upsert_model_master_editor(df_editor: pd.DataFrame) -> None:
    if df_editor is None:
        return
    save_model_master_records(df_editor, replace_all=True)


def _load_model_master() -> pd.DataFrame:
    df = read_model_master_records()
    df["model"] = df["model"].astype(str).str.strip().str.upper()
    return df.drop_duplicates(subset=["model"], keep="last")

# ============================================================
# Cost
# ============================================================

EXW_ALIASES = {
    "model_id": ["model", "model_id", "hau model", "hau_model", "客户型号", "sku"],
    "exw_cost": ["exw", "exw cost", "exw_cost", "工厂结算价", "cost"],
    "currency": ["currency", "cur", "币种"],
    "cost_month": ["cost month", "cost_month", "cost time", "成本时间", "月份-年", "month", "date"],
}
LANDED_ALIASES = {
    "model_id": ["model", "model_id", "hau model", "hau_model", "客户型号", "sku"],
    "landed_cost": ["landed cost", "landed_cost", "landed", "到库成本", "cost"],
    "currency": ["currency", "cur", "币种"],
    "cost_month": ["cost month", "cost_month", "cost time", "成本时间", "月份-年", "month", "date"],
}


def normalize_exw_cost_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = _rename_by_aliases(_standardize_columns(df_raw), EXW_ALIASES)
    missing = [c for c in ["model_id", "exw_cost", "cost_month"] if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required EXW columns: {missing}")
    if "currency" not in df.columns:
        df["currency"] = "USD"
    out = df[["model_id", "exw_cost", "currency", "cost_month"]].copy()
    out["model_id"] = out["model_id"].apply(_norm_model)
    out["exw_cost"] = pd.to_numeric(out["exw_cost"], errors="coerce")
    out["currency"] = out["currency"].apply(lambda x: _norm_text(x).upper() or "USD")
    out["cost_month"] = out["cost_month"].apply(normalize_month_str)
    out = out.dropna(subset=["exw_cost"])
    out = out[(out["model_id"] != "") & (out["cost_month"] != "")]
    return out.reset_index(drop=True)


def normalize_landed_cost_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = _rename_by_aliases(_standardize_columns(df_raw), LANDED_ALIASES)
    missing = [c for c in ["model_id", "landed_cost", "cost_month"] if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required Landed Cost columns: {missing}")
    if "currency" not in df.columns:
        df["currency"] = "AUD"
    out = df[["model_id", "landed_cost", "currency", "cost_month"]].copy()
    out["model_id"] = out["model_id"].apply(_norm_model)
    out["landed_cost"] = pd.to_numeric(out["landed_cost"], errors="coerce")
    out["currency"] = out["currency"].apply(lambda x: _norm_text(x).upper() or "AUD")
    out["cost_month"] = out["cost_month"].apply(normalize_month_str)
    out = out.dropna(subset=["landed_cost"])
    out = out[(out["model_id"] != "") & (out["cost_month"] != "")]
    return out.reset_index(drop=True)


def save_exw_cost_records(df: pd.DataFrame, replace_all: bool = False) -> tuple[int, int]:
    init_all_shared_db()
    out = normalize_exw_cost_df(df)
    out, ignored = _filter_valid_models(out, "model_id")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out["uploaded_at"] = now
    with get_conn() as conn:
        if replace_all:
            conn.execute("DELETE FROM exw_cost")
        out.to_sql("exw_cost", conn, if_exists="append", index=False)
        conn.commit()
    clear_all_caches()
    return int(len(out)), ignored


def save_landed_cost_records(df: pd.DataFrame, replace_all: bool = False) -> tuple[int, int]:
    init_all_shared_db()
    out = normalize_landed_cost_df(df)
    out, ignored = _filter_valid_models(out, "model_id")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out["uploaded_at"] = now
    with get_conn() as conn:
        if replace_all:
            conn.execute("DELETE FROM landed_cost")
        out.to_sql("landed_cost", conn, if_exists="append", index=False)
        conn.commit()
    clear_all_caches()
    return int(len(out)), ignored


def read_exw_cost_records() -> pd.DataFrame:
    init_cost_db()
    with get_conn() as conn:
        return pd.read_sql_query("SELECT id, model_id, exw_cost, currency, cost_month, uploaded_at FROM exw_cost ORDER BY cost_month DESC, model_id, id DESC", conn)


def read_landed_cost_records() -> pd.DataFrame:
    init_cost_db()
    with get_conn() as conn:
        return pd.read_sql_query("SELECT id, model_id, landed_cost, currency, cost_month, uploaded_at FROM landed_cost ORDER BY cost_month DESC, model_id, id DESC", conn)


def clear_exw_cost_records() -> None:
    init_cost_db()
    with get_conn() as conn:
        conn.execute("DELETE FROM exw_cost")
        conn.commit()
    clear_all_caches()


def clear_landed_cost_records() -> None:
    init_cost_db()
    with get_conn() as conn:
        conn.execute("DELETE FROM landed_cost")
        conn.commit()
    clear_all_caches()

# ============================================================
# Store Master / Sales by Stores
# ============================================================

STORE_ALIASES = {
    "business_name": ["store name", "store_name", "business name", "business_name", "门店名", "store"],
    "region": ["region", "store region", "门店地区", "地区", "state", "suburb"],
    "retailer": ["channel", "retailer", "store channel", "门店渠道", "account", "banner"],
    "latitude": ["latitude", "lat", "y", "纬度"],
    "longitude": ["longitude", "lng", "lon", "long", "x", "经度"],
}
SALES_STORE_ALIASES = {
    "sales_date": ["date", "sales date", "sales_date", "week", "month", "period"],
    "business_name": ["business name", "business_name", "store name", "store", "customer", "customer name", "门店名"],
    "model": ["model", "sku", "hau model", "hau_model", "product model", "客户型号"],
    "sales": ["sales", "qty", "quantity", "sell out", "sales qty", "units", "销量"],
}


def normalize_store_master_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = _rename_by_aliases(_standardize_columns(df_raw), STORE_ALIASES)
    missing = [c for c in ["business_name", "retailer", "latitude", "longitude"] if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required Store Master columns: {missing}")
    if "region" not in df.columns:
        df["region"] = ""
    out = df[["business_name", "region", "retailer", "latitude", "longitude"]].copy()
    out["business_name"] = out["business_name"].apply(_norm_store)
    out["region"] = out["region"].apply(_norm_text)
    out["retailer"] = out["retailer"].apply(_norm_text)
    out["latitude"] = pd.to_numeric(out["latitude"], errors="coerce")
    out["longitude"] = pd.to_numeric(out["longitude"], errors="coerce")
    out = out.dropna(subset=["latitude", "longitude"])
    out = out[out["business_name"] != ""]
    return out.drop_duplicates(subset=["business_name"], keep="last").reset_index(drop=True)


def save_store_master_records(df: pd.DataFrame, replace_all: bool = True) -> None:
    init_store_db()
    out = normalize_store_master_df(df)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out["updated_at"] = now
    with get_conn() as conn:
        if replace_all:
            conn.execute("DELETE FROM store_locations")
        for _, r in out.iterrows():
            conn.execute(
                """
                INSERT INTO store_locations (business_name, region, retailer, latitude, longitude, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(business_name) DO UPDATE SET
                    region=excluded.region,
                    retailer=excluded.retailer,
                    latitude=excluded.latitude,
                    longitude=excluded.longitude,
                    updated_at=excluded.updated_at
                """,
                (r["business_name"], r["region"], r["retailer"], float(r["latitude"]), float(r["longitude"]), now),
            )
        conn.commit()
    clear_all_caches()


def read_store_master_records() -> pd.DataFrame:
    init_store_db()
    with get_conn() as conn:
        return pd.read_sql_query(
            "SELECT business_name, region, retailer, latitude, longitude, updated_at FROM store_locations ORDER BY retailer, region, business_name",
            conn,
        )


def clear_store_master_records() -> None:
    init_store_db()
    with get_conn() as conn:
        conn.execute("DELETE FROM store_locations")
        conn.commit()
    clear_all_caches()


def normalize_sales_by_stores_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = _rename_by_aliases(_standardize_columns(df_raw), SALES_STORE_ALIASES)
    missing = [c for c in ["sales_date", "business_name", "model", "sales"] if c not in df.columns]
    if missing:
        raise ValueError(f"Sales by Stores file missing required columns: {missing}")
    out = df[["sales_date", "business_name", "model", "sales"]].copy()
    out["sales_date"] = pd.to_datetime(out["sales_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    out["business_name"] = out["business_name"].apply(_norm_store)
    out["model"] = out["model"].apply(_norm_model)
    out["sales"] = pd.to_numeric(out["sales"], errors="coerce").fillna(0)
    out = out.dropna(subset=["sales_date"])
    out = out[(out["business_name"] != "") & (out["model"] != "")]
    return out.reset_index(drop=True)


def save_sales_by_stores_records(df: pd.DataFrame, replace_all: bool = False) -> tuple[int, int]:
    init_all_shared_db()
    out = normalize_sales_by_stores_df(df)
    out, ignored = _filter_valid_models(out, "model")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out["uploaded_at"] = now
    with get_conn() as conn:
        if replace_all:
            conn.execute("DELETE FROM sales_records")
        out.to_sql("sales_records", conn, if_exists="append", index=False)
        conn.commit()
    clear_all_caches()
    return int(len(out)), ignored


def read_sales_by_stores_records() -> pd.DataFrame:
    init_sales_by_stores_db()
    with get_conn() as conn:
        return pd.read_sql_query("SELECT id, sales_date, business_name, model, sales, uploaded_at FROM sales_records ORDER BY sales_date DESC, id DESC", conn)


def clear_sales_by_stores_records() -> None:
    init_sales_by_stores_db()
    with get_conn() as conn:
        conn.execute("DELETE FROM sales_records")
        conn.commit()
    clear_all_caches()

# Names expected by Sales Heatmap common.py
read_store_locations = read_store_master_records
read_sales_records = read_sales_by_stores_records
save_store_locations = save_store_master_records
save_sales_records = save_sales_by_stores_records
clear_all_store_locations = clear_store_master_records
clear_all_sales_records = clear_sales_by_stores_records

# ============================================================
# Sales Agent
# ============================================================

SALES_AGENT_ALIASES = {
    "date": ["thstats-weeklysalesamt[salesrangedate]", "salesrangedate", "sales range date", "date", "sales_date", "sales date"],
    "channel": ["thstats-weeklysalesamt[channel]", "channel", "retailer", "account", "banner", "retailer / channel"],
    "model": ["thstats-weeklysalesamt[model]", "model", "sku", "hau model", "hau_model", "model_id", "model id"],
    "avl_soh_amt": ["thstats-weeklysalesamt[avlsoh_amt]", "avlsoh_amt", "avl soh amt"],
    "soo_amt": ["thstats-weeklysalesamt[soo_amt]", "soo_amt", "soo amt"],
    "daily_sales_amt": ["thstats-weeklysalesamt[dailysales_amt]", "dailysales_amt", "daily sales amt"],
    "price": ["[sumprice]", "sumprice", "price", "avg price", "average price", "asp"],
    "sum_avl_soh": ["[sumavlsoh]", "sumavlsoh", "sum_avl_soh", "avl soh", "soh"],
    "sum_soo": ["[sumsoo]", "sumsoo", "sum_soo", "soo"],
    "sales_qty": ["[sumdailysales_qty]", "sumdailysales_qty", "sales_qty", "sales qty", "qty", "units", "volume"],
}
SALES_AGENT_EXPECTED = ["date", "channel", "model", "avl_soh_amt", "soo_amt", "daily_sales_amt", "price", "sum_avl_soh", "sum_soo", "sales_qty"]
EXPECTED = SALES_AGENT_EXPECTED


def normalize_sales_agent_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = _rename_by_aliases(_standardize_columns(df_raw), SALES_AGENT_ALIASES)
    missing = [c for c in SALES_AGENT_EXPECTED if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required Sales Agent columns: {missing}")
    out = df[SALES_AGENT_EXPECTED].copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date", "model"])
    out["channel"] = out["channel"].apply(_norm_text)
    out["model"] = out["model"].apply(_norm_model)
    for col in ["avl_soh_amt", "soo_amt", "daily_sales_amt", "price", "sum_avl_soh", "sum_soo", "sales_qty"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out[out["model"] != ""]
    out = out.rename(columns={"date": "sales_date"})
    out["sales_date"] = out["sales_date"].dt.strftime("%Y-%m-%d")
    return out.reset_index(drop=True)


def save_sales_agent_records(df: pd.DataFrame, replace_all: bool = False) -> tuple[int, int]:
    init_all_shared_db()
    out = normalize_sales_agent_df(df)
    out, ignored = _filter_valid_models(out, "model")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out["uploaded_at"] = now
    with get_conn() as conn:
        if replace_all:
            conn.execute("DELETE FROM sales_agent_records")
        out.to_sql("sales_agent_records", conn, if_exists="append", index=False)
        conn.commit()
    clear_all_caches()
    return int(len(out)), ignored


def read_sales_agent_records() -> pd.DataFrame:
    init_sales_agent_db()
    with get_conn() as conn:
        return pd.read_sql_query(
            """
            SELECT id, sales_date, channel, model, avl_soh_amt, soo_amt, daily_sales_amt,
                   price, sum_avl_soh, sum_soo, sales_qty, uploaded_at
            FROM sales_agent_records
            ORDER BY sales_date DESC, id DESC
            """,
            conn,
        )


def clear_sales_agent_records() -> None:
    init_sales_agent_db()
    with get_conn() as conn:
        conn.execute("DELETE FROM sales_agent_records")
        conn.commit()
    clear_all_caches()


def get_sales_agent_summary() -> dict:
    init_sales_agent_db()
    with get_conn() as conn:
        row_count = conn.execute("SELECT COUNT(*) FROM sales_agent_records").fetchone()[0]
        min_date, max_date = conn.execute("SELECT MIN(sales_date), MAX(sales_date) FROM sales_agent_records").fetchone()
    return {"rows": int(row_count), "min_date": min_date or "-", "max_date": max_date or "-"}


def enrich_sales_agent_data(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        return out
    out["sales_date"] = pd.to_datetime(out["sales_date"], errors="coerce")
    out["model"] = out["model"].apply(_norm_model)
    model_master = _load_model_master()
    # Strict shared rule: if HAU Model does not exist in Product Master, it does not count.
    out = out.merge(model_master, on="model", how="inner")
    out["year"] = out["sales_date"].dt.year
    out["month"] = out["sales_date"].dt.month
    iso = out["sales_date"].dt.isocalendar()
    out["week"] = iso.week.astype("Int64")
    out["sales_value_est"] = out["sales_qty"].fillna(0) * out["price"].fillna(0)
    return out


@st.cache_data(show_spinner=False)
def load_sales_agent_data() -> pd.DataFrame:
    init_all_shared_db()
    raw = read_sales_agent_records()
    if raw.empty:
        return raw
    return enrich_sales_agent_data(raw)


@st.cache_data(show_spinner=False)
def load_sales_data(file_path: str) -> pd.DataFrame:
    raw = _load_raw_file(file_path)
    out = normalize_sales_agent_df(raw)
    out["sales_date"] = pd.to_datetime(out["sales_date"])
    return enrich_sales_agent_data(out)


def summarize_dataset(df: pd.DataFrame) -> dict:
    min_date = df["sales_date"].min() if "sales_date" in df.columns and not df.empty else pd.NaT
    max_date = df["sales_date"].max() if "sales_date" in df.columns and not df.empty else pd.NaT
    return {
        "rows": int(len(df)),
        "channels": int(df["channel"].nunique(dropna=True)) if "channel" in df.columns else 0,
        "models": int(df["model"].nunique(dropna=True)) if "model" in df.columns else 0,
        "product_lines": int(df["product_line"].nunique(dropna=True)) if "product_line" in df.columns else 0,
        "categories": int(df["category"].nunique(dropna=True)) if "category" in df.columns else 0,
        "series": int(df["series_name"].nunique(dropna=True)) if "series_name" in df.columns else 0,
        "min_date": min_date.date().isoformat() if pd.notna(min_date) else "-",
        "max_date": max_date.date().isoformat() if pd.notna(max_date) else "-",
        "columns": df.columns.tolist(),
    }

# ============================================================
# Summary / clear all
# ============================================================

def table_count(table: str) -> int:
    try:
        with get_conn() as conn:
            if not _table_exists(conn, table):
                return 0
            return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    except Exception:
        return 0


def clear_all_database_records() -> None:
    init_all_shared_db()
    with get_conn() as conn:
        for table in ["model_master", "exw_cost", "landed_cost", "store_locations", "sales_records", "sales_agent_records"]:
            if _table_exists(conn, table):
                conn.execute(f"DELETE FROM {table}")
        conn.commit()
    clear_all_caches()


def clear_all_caches() -> None:
    try:
        load_sales_agent_data.clear()
    except Exception:
        pass
    try:
        load_sales_data.clear()
    except Exception:
        pass

