import sqlite3
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = str(BASE_DIR / "data" / "app_data.db")


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def query_df(sql, params=None):
    conn = get_conn()
    df = pd.read_sql_query(sql, conn, params=params or ())
    conn.close()
    return df


def execute_sql(sql, params=None, many=False):
    conn = get_conn()
    cur = conn.cursor()
    if many:
        cur.executemany(sql, params or [])
    else:
        cur.execute(sql, params or ())
    conn.commit()
    conn.close()


def _table_exists(conn, table):
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def _columns(conn, table):
    if not _table_exists(conn, table):
        return []
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _ensure_column(conn, table, column, definition):
    if column not in _columns(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
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
    cols = _columns(conn, "model_master")
    if "model_id" not in cols and "model" in cols:
        cur.execute("ALTER TABLE model_master RENAME TO model_master_old")
        cur.execute(
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
        cur.execute(
            f"""
            INSERT OR REPLACE INTO model_master
                (model_id, product_line, category, hau_model, hq_model, series, series_name, updated_at)
            SELECT UPPER(TRIM(model)), product_line, category, UPPER(TRIM(model)), '', {series_expr}, {series_expr}, COALESCE(updated_at, datetime('now'))
            FROM model_master_old
            WHERE model IS NOT NULL AND TRIM(model) <> ''
            """
        )
        cur.execute("DROP TABLE model_master_old")
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
        cur.execute("UPDATE model_master SET hau_model = model_id WHERE hau_model IS NULL OR TRIM(hau_model) = ''")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS exw_cost (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_id TEXT,
            exw_cost REAL,
            currency TEXT,
            cost_month TEXT,
            uploaded_at TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS landed_cost (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_id TEXT,
            landed_cost REAL,
            currency TEXT,
            cost_month TEXT,
            uploaded_at TEXT
        )
        """
    )
    for table in ["exw_cost", "landed_cost"]:
        _ensure_column(conn, table, "currency", "TEXT")
        _ensure_column(conn, table, "uploaded_at", "TEXT")

    conn.commit()
    conn.close()


# =========================
# helpers
# =========================
def normalize_month_str(v):
    if pd.isna(v):
        return ""
    text = str(v).strip()
    if not text:
        return ""
    dt = pd.to_datetime(text, errors="coerce")
    if pd.notna(dt):
        return dt.strftime("%Y/%m")
    return text


def _norm_model(v):
    if pd.isna(v):
        return ""
    return str(v).strip().upper()


# =========================
# 型号主数据
# =========================
def get_model_master():
    init_db()
    return query_df(
        """
        SELECT model_id, product_line, category
        FROM model_master
        ORDER BY model_id
        """
    )


def upsert_model_master(df: pd.DataFrame):
    init_db()
    rows = []
    for _, r in df.iterrows():
        model_id = _norm_model(r.get("客户型号", r.get("HAU Model", r.get("hau_model", r.get("model_id", "")))))
        product_line = str(r.get("产品线", r.get("product_line", ""))).strip() if pd.notna(r.get("产品线", r.get("product_line", ""))) else ""
        category = str(r.get("品类", r.get("category", ""))).strip() if pd.notna(r.get("品类", r.get("category", ""))) else ""
        hq_model = str(r.get("HQ Model", r.get("hq_model", ""))).strip() if pd.notna(r.get("HQ Model", r.get("hq_model", ""))) else ""
        series = str(r.get("Series", r.get("series", r.get("series_name", "")))).strip() if pd.notna(r.get("Series", r.get("series", r.get("series_name", "")))) else ""
        if model_id:
            rows.append((model_id, product_line, category, model_id, hq_model, series, series))
    if rows:
        execute_sql(
            """
            INSERT OR REPLACE INTO model_master (model_id, product_line, category, hau_model, hq_model, series, series_name, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            rows,
            many=True,
        )


def delete_models(model_ids):
    init_db()
    ids = [_norm_model(x) for x in model_ids if _norm_model(x)]
    if not ids:
        return
    execute_sql(f"DELETE FROM model_master WHERE model_id IN ({','.join(['?'] * len(ids))})", ids)


# =========================
# 写入 EXW / landed
# =========================
def insert_exw(df: pd.DataFrame):
    init_db()
    valid = set(get_model_master()["model_id"].astype(str).str.upper())
    rows = []
    for _, r in df.iterrows():
        model_id = _norm_model(r.get("客户型号", r.get("model_id", "")))
        exw_cost = pd.to_numeric(r.get("工厂结算价", r.get("exw_cost")), errors="coerce")
        cost_month = normalize_month_str(r.get("月份-年", r.get("cost_month", "")))
        currency = str(r.get("currency", r.get("币种", "USD"))).strip().upper() or "USD"
        if model_id and model_id in valid and pd.notna(exw_cost) and cost_month:
            rows.append((model_id, float(exw_cost), currency, cost_month))
    if rows:
        execute_sql("INSERT INTO exw_cost (model_id, exw_cost, currency, cost_month, uploaded_at) VALUES (?, ?, ?, ?, datetime('now'))", rows, many=True)


def insert_landed(df: pd.DataFrame):
    init_db()
    valid = set(get_model_master()["model_id"].astype(str).str.upper())
    rows = []
    for _, r in df.iterrows():
        model_id = _norm_model(r.get("客户型号", r.get("model_id", "")))
        landed_cost = pd.to_numeric(r.get("到库成本", r.get("landed_cost")), errors="coerce")
        cost_month = normalize_month_str(r.get("月份-年", r.get("cost_month", "")))
        currency = str(r.get("currency", r.get("币种", "AUD"))).strip().upper() or "AUD"
        if model_id and model_id in valid and pd.notna(landed_cost) and cost_month:
            rows.append((model_id, float(landed_cost), currency, cost_month))
    if rows:
        execute_sql("INSERT INTO landed_cost (model_id, landed_cost, currency, cost_month, uploaded_at) VALUES (?, ?, ?, ?, datetime('now'))", rows, many=True)


# =========================
# 历史 / 管理
# =========================
def get_exw_history():
    init_db()
    return query_df("SELECT id, model_id, exw_cost, cost_month, currency FROM exw_cost ORDER BY cost_month DESC, model_id, id DESC")


def get_landed_history():
    init_db()
    return query_df("SELECT id, model_id, landed_cost, cost_month, currency FROM landed_cost ORDER BY cost_month DESC, model_id, id DESC")


def update_exw_record(record_id, model_id, exw_cost, cost_month):
    init_db()
    execute_sql("UPDATE exw_cost SET model_id = ?, exw_cost = ?, cost_month = ? WHERE id = ?", (_norm_model(model_id), float(exw_cost), normalize_month_str(cost_month), int(record_id)))


def update_landed_record(record_id, model_id, landed_cost, cost_month):
    init_db()
    execute_sql("UPDATE landed_cost SET model_id = ?, landed_cost = ?, cost_month = ? WHERE id = ?", (_norm_model(model_id), float(landed_cost), normalize_month_str(cost_month), int(record_id)))


def delete_exw_records(record_ids):
    init_db()
    ids = [int(x) for x in record_ids]
    if ids:
        execute_sql(f"DELETE FROM exw_cost WHERE id IN ({','.join(['?'] * len(ids))})", ids)


def delete_landed_records(record_ids):
    init_db()
    ids = [int(x) for x in record_ids]
    if ids:
        execute_sql(f"DELETE FROM landed_cost WHERE id IN ({','.join(['?'] * len(ids))})", ids)


# =========================
# 最新成本
# =========================
def get_latest_exw():
    init_db()
    return query_df(
        """
        SELECT e.model_id, m.product_line, m.category, e.cost_month, e.exw_cost
        FROM exw_cost e
        INNER JOIN (SELECT model_id, MAX(cost_month) AS max_month FROM exw_cost GROUP BY model_id) t
            ON e.model_id = t.model_id AND e.cost_month = t.max_month
        INNER JOIN (SELECT model_id, cost_month, MAX(id) AS max_id FROM exw_cost GROUP BY model_id, cost_month) x
            ON e.model_id = x.model_id AND e.cost_month = x.cost_month AND e.id = x.max_id
        INNER JOIN model_master m ON e.model_id = m.model_id
        ORDER BY e.model_id
        """
    )


def get_latest_landed():
    init_db()
    return query_df(
        """
        SELECT l.model_id, m.product_line, m.category, l.cost_month, l.landed_cost
        FROM landed_cost l
        INNER JOIN (SELECT model_id, MAX(cost_month) AS max_month FROM landed_cost GROUP BY model_id) t
            ON l.model_id = t.model_id AND l.cost_month = t.max_month
        INNER JOIN (SELECT model_id, cost_month, MAX(id) AS max_id FROM landed_cost GROUP BY model_id, cost_month) x
            ON l.model_id = x.model_id AND l.cost_month = x.cost_month AND l.id = x.max_id
        INNER JOIN model_master m ON l.model_id = m.model_id
        ORDER BY l.model_id
        """
    )
