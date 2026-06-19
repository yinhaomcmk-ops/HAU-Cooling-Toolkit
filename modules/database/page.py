from __future__ import annotations

import io
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from services.sales_data_loader import (
    DB_PATH,
    _load_raw_file,
    clear_all_database_records,
    clear_exw_cost_records,
    clear_landed_cost_records,
    clear_product_master_records,
    clear_sales_agent_records,
    clear_sales_by_stores_records,
    clear_store_master_records,
    clear_highlight_store_records,
    get_sales_agent_summary,
    init_all_shared_db,
    normalize_exw_cost_df,
    normalize_landed_cost_df,
    normalize_product_master_df,
    normalize_sales_agent_df,
    normalize_sales_by_stores_df,
    normalize_store_master_df,
    normalize_highlight_store_df,
    read_exw_cost_records,
    read_landed_cost_records,
    read_product_master_records,
    read_sales_agent_records,
    read_sales_by_stores_records,
    read_store_master_records,
    read_highlight_store_records,
    save_exw_cost_records,
    save_landed_cost_records,
    save_product_master_records,
    save_sales_agent_records,
    save_sales_by_stores_records,
    save_store_master_records,
    save_highlight_store_records,
    table_count,
)


# -----------------------------
# SQLite lock protection
# -----------------------------
_ORIGINAL_SQLITE_CONNECT = sqlite3.connect


def _connect_with_busy_timeout(*args, **kwargs):
    # Streamlit reruns + OneDrive can briefly keep the SQLite file locked.
    # A longer timeout prevents intermittent "database is locked" failures.
    kwargs.setdefault("timeout", 30)
    conn = _ORIGINAL_SQLITE_CONNECT(*args, **kwargs)
    try:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
    except Exception:
        pass
    return conn


sqlite3.connect = _connect_with_busy_timeout


def _run_db_with_retry(func, *args, retries: int = 5, delay: float = 0.8, **kwargs):
    last_exc = None
    for i in range(retries):
        try:
            return func(*args, **kwargs)
        except sqlite3.OperationalError as exc:
            last_exc = exc
            if "database is locked" not in str(exc).lower():
                raise
            time.sleep(delay * (i + 1))
    raise last_exc

st.title("Database")
st.caption(
    "Central shared database. HAU Model is the key across Product Master, Cost, Sales by Stores and Sales Agent. "
    "Rows whose HAU Model is not in Product Master are ignored when saving shared analysis data."
)

_run_db_with_retry(init_all_shared_db)

# -----------------------------
# Utilities
# -----------------------------
def to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Data") -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    return buffer.getvalue()


def ensure_download_df(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=columns)
    out = df.copy()
    for c in columns:
        if c not in out.columns:
            out[c] = ""
    return out[columns]


def show_upload_result(saved: int | None = None, ignored: int | None = None, action: str = "saved"):
    if ignored is None:
        st.success(f"Data {action}.")
    else:
        st.success(f"Rows {action}: {saved:,}. Ignored because HAU Model not in Product Master: {ignored:,}.")


def render_clear_button(label: str, clear_func, key: str):
    with st.expander("Danger zone", expanded=False):
        st.warning("This action cannot be undone.")
        if st.button(label, key=key, use_container_width=True):
            clear_func()
            st.success("Cleared.")
            st.rerun()


# -----------------------------
# Cost file helpers
# -----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
COST_DATA_DIR = PROJECT_ROOT / "data"
PRODUCT_MASTER_FILE = COST_DATA_DIR / "product_model_master.xlsx"
EXW_COST_FILE = COST_DATA_DIR / "exw_cost.xlsx"
LANDED_COST_FILE = COST_DATA_DIR / "landed_cost.xlsx"
STORE_MASTER_FILE = COST_DATA_DIR / "store_master.xlsx"
HIGHLIGHT_STORE_FILE = COST_DATA_DIR / "highlight_store.xlsx"
SALES_BY_STORES_FILE = COST_DATA_DIR / "sales_summary_store.xlsx"
SALES_AGENT_FILE = COST_DATA_DIR / "sales_agent.xlsx"


STORE_MASTER_COLUMNS = ["business_name", "business_name_short", "region", "retailer", "latitude", "longitude"]


def _clean_store_name(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip().upper()


def _normalise_store_master_with_short(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise Store Master and preserve business_name_short for map labels."""
    if df is None or df.empty:
        return pd.DataFrame(columns=STORE_MASTER_COLUMNS)

    raw = df.copy()
    raw.columns = [str(c).strip().lower() for c in raw.columns]

    rename_map = {}
    candidates = {
        "business_name": ["business_name", "business name", "store", "store name", "customer", "customer name"],
        "business_name_short": ["business_name_short", "business name short", "short", "short name", "store short", "store_short", "short_store_name"],
        "region": ["region", "state", "area", "territory"],
        "retailer": ["retailer", "channel", "banner", "account", "group"],
        "latitude": ["latitude", "lat", "y"],
        "longitude": ["longitude", "lon", "lng", "long", "x"],
    }
    for target, opts in candidates.items():
        for opt in opts:
            if opt in raw.columns:
                rename_map[opt] = target
                break
    raw = raw.rename(columns=rename_map)

    for c in STORE_MASTER_COLUMNS:
        if c not in raw.columns:
            raw[c] = ""

    out = raw[STORE_MASTER_COLUMNS].copy()
    out["business_name"] = out["business_name"].apply(_clean_store_name)
    out["business_name_short"] = out["business_name_short"].fillna("").astype(str).str.strip()
    out["region"] = out["region"].fillna("").astype(str).str.strip()
    out["retailer"] = out["retailer"].fillna("Unknown").astype(str).str.strip().replace("", "Unknown")
    out["latitude"] = pd.to_numeric(out["latitude"], errors="coerce")
    out["longitude"] = pd.to_numeric(out["longitude"], errors="coerce")
    out = out[(out["business_name"] != "") & out["latitude"].notna() & out["longitude"].notna()]
    out = out.drop_duplicates(subset=["business_name"], keep="last")
    return out.reset_index(drop=True)


def _ensure_store_master_short_schema(conn):
    cols = pd.read_sql_query("PRAGMA table_info(store_locations)", conn)["name"].tolist()
    if "business_name_short" not in cols:
        conn.execute("ALTER TABLE store_locations ADD COLUMN business_name_short TEXT")
    if "region" not in cols:
        conn.execute("ALTER TABLE store_locations ADD COLUMN region TEXT")


def save_store_master_records_with_short(df: pd.DataFrame, replace_all: bool = False):
    out = _normalise_store_master_with_short(df)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with sqlite3.connect(DB_PATH) as conn:
        _ensure_store_master_short_schema(conn)
        if replace_all:
            conn.execute("DELETE FROM store_locations")
        for _, row in out.iterrows():
            conn.execute(
                """
                INSERT INTO store_locations
                    (business_name, business_name_short, region, retailer, latitude, longitude, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(business_name) DO UPDATE SET
                    business_name_short=excluded.business_name_short,
                    region=excluded.region,
                    retailer=excluded.retailer,
                    latitude=excluded.latitude,
                    longitude=excluded.longitude,
                    updated_at=excluded.updated_at
                """,
                (
                    row["business_name"],
                    row["business_name_short"],
                    row["region"],
                    row["retailer"],
                    float(row["latitude"]),
                    float(row["longitude"]),
                    now,
                ),
            )
        conn.commit()
    return len(out)


def read_store_master_records_with_short() -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        _ensure_store_master_short_schema(conn)
        return pd.read_sql_query(
            """
            SELECT business_name, business_name_short, region, retailer, latitude, longitude, updated_at
            FROM store_locations
            ORDER BY business_name
            """,
            conn,
        )



def _read_cost_source_file(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    return pd.read_excel(path)


def _write_cost_source_file(df: pd.DataFrame, path: Path, sheet_name: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    out = df.copy()
    if "cost_month" in out.columns:
        out["cost_month"] = out["cost_month"].astype(str).str[:10]
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        out.to_excel(writer, index=False, sheet_name=sheet_name[:31])


def _auto_sync_cost_from_files() -> None:
    """Load fixed cost source files from /data into SQLite on app start."""
    synced = []
    try:
        if EXW_COST_FILE.exists():
            exw_df = normalize_exw_cost_df(_read_cost_source_file(EXW_COST_FILE))
            save_exw_cost_records(exw_df, replace_all=True)
            synced.append(f"EXW: {len(exw_df):,}")
        if LANDED_COST_FILE.exists():
            landed_df = normalize_landed_cost_df(_read_cost_source_file(LANDED_COST_FILE))
            save_landed_cost_records(landed_df, replace_all=True)
            synced.append(f"Landed: {len(landed_df):,}")
        if synced:
            st.session_state["cost_file_sync_message"] = " | ".join(synced)
    except Exception as exc:
        st.session_state["cost_file_sync_error"] = str(exc)


def _normalise_cost_key(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "model_id" in out.columns:
        out["model_id"] = out["model_id"].fillna("").astype(str).str.strip().str.upper()
    if "currency" in out.columns:
        out["currency"] = out["currency"].fillna("").astype(str).str.strip().str.upper()
    if "cost_month" in out.columns:
        out["cost_month"] = out["cost_month"].fillna("").astype(str).str[:10]
    return out


def _merge_cost_rows(current: pd.DataFrame, edited: pd.DataFrame, columns: list[str], cost_col: str) -> pd.DataFrame:
    base = ensure_download_df(current, columns)
    edited = ensure_download_df(edited, columns)
    base = _normalise_cost_key(base)
    edited = _normalise_cost_key(edited)

    edited[cost_col] = pd.to_numeric(edited[cost_col], errors="coerce")
    edited = edited[(edited["model_id"] != "") & (edited["cost_month"] != "")]
    edited = edited.drop_duplicates(subset=["model_id", "cost_month"], keep="last")

    if base.empty:
        return edited[columns].reset_index(drop=True)

    base = base[~base.set_index(["model_id", "cost_month"]).index.isin(
        edited.set_index(["model_id", "cost_month"]).index
    )]
    out = pd.concat([base, edited], ignore_index=True)
    out = out.drop_duplicates(subset=["model_id", "cost_month"], keep="last")
    out = out.sort_values(["cost_month", "model_id"], ascending=[False, True]).reset_index(drop=True)
    return out[columns]


def _filter_cost_df(df: pd.DataFrame, cost_col: str, key_prefix: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    filtered = df.copy()
    filtered = _normalise_cost_key(filtered)

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        model_filter = st.text_input("Filter by model", key=f"{key_prefix}_model_filter")
    with c2:
        month_options = sorted([x for x in filtered["cost_month"].dropna().astype(str).unique().tolist() if x], reverse=True)
        month_filter = st.selectbox("Cost Month", ["All"] + month_options, key=f"{key_prefix}_month_filter")
    with c3:
        currency_options = sorted([x for x in filtered["currency"].dropna().astype(str).unique().tolist() if x])
        currency_filter = st.selectbox("Currency", ["All"] + currency_options, key=f"{key_prefix}_currency_filter")

    if model_filter:
        filtered = filtered[filtered["model_id"].str.contains(model_filter.strip(), case=False, na=False)]
    if month_filter != "All":
        filtered = filtered[filtered["cost_month"] == month_filter]
    if currency_filter != "All":
        filtered = filtered[filtered["currency"] == currency_filter]
    return filtered


def _render_single_cost_form(cost_type: str, columns: list[str], cost_col: str, current: pd.DataFrame, save_func, source_path: Path, sheet_name: str, key_prefix: str):
    with st.expander(f"Add / Update One {cost_type} Cost", expanded=False):
        c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
        with c1:
            model = st.text_input("Model", key=f"{key_prefix}_single_model").strip().upper()
        with c2:
            cost = st.number_input("Cost", min_value=0.0, step=0.01, format="%.4f", key=f"{key_prefix}_single_cost")
        with c3:
            currency = st.text_input("Currency", value="USD" if cost_type == "EXW" else "AUD", key=f"{key_prefix}_single_currency").strip().upper()
        with c4:
            cost_month = st.text_input("Cost Month", value=datetime.now().strftime("%Y-%m"), key=f"{key_prefix}_single_month").strip()

        if st.button(f"Save One {cost_type} Cost", use_container_width=True, key=f"{key_prefix}_save_single"):
            row = pd.DataFrame([{ "model_id": model, cost_col: cost, "currency": currency, "cost_month": cost_month }])
            new_all = _merge_cost_rows(current, row, columns, cost_col)
            saved, ignored = save_func(new_all, replace_all=True)
            _write_cost_source_file(new_all, source_path, sheet_name)
            show_upload_result(saved, ignored, action="updated")
            st.rerun()


def _render_cost_tab(cost_type: str, current: pd.DataFrame, columns: list[str], cost_col: str, save_func, clear_func, source_path: Path, sheet_name: str, key_prefix: str):
    download_df = ensure_download_df(current, columns)

    src_msg = str(source_path.relative_to(PROJECT_ROOT)) if source_path.exists() else f"{source_path.relative_to(PROJECT_ROOT)} not found"
    st.caption(f"Source file: `{src_msg}`")
    st.download_button(
        f"Download Current {cost_type} Cost",
        data=to_excel_bytes(download_df, f"{cost_type} Cost"),
        file_name=f"{key_prefix}_cost_current.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        key=f"{key_prefix}_download",
    )

    _render_single_cost_form(cost_type, columns, cost_col, current, save_func, source_path, sheet_name, key_prefix)

    st.subheader(f"Edit Current {cost_type} Cost")
    if download_df.empty:
        editor_source = pd.DataFrame([{c: "" for c in columns}])
    else:
        editor_source = _filter_cost_df(download_df, cost_col, key_prefix)

    edited = st.data_editor(
        editor_source,
        use_container_width=True,
        num_rows="dynamic",
        height=500,
        hide_index=True,
        key=f"{key_prefix}_editor",
    )

    if st.button(f"Save Edited {cost_type} Cost", use_container_width=True, key=f"{key_prefix}_save_editor"):
        new_all = _merge_cost_rows(download_df, edited, columns, cost_col)
        saved, ignored = save_func(new_all, replace_all=True)
        _write_cost_source_file(new_all, source_path, sheet_name)
        show_upload_result(saved, ignored, action="updated")
        st.rerun()

    render_clear_button(f"Clear {cost_type} Cost", clear_func, f"clear_{key_prefix}")


if "cost_file_auto_synced" not in st.session_state:
    _auto_sync_cost_from_files()
    st.session_state["cost_file_auto_synced"] = True

# -----------------------------
# Fixed data file helpers for Store / Sales
# -----------------------------
def _read_source_file(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    return pd.read_excel(path)


def _write_source_file(df: pd.DataFrame, path: Path, sheet_name: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    out = df.copy()
    for c in ["sales_date", "cost_month"]:
        if c in out.columns:
            out[c] = out[c].astype(str).str[:10]
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        out.to_excel(writer, index=False, sheet_name=sheet_name[:31])


def _auto_sync_store_sales_from_files() -> None:
    """Load fixed source files under /data into SQLite once per session."""
    synced = []
    errors = []
    jobs = [
        (STORE_MASTER_FILE, _normalise_store_master_with_short, save_store_master_records_with_short, "Store Master"),
        (HIGHLIGHT_STORE_FILE, normalize_highlight_store_df, save_highlight_store_records, "Highlight Store"),
        (SALES_BY_STORES_FILE, normalize_sales_by_stores_df, save_sales_by_stores_records, "Sales by Stores"),
        (SALES_AGENT_FILE, normalize_sales_agent_df, save_sales_agent_records, "Sales Agent"),
    ]
    for path, normalizer, saver, label in jobs:
        try:
            if path.exists():
                df = normalizer(_read_source_file(path))
                result = saver(df, replace_all=True)
                if isinstance(result, tuple):
                    saved, ignored = result
                    synced.append(f"{label}: {saved:,} saved, {ignored:,} ignored")
                else:
                    synced.append(f"{label}: {len(df):,}")
        except Exception as exc:
            errors.append(f"{label}: {exc}")
    if synced:
        st.session_state["store_sales_file_sync_message"] = " | ".join(synced)
    if errors:
        st.session_state["store_sales_file_sync_error"] = " | ".join(errors)


def _norm_text_col(df: pd.DataFrame, col: str, upper: bool = False) -> pd.DataFrame:
    out = df.copy()
    if col in out.columns:
        out[col] = out[col].fillna("").astype(str).str.strip()
        if upper:
            out[col] = out[col].str.upper()
    return out


def _format_date_col(df: pd.DataFrame, col: str = "sales_date") -> pd.DataFrame:
    out = df.copy()
    if col in out.columns:
        out[col] = pd.to_datetime(out[col], errors="coerce").dt.strftime("%Y-%m-%d").fillna(out[col].astype(str).str[:10])
    return out


def _merge_edited_rows(base: pd.DataFrame, original_filtered: pd.DataFrame, edited: pd.DataFrame, columns: list[str], key_cols: list[str]) -> pd.DataFrame:
    base = ensure_download_df(base, columns)
    original_filtered = ensure_download_df(original_filtered, columns)
    edited = ensure_download_df(edited, columns)

    for df in (base, original_filtered, edited):
        for col in key_cols:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str).str.strip()
        if "sales_date" in df.columns:
            df["sales_date"] = pd.to_datetime(df["sales_date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna(df["sales_date"].astype(str).str[:10])

    edited = edited.copy()
    first_key = key_cols[0]
    edited = edited[edited[first_key].fillna("").astype(str).str.strip() != ""]

    if not original_filtered.empty:
        old_keys = set(map(tuple, original_filtered[key_cols].astype(str).values.tolist()))
        keep_mask = ~base[key_cols].astype(str).apply(tuple, axis=1).isin(old_keys)
        base = base[keep_mask]

    out = pd.concat([base, edited], ignore_index=True)
    out = out.drop_duplicates(subset=key_cols, keep="last")
    return out[columns].reset_index(drop=True)


def _filter_store_master_df(df: pd.DataFrame, key_prefix: str) -> pd.DataFrame:
    filtered = ensure_download_df(df, STORE_MASTER_COLUMNS)
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        store_filter = st.text_input("Filter by store", key=f"{key_prefix}_store_filter")
    with c2:
        regions = sorted([x for x in filtered["region"].dropna().astype(str).unique().tolist() if x])
        region_filter = st.selectbox("Region", ["All"] + regions, key=f"{key_prefix}_region_filter")
    with c3:
        channels = sorted([x for x in filtered["retailer"].dropna().astype(str).unique().tolist() if x])
        channel_filter = st.selectbox("Channel", ["All"] + channels, key=f"{key_prefix}_channel_filter")
    if store_filter:
        filtered = filtered[filtered["business_name"].astype(str).str.contains(store_filter.strip(), case=False, na=False)]
    if region_filter != "All":
        filtered = filtered[filtered["region"].astype(str) == region_filter]
    if channel_filter != "All":
        filtered = filtered[filtered["retailer"].astype(str) == channel_filter]
    return filtered


def _filter_highlight_store_df(df: pd.DataFrame, key_prefix: str) -> pd.DataFrame:
    filtered = ensure_download_df(df, ["business_name", "highlight_color"])
    c1, c2 = st.columns([2, 1])
    with c1:
        store_filter = st.text_input("Filter by store", key=f"{key_prefix}_store_filter")
    with c2:
        colors = sorted([x for x in filtered["highlight_color"].dropna().astype(str).unique().tolist() if x])
        color_filter = st.selectbox("Highlight Color", ["All"] + colors, key=f"{key_prefix}_color_filter")
    if store_filter:
        filtered = filtered[filtered["business_name"].astype(str).str.contains(store_filter.strip(), case=False, na=False)]
    if color_filter != "All":
        filtered = filtered[filtered["highlight_color"].astype(str) == color_filter]
    return filtered


def _filter_sales_df(df: pd.DataFrame, key_prefix: str, store_master: pd.DataFrame | None = None) -> pd.DataFrame:
    filtered = df.copy()
    if "sales_date" in filtered.columns:
        filtered = _format_date_col(filtered, "sales_date")
        dt = pd.to_datetime(filtered["sales_date"], errors="coerce")
        filtered["_week"] = dt.dt.strftime("%G-W%V")

    # Add channel / region helper columns for Sales by Stores from Store Master.
    helper_cols = []
    if "channel" not in filtered.columns and "business_name" in filtered.columns and store_master is not None and not store_master.empty:
        sm = ensure_download_df(store_master, STORE_MASTER_COLUMNS)[["business_name", "region", "retailer"]].copy()
        sm["_store_key"] = sm["business_name"].fillna("").astype(str).str.strip().str.upper()
        filtered["_store_key"] = filtered["business_name"].fillna("").astype(str).str.strip().str.upper()
        filtered = filtered.merge(sm[["_store_key", "region", "retailer"]], on="_store_key", how="left")
        filtered = filtered.rename(columns={"retailer": "channel"})
        helper_cols.extend(["_store_key", "region", "channel"])

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        model_filter = st.text_input("Filter by model", key=f"{key_prefix}_model_filter")
    with c2:
        weeks = sorted([x for x in filtered.get("_week", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if x], reverse=True)
        week_filter = st.selectbox("Week", ["All"] + weeks, key=f"{key_prefix}_week_filter")
    with c3:
        channels = sorted([x for x in filtered.get("channel", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if x])
        channel_filter = st.selectbox("Channel", ["All"] + channels, key=f"{key_prefix}_channel_filter")

    if "business_name" in filtered.columns:
        c4, c5 = st.columns([2, 1])
        with c4:
            store_filter = st.text_input("Filter by store", key=f"{key_prefix}_store_filter")
        with c5:
            regions = sorted([x for x in filtered.get("region", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if x])
            region_filter = st.selectbox("Region", ["All"] + regions, key=f"{key_prefix}_region_filter")
    else:
        store_filter = ""
        region_filter = "All"

    if model_filter and "model" in filtered.columns:
        filtered = filtered[filtered["model"].astype(str).str.contains(model_filter.strip(), case=False, na=False)]
    if week_filter != "All" and "_week" in filtered.columns:
        filtered = filtered[filtered["_week"] == week_filter]
    if channel_filter != "All" and "channel" in filtered.columns:
        filtered = filtered[filtered["channel"].astype(str) == channel_filter]
    if store_filter and "business_name" in filtered.columns:
        filtered = filtered[filtered["business_name"].astype(str).str.contains(store_filter.strip(), case=False, na=False)]
    if region_filter != "All" and "region" in filtered.columns:
        filtered = filtered[filtered["region"].astype(str) == region_filter]

    drop_cols = [c for c in ["_week", "_store_key", "region", "channel"] if c in filtered.columns and c not in df.columns]
    return filtered.drop(columns=drop_cols, errors="ignore")


def _cap_editor_rows(df: pd.DataFrame, max_rows: int = 5000) -> pd.DataFrame:
    if len(df) > max_rows:
        st.warning(f"Filtered result has {len(df):,} rows. Showing first {max_rows:,} rows only. Please narrow the filters before saving large changes.")
        return df.head(max_rows).copy()
    return df.copy()


def _render_store_master_tab():
    columns = STORE_MASTER_COLUMNS
    current = read_store_master_records_with_short()
    download_df = ensure_download_df(current, columns)
    st.caption(f"Source file: `{STORE_MASTER_FILE.relative_to(PROJECT_ROOT)}`" if STORE_MASTER_FILE.exists() else f"Source file: `{STORE_MASTER_FILE.relative_to(PROJECT_ROOT)}` not found")
    st.download_button("Download Current Store Master", data=to_excel_bytes(download_df, "Store Master"), file_name="store_master_current.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, key="store_master_download")
    st.subheader("Edit Store Master")
    filtered = _filter_store_master_df(download_df, "store_master")
    if filtered.empty:
        filtered = pd.DataFrame([{c: "" for c in columns}])
    edited = st.data_editor(filtered, use_container_width=True, num_rows="dynamic", height=520, hide_index=True, key="store_master_editor")
    if st.button("Save Edited Store Master", use_container_width=True, key="save_store_master_editor"):
        new_all = _merge_edited_rows(download_df, filtered, edited, columns, ["business_name"])
        save_store_master_records_with_short(new_all, replace_all=True)
        _write_source_file(new_all, STORE_MASTER_FILE, "Store Master")
        st.success("Store Master updated to database and source file.")
        st.rerun()
    render_clear_button("Clear Store Master", clear_store_master_records, "clear_store_master")


def _render_highlight_store_tab():
    columns = ["business_name", "highlight_color"]
    current = read_highlight_store_records()
    download_df = ensure_download_df(current, columns)
    st.caption(f"Source file: `{HIGHLIGHT_STORE_FILE.relative_to(PROJECT_ROOT)}`" if HIGHLIGHT_STORE_FILE.exists() else f"Source file: `{HIGHLIGHT_STORE_FILE.relative_to(PROJECT_ROOT)}` not found")
    st.download_button("Download Current Highlight Store", data=to_excel_bytes(download_df, "Highlight Store"), file_name="highlight_store_current.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, key="highlight_store_download")
    st.subheader("Edit Highlight Store")
    filtered = _filter_highlight_store_df(download_df, "highlight_store")
    if filtered.empty:
        filtered = pd.DataFrame([{c: "" for c in columns}])
    edited = st.data_editor(filtered, use_container_width=True, num_rows="dynamic", height=520, hide_index=True, key="highlight_store_editor")
    if st.button("Save Edited Highlight Store", use_container_width=True, key="save_highlight_store_editor"):
        new_all = _merge_edited_rows(download_df, filtered, edited, columns, ["business_name"])
        save_highlight_store_records(new_all, replace_all=True)
        _write_source_file(new_all, HIGHLIGHT_STORE_FILE, "Highlight Store")
        st.success("Highlight Store updated to database and source file.")
        st.rerun()
    render_clear_button("Clear Highlight Store", clear_highlight_store_records, "clear_highlight_store")


def _render_sales_by_stores_tab():
    columns = ["sales_date", "business_name", "model", "sales"]
    current = read_sales_by_stores_records()
    download_df = ensure_download_df(current, columns)
    download_df = _format_date_col(download_df, "sales_date")
    st.caption(f"Source file: `{SALES_BY_STORES_FILE.relative_to(PROJECT_ROOT)}`" if SALES_BY_STORES_FILE.exists() else f"Source file: `{SALES_BY_STORES_FILE.relative_to(PROJECT_ROOT)}` not found")
    st.download_button("Download Current Sales by Stores", data=to_excel_bytes(download_df, "Sales by Stores"), file_name="sales_by_stores_current.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, key="sales_by_stores_download")
    st.subheader("Edit Sales by Stores")
    filtered = _filter_sales_df(download_df, "sales_summary_store", read_store_master_records_with_short())
    filtered = _cap_editor_rows(filtered)
    if filtered.empty:
        filtered = pd.DataFrame([{c: "" for c in columns}])
    edited = st.data_editor(filtered, use_container_width=True, num_rows="dynamic", height=560, hide_index=True, key="sales_by_stores_editor")
    if st.button("Save Edited Sales by Stores", use_container_width=True, key="save_sales_by_stores_editor"):
        new_all = _merge_edited_rows(download_df, filtered, edited, columns, ["sales_date", "business_name", "model"])
        saved, ignored = save_sales_by_stores_records(new_all, replace_all=True)
        _write_source_file(new_all, SALES_BY_STORES_FILE, "Sales by Stores")
        show_upload_result(saved, ignored, action="updated")
        st.rerun()
    render_clear_button("Clear Sales by Stores", clear_sales_by_stores_records, "clear_sales_by_stores")


def _render_sales_agent_tab():
    columns = ["sales_date", "channel", "model", "avl_soh_amt", "soo_amt", "daily_sales_amt", "price", "sum_avl_soh", "sum_soo", "sales_qty"]
    current = read_sales_agent_records()
    download_df = ensure_download_df(current, columns)
    download_df = _format_date_col(download_df, "sales_date")
    st.caption(f"Source file: `{SALES_AGENT_FILE.relative_to(PROJECT_ROOT)}`" if SALES_AGENT_FILE.exists() else f"Source file: `{SALES_AGENT_FILE.relative_to(PROJECT_ROOT)}` not found")
    st.download_button("Download Current Sales Agent Data", data=to_excel_bytes(download_df, "Sales Agent"), file_name="sales_agent_current.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, key="sales_agent_download")
    st.subheader("Edit Sales Agent Data")
    st.caption(f"Rows: {sales_meta['rows']:,} | Date range: {sales_meta['min_date']} → {sales_meta['max_date']}")
    filtered = _filter_sales_df(download_df, "sales_agent")
    filtered = _cap_editor_rows(filtered)
    if filtered.empty:
        filtered = pd.DataFrame([{c: "" for c in columns}])
    edited = st.data_editor(filtered, use_container_width=True, num_rows="dynamic", height=560, hide_index=True, key="sales_agent_editor")
    if st.button("Save Edited Sales Agent Data", use_container_width=True, key="save_sales_agent_editor"):
        new_all = _merge_edited_rows(download_df, filtered, edited, columns, ["sales_date", "channel", "model"])
        saved, ignored = save_sales_agent_records(new_all, replace_all=True)
        _write_source_file(new_all, SALES_AGENT_FILE, "Sales Agent")
        show_upload_result(saved, ignored, action="updated")
        st.rerun()
    render_clear_button("Clear Sales Agent Sales", clear_sales_agent_records, "clear_sales_agent")


if "store_sales_file_auto_synced" not in st.session_state:
    _auto_sync_store_sales_from_files()
    st.session_state["store_sales_file_auto_synced"] = True


# -----------------------------
# Product Master fixed-file helpers
# -----------------------------
def _auto_sync_product_from_file() -> None:
    """Load Product Master from /data/product_model_master.xlsx into SQLite once per session."""
    try:
        if PRODUCT_MASTER_FILE.exists():
            df = normalize_product_master_df(_read_source_file(PRODUCT_MASTER_FILE))
            save_product_master_records(df, replace_all=True)
            st.session_state["product_file_sync_message"] = f"Product Master: {len(df):,}"
    except Exception as exc:
        st.session_state["product_file_sync_error"] = str(exc)


def _normalise_product_df(df: pd.DataFrame) -> pd.DataFrame:
    out = ensure_download_df(df, ["product_line", "category", "hau_model", "hq_model", "series"]).copy()
    for col in ["product_line", "category", "hau_model", "hq_model", "series"]:
        out[col] = out[col].fillna("").astype(str).str.strip()
    out["hau_model"] = out["hau_model"].str.upper()
    return out


def _filter_product_df(df: pd.DataFrame, key_prefix: str = "product") -> pd.DataFrame:
    filtered = _normalise_product_df(df)
    c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
    with c1:
        model_filter = st.text_input("Filter by HAU Model", key=f"{key_prefix}_model_filter")
    with c2:
        product_lines = sorted([x for x in filtered["product_line"].dropna().astype(str).unique().tolist() if x])
        product_line_filter = st.selectbox("Product Line", ["All"] + product_lines, key=f"{key_prefix}_line_filter")
    with c3:
        categories = sorted([x for x in filtered["category"].dropna().astype(str).unique().tolist() if x])
        category_filter = st.selectbox("Category", ["All"] + categories, key=f"{key_prefix}_category_filter")
    with c4:
        series_options = sorted([x for x in filtered["series"].dropna().astype(str).unique().tolist() if x])
        series_filter = st.selectbox("Series", ["All"] + series_options, key=f"{key_prefix}_series_filter")

    if model_filter:
        filtered = filtered[filtered["hau_model"].astype(str).str.contains(model_filter.strip(), case=False, na=False)]
    if product_line_filter != "All":
        filtered = filtered[filtered["product_line"].astype(str) == product_line_filter]
    if category_filter != "All":
        filtered = filtered[filtered["category"].astype(str) == category_filter]
    if series_filter != "All":
        filtered = filtered[filtered["series"].astype(str) == series_filter]
    return filtered


def _merge_product_rows(base: pd.DataFrame, original_filtered: pd.DataFrame, edited: pd.DataFrame) -> pd.DataFrame:
    columns = ["product_line", "category", "hau_model", "hq_model", "series"]
    base = _normalise_product_df(base)
    original_filtered = _normalise_product_df(original_filtered)
    edited = _normalise_product_df(edited)

    edited = edited[edited["hau_model"].fillna("").astype(str).str.strip() != ""]

    if not original_filtered.empty:
        old_keys = set(original_filtered["hau_model"].astype(str).str.upper().tolist())
        base = base[~base["hau_model"].astype(str).str.upper().isin(old_keys)]

    out = pd.concat([base, edited], ignore_index=True)
    out = out.drop_duplicates(subset=["hau_model"], keep="last")
    out = out.sort_values(["product_line", "category", "series", "hau_model"]).reset_index(drop=True)
    return out[columns]


def _render_single_product_form(current: pd.DataFrame):
    columns = ["product_line", "category", "hau_model", "hq_model", "series"]
    with st.expander("Add / Update One Product", expanded=False):
        c1, c2, c3, c4, c5 = st.columns([1, 1, 1.4, 1.4, 1])
        with c1:
            product_line = st.text_input("Product Line", key="product_single_line").strip()
        with c2:
            category = st.text_input("Category", key="product_single_category").strip()
        with c3:
            hau_model = st.text_input("HAU Model", key="product_single_hau").strip().upper()
        with c4:
            hq_model = st.text_input("HQ Model", key="product_single_hq").strip()
        with c5:
            series = st.text_input("Series", key="product_single_series").strip()

        if st.button("Save One Product", use_container_width=True, key="product_save_single"):
            if not hau_model:
                st.warning("HAU Model is required.")
                return
            row = pd.DataFrame([{
                "product_line": product_line,
                "category": category,
                "hau_model": hau_model,
                "hq_model": hq_model,
                "series": series,
            }])
            new_all = _merge_product_rows(current, pd.DataFrame(columns=columns), row)
            save_product_master_records(new_all, replace_all=True)
            _write_source_file(new_all, PRODUCT_MASTER_FILE, "Product Master")
            st.success("Product Master updated to database and source file.")
            st.rerun()


def _render_product_master_page():
    columns = ["product_line", "category", "hau_model", "hq_model", "series"]
    st.header("Product Model Master")
    st.caption(
        "Product Master is automatically loaded from fixed file under the project data folder. "
        "Upload is removed. Add/update one product, edit the filtered table, then save to sync both Excel and database."
    )

    if "product_file_sync_message" in st.session_state:
        st.info(f"Auto synced from product file: {st.session_state['product_file_sync_message']}")
    if "product_file_sync_error" in st.session_state:
        st.warning(f"Product file auto sync failed: {st.session_state['product_file_sync_error']}")

    current = read_product_master_records()
    download_df = ensure_download_df(current, columns)
    st.caption(
        f"Source file: `{PRODUCT_MASTER_FILE.relative_to(PROJECT_ROOT)}`"
        if PRODUCT_MASTER_FILE.exists()
        else f"Source file: `{PRODUCT_MASTER_FILE.relative_to(PROJECT_ROOT)}` not found"
    )
    st.download_button(
        "Download Current Product Master",
        data=to_excel_bytes(download_df, "Product Master"),
        file_name="product_model_master_current.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        key="product_download",
    )

    _render_single_product_form(download_df)

    st.subheader("Edit Product Master")
    filtered = _filter_product_df(download_df, "product")
    if filtered.empty:
        filtered = pd.DataFrame([{c: "" for c in columns}])

    edited = st.data_editor(
        filtered,
        use_container_width=True,
        num_rows="dynamic",
        height=520,
        hide_index=True,
        key="product_editor",
    )

    if st.button("Save Edited Product Master", use_container_width=True, key="product_save_editor"):
        new_all = _merge_product_rows(download_df, filtered, edited)
        save_product_master_records(new_all, replace_all=True)
        _write_source_file(new_all, PRODUCT_MASTER_FILE, "Product Master")
        st.success("Product Master updated to database and source file.")
        st.rerun()

    render_clear_button("Clear Product Master", clear_product_master_records, "clear_product")


if "product_file_auto_synced" not in st.session_state:
    _auto_sync_product_from_file()
    st.session_state["product_file_auto_synced"] = True


# -----------------------------
# Metrics
# -----------------------------
sales_meta = get_sales_agent_summary()
product_count = table_count("model_master")
exw_count = table_count("exw_cost")
landed_count = table_count("landed_cost")
store_count = table_count("store_locations")
store_sales_count = table_count("sales_records")
highlight_count = table_count("highlight_stores")

m1, m2, m3, m4, m5, m6, m7 = st.columns(7)
m1.metric("Product Master", f"{product_count:,}")
m2.metric("EXW Cost", f"{exw_count:,}")
m3.metric("Landed Cost", f"{landed_count:,}")
m4.metric("Store Master", f"{store_count:,}")
m5.metric("Sales by Stores", f"{store_sales_count:,}")
m6.metric("Highlight Stores", f"{highlight_count:,}")
m7.metric("Sales Agent", f"{sales_meta['rows']:,}")

st.markdown("---")

with st.sidebar:
    st.markdown("### Database Menu")
    menu_options = [
        "0. Summary",
        "1. Model DB",
        "2. Cost DB",
        "3. Store DB",
        "4. Sales DB",
    ]
    selected_menu = st.radio("", menu_options, label_visibility="collapsed", key="database_menu")

# -----------------------------
# 1. Product Master
# -----------------------------
if selected_menu == "1. Model DB":
    _render_product_master_page()


# -----------------------------
# 2. Cost
# -----------------------------
elif selected_menu == "2. Cost DB":
    st.header("Cost Maintenance")
    st.caption(
        "Cost records are automatically loaded from fixed files under the project data folder. "
        "You can maintain one cost row, edit the filtered table, download current cost, and save changes back to both Excel and database."
    )

    if "cost_file_sync_message" in st.session_state:
        st.info(f"Auto synced from cost files: {st.session_state['cost_file_sync_message']}")
    if "cost_file_sync_error" in st.session_state:
        st.warning(f"Cost file auto sync failed: {st.session_state['cost_file_sync_error']}")

    exw_tab, landed_tab = st.tabs(["EXW Cost", "Landed Cost"])

    with exw_tab:
        current = read_exw_cost_records()
        _render_cost_tab(
            cost_type="EXW",
            current=current,
            columns=["model_id", "exw_cost", "currency", "cost_month"],
            cost_col="exw_cost",
            save_func=save_exw_cost_records,
            clear_func=clear_exw_cost_records,
            source_path=EXW_COST_FILE,
            sheet_name="EXW Cost",
            key_prefix="exw",
        )

    with landed_tab:
        current = read_landed_cost_records()
        _render_cost_tab(
            cost_type="Landed",
            current=current,
            columns=["model_id", "landed_cost", "currency", "cost_month"],
            cost_col="landed_cost",
            save_func=save_landed_cost_records,
            clear_func=clear_landed_cost_records,
            source_path=LANDED_COST_FILE,
            sheet_name="Landed Cost",
            key_prefix="landed",
        )

# -----------------------------
# 3. Store Data
# -----------------------------
elif selected_menu == "3. Store DB":
    st.header("Store Data Maintenance")
    st.caption(
        "Store Master and Highlight Store are maintained from fixed files under the project data folder. "
        "Upload is removed. Edit the filtered table and save to sync both source file and database."
    )
    if "store_sales_file_sync_message" in st.session_state:
        st.info(f"Auto synced from source files: {st.session_state['store_sales_file_sync_message']}")
    if "store_sales_file_sync_error" in st.session_state:
        st.warning(f"Source file auto sync failed: {st.session_state['store_sales_file_sync_error']}")

    store_tab, highlight_tab = st.tabs(["Store Master", "Highlight Store"])
    with store_tab:
        _render_store_master_tab()
    with highlight_tab:
        _render_highlight_store_tab()

# -----------------------------
# 4. Sales Data
# -----------------------------
elif selected_menu == "4. Sales DB":
    st.header("Sales Data Maintenance")
    st.caption(
        "Sales by Stores and Sales Agent data are maintained from fixed files under the project data folder. "
        "Upload is removed. Use filters by model, week, channel and store where available, then edit and save."
    )
    if "store_sales_file_sync_message" in st.session_state:
        st.info(f"Auto synced from source files: {st.session_state['store_sales_file_sync_message']}")
    if "store_sales_file_sync_error" in st.session_state:
        st.warning(f"Source file auto sync failed: {st.session_state['store_sales_file_sync_error']}")

    sbs_tab, sa_tab = st.tabs(["Sales by Stores", "Sales Agent"])
    with sbs_tab:
        _render_sales_by_stores_tab()
    with sa_tab:
        _render_sales_agent_tab()

# -----------------------------
# 6. Status
# -----------------------------
elif selected_menu == "0. Summary":
    st.header("Shared Database Status")
    st.caption("Inspect and clear the shared SQLite database.")
    rows = []
    try:
        with sqlite3.connect(DB_PATH) as conn:
            tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name", conn)["name"].tolist()
            for table in tables:
                try:
                    count = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
                    rows.append({"table": table, "rows": count, "columns": ", ".join(cols)})
                except Exception:
                    rows.append({"table": table, "rows": "-", "columns": "-"})
    except Exception as exc:
        st.error(f"Cannot inspect database: {exc}")
    st.dataframe(pd.DataFrame(rows), use_container_width=True, height=520, hide_index=True)

    with st.expander("Clear all shared data", expanded=False):
        st.error("This clears Product Master, Cost, Store Master, Highlight Store, Sales by Stores and Sales Agent data.")
        confirm = st.text_input("Type CLEAR to confirm", key="clear_all_confirm")
        if st.button("Clear Entire Database", use_container_width=True, key="clear_all"):
            if confirm == "CLEAR":
                clear_all_database_records()
                clear_highlight_store_records()
                st.success("Entire database cleared.")
                st.rerun()
            else:
                st.warning("Please type CLEAR first.")
