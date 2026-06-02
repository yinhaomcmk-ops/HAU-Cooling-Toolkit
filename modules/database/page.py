from __future__ import annotations

import io
import sqlite3
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
    get_sales_agent_summary,
    init_all_shared_db,
    normalize_exw_cost_df,
    normalize_landed_cost_df,
    normalize_product_master_df,
    normalize_sales_agent_df,
    normalize_sales_by_stores_df,
    normalize_store_master_df,
    read_exw_cost_records,
    read_landed_cost_records,
    read_product_master_records,
    read_sales_agent_records,
    read_sales_by_stores_records,
    read_store_master_records,
    save_exw_cost_records,
    save_landed_cost_records,
    save_product_master_records,
    save_sales_agent_records,
    save_sales_by_stores_records,
    save_store_master_records,
    table_count,
)

st.title("Database")
st.caption(
    "Central shared database. HAU Model is the key across Product Master, Cost, Sales by Stores and Sales Agent. "
    "Rows whose HAU Model is not in Product Master are ignored when saving shared analysis data."
)

init_all_shared_db()

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
# Highlight Store helpers
# -----------------------------
def _norm_store_name(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip().upper()


def init_highlight_store_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS highlight_stores (
                business_name TEXT PRIMARY KEY,
                highlight_color TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_highlight_stores_business_name ON highlight_stores(business_name)")
        conn.commit()


def normalize_highlight_store_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    aliases = {
        "business_name": ["business name", "business_name", "store name", "store", "customer", "customer name", "门店名"],
        "highlight_color": ["highlight color", "highlight_color", "highlight colour", "highlight_colour", "color", "colour", "颜色"],
    }
    rename_map = {}
    current = set(df.columns)
    for target, names in aliases.items():
        for name in names:
            if name.lower() in current:
                rename_map[name.lower()] = target
                break
    df = df.rename(columns=rename_map)
    missing = [c for c in ["business_name", "highlight_color"] if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required Highlight Store columns: {missing}")
    out = df[["business_name", "highlight_color"]].copy()
    out["business_name"] = out["business_name"].apply(_norm_store_name)
    out["highlight_color"] = out["highlight_color"].fillna("").astype(str).str.strip()
    out = out[(out["business_name"] != "") & (out["highlight_color"] != "")]
    return out.drop_duplicates(subset=["business_name"], keep="last").reset_index(drop=True)


def save_highlight_store_records(df: pd.DataFrame, replace_all: bool = True) -> None:
    init_highlight_store_db()
    out = normalize_highlight_store_df(df)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with sqlite3.connect(DB_PATH) as conn:
        if replace_all:
            conn.execute("DELETE FROM highlight_stores")
        for _, r in out.iterrows():
            conn.execute(
                """
                INSERT INTO highlight_stores (business_name, highlight_color, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(business_name) DO UPDATE SET
                    highlight_color=excluded.highlight_color,
                    updated_at=excluded.updated_at
                """,
                (r["business_name"], r["highlight_color"], now),
            )
        conn.commit()


def read_highlight_store_records() -> pd.DataFrame:
    init_highlight_store_db()
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(
            "SELECT business_name, highlight_color, updated_at FROM highlight_stores ORDER BY business_name",
            conn,
        )


def clear_highlight_store_records() -> None:
    init_highlight_store_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM highlight_stores")
        conn.commit()


init_highlight_store_db()


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
        "Product Model Master",
        "Cost Maintenance",
        "Store Master Maintenance",
        "Sales by Stores Maintenance",
        "Highlight Store Maintenance",
        "Sales Agent Sales Maintenance",
        "Shared Database Status",
    ]
    selected_menu = st.radio("", menu_options, label_visibility="collapsed", key="database_menu")

# -----------------------------
# 1. Product Master
# -----------------------------
if selected_menu == "Product Model Master":
    st.header("Product Model Master")
    st.caption("Universal product table for all modules. Fields: Product Line, Category, HAU Model, HQ Model, Series.")

    current = read_product_master_records()
    download_df = ensure_download_df(current, ["product_line", "category", "hau_model", "hq_model", "series"])
    st.download_button(
        "Download Current Product Master",
        data=to_excel_bytes(download_df, "Product Master"),
        file_name="product_model_master_current.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    mode = st.radio("Save mode", ["Replace product master", "Update / append by HAU Model"], horizontal=True, key="product_mode")
    uploaded = st.file_uploader("Upload Product Model Master", type=["xlsx", "csv"], key="product_upload")
    if uploaded is not None:
        try:
            preview = normalize_product_master_df(_load_raw_file(uploaded))
            st.success(f"File valid. Rows ready: {len(preview):,}")
            st.dataframe(preview.head(80), use_container_width=True, height=300)
            if st.button("Save Product Master", use_container_width=True, key="save_product"):
                save_product_master_records(preview, replace_all=(mode == "Replace product master"))
                st.success("Product Master saved.")
                st.rerun()
        except Exception as exc:
            st.error(f"Upload failed: {exc}")

    st.subheader("Manual maintenance")
    editor_df = download_df.copy()
    if editor_df.empty:
        editor_df = pd.DataFrame([{c: "" for c in ["product_line", "category", "hau_model", "hq_model", "series"]}])
    edited = st.data_editor(editor_df, use_container_width=True, num_rows="dynamic", height=430, hide_index=True)
    if st.button("Save edited Product Master", use_container_width=True, key="save_product_editor"):
        save_product_master_records(edited, replace_all=True)
        st.success("Product Master updated.")
        st.rerun()
    render_clear_button("Clear Product Master", clear_product_master_records, "clear_product")

# -----------------------------
# 2. Cost
# -----------------------------
elif selected_menu == "Cost Maintenance":
    st.header("Cost Maintenance")
    st.caption("Cost records are linked by HAU Model. Invalid models are ignored on save.")
    exw_tab, landed_tab = st.tabs(["EXW Cost", "Landed Cost"])

    with exw_tab:
        current = read_exw_cost_records()
        download_df = ensure_download_df(current, ["model_id", "exw_cost", "currency", "cost_month"])
        st.download_button(
            "Download Current EXW Cost",
            data=to_excel_bytes(download_df, "EXW Cost"),
            file_name="exw_cost_current.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
        mode = st.radio("EXW save mode", ["Append uploaded rows", "Replace all EXW Cost"], horizontal=True, key="exw_mode")
        uploaded = st.file_uploader("Upload EXW Cost", type=["xlsx", "csv"], key="exw_upload")
        if uploaded is not None:
            try:
                preview = normalize_exw_cost_df(_load_raw_file(uploaded))
                st.success(f"File valid. Rows ready before model check: {len(preview):,}")
                st.dataframe(preview.head(80), use_container_width=True, height=300)
                if st.button("Save EXW Cost", use_container_width=True, key="save_exw"):
                    saved, ignored = save_exw_cost_records(preview, replace_all=(mode == "Replace all EXW Cost"))
                    show_upload_result(saved, ignored)
                    st.rerun()
            except Exception as exc:
                st.error(f"Upload failed: {exc}")
        st.subheader("Current EXW Cost")
        st.dataframe(current, use_container_width=True, height=360, hide_index=True)
        render_clear_button("Clear EXW Cost", clear_exw_cost_records, "clear_exw")

    with landed_tab:
        current = read_landed_cost_records()
        download_df = ensure_download_df(current, ["model_id", "landed_cost", "currency", "cost_month"])
        st.download_button(
            "Download Current Landed Cost",
            data=to_excel_bytes(download_df, "Landed Cost"),
            file_name="landed_cost_current.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
        mode = st.radio("Landed save mode", ["Append uploaded rows", "Replace all Landed Cost"], horizontal=True, key="landed_mode")
        uploaded = st.file_uploader("Upload Landed Cost", type=["xlsx", "csv"], key="landed_upload")
        if uploaded is not None:
            try:
                preview = normalize_landed_cost_df(_load_raw_file(uploaded))
                st.success(f"File valid. Rows ready before model check: {len(preview):,}")
                st.dataframe(preview.head(80), use_container_width=True, height=300)
                if st.button("Save Landed Cost", use_container_width=True, key="save_landed"):
                    saved, ignored = save_landed_cost_records(preview, replace_all=(mode == "Replace all Landed Cost"))
                    show_upload_result(saved, ignored)
                    st.rerun()
            except Exception as exc:
                st.error(f"Upload failed: {exc}")
        st.subheader("Current Landed Cost")
        st.dataframe(current, use_container_width=True, height=360, hide_index=True)
        render_clear_button("Clear Landed Cost", clear_landed_cost_records, "clear_landed")

# -----------------------------
# 3. Store Master
# -----------------------------
elif selected_menu == "Store Master Maintenance":
    st.header("Store Master Maintenance")
    st.caption("Fields: Store Name, Store Region, Store Channel, Latitude, Longitude.")
    current = read_store_master_records()
    download_df = ensure_download_df(current, ["business_name", "region", "retailer", "latitude", "longitude"])
    st.download_button(
        "Download Current Store Master",
        data=to_excel_bytes(download_df, "Store Master"),
        file_name="store_master_current.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
    mode = st.radio("Save mode", ["Replace Store Master", "Update / append by Store Name"], horizontal=True, key="store_mode")
    uploaded = st.file_uploader("Upload Store Master", type=["xlsx", "csv"], key="store_upload")
    if uploaded is not None:
        try:
            preview = normalize_store_master_df(_load_raw_file(uploaded))
            st.success(f"File valid. Rows ready: {len(preview):,}")
            st.dataframe(preview.head(80), use_container_width=True, height=300)
            if st.button("Save Store Master", use_container_width=True, key="save_store"):
                save_store_master_records(preview, replace_all=(mode == "Replace Store Master"))
                st.success("Store Master saved.")
                st.rerun()
        except Exception as exc:
            st.error(f"Upload failed: {exc}")
    st.subheader("Manual maintenance")
    editor_df = download_df.copy()
    if editor_df.empty:
        editor_df = pd.DataFrame([{c: "" for c in ["business_name", "region", "retailer", "latitude", "longitude"]}])
    edited = st.data_editor(editor_df, use_container_width=True, num_rows="dynamic", height=430, hide_index=True)
    if st.button("Save edited Store Master", use_container_width=True, key="save_store_editor"):
        save_store_master_records(edited, replace_all=True)
        st.success("Store Master updated.")
        st.rerun()
    render_clear_button("Clear Store Master", clear_store_master_records, "clear_store")

# -----------------------------
# 4. Sales by Stores
# -----------------------------
elif selected_menu == "Sales by Stores Maintenance":
    st.header("Sales by Stores Maintenance")
    st.caption("Current format unchanged. Required: sales_date, business_name, model, sales. Invalid HAU Models are ignored on save.")
    current = read_sales_by_stores_records()
    download_df = ensure_download_df(current, ["sales_date", "business_name", "model", "sales"])
    st.download_button(
        "Download Current Sales by Stores",
        data=to_excel_bytes(download_df, "Sales by Stores"),
        file_name="sales_by_stores_current.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
    mode = st.radio("Save mode", ["Append uploaded rows", "Replace all Sales by Stores data"], horizontal=True, key="sbs_mode")
    uploaded = st.file_uploader("Upload Sales by Stores file", type=["xlsx", "csv"], key="sbs_upload")
    if uploaded is not None:
        try:
            preview = normalize_sales_by_stores_df(_load_raw_file(uploaded))
            st.success(f"File valid. Rows ready before model check: {len(preview):,}")
            st.dataframe(preview.head(80), use_container_width=True, height=300)
            if st.button("Save Sales by Stores", use_container_width=True, key="save_sbs"):
                saved, ignored = save_sales_by_stores_records(preview, replace_all=(mode == "Replace all Sales by Stores data"))
                show_upload_result(saved, ignored)
                st.rerun()
        except Exception as exc:
            st.error(f"Upload failed: {exc}")
    st.subheader("Current saved data")
    st.dataframe(current.head(1000), use_container_width=True, height=430, hide_index=True)
    render_clear_button("Clear Sales by Stores", clear_sales_by_stores_records, "clear_sbs")

# -----------------------------
# 5. Highlight Store
# -----------------------------
elif selected_menu == "Highlight Store Maintenance":
    st.header("Highlight Store Maintenance")
    st.caption("Upload stores that need an additional coloured border on Sales Heatmap. This does not change the red/yellow/green sales colour.")
    st.markdown("Required columns: **Business Name**, **Highlight Color**")

    current = read_highlight_store_records()
    download_df = ensure_download_df(current, ["business_name", "highlight_color"])
    st.download_button(
        "Download Current Highlight Store",
        data=to_excel_bytes(download_df, "Highlight Store"),
        file_name="highlight_store_current.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    mode = st.radio("Save mode", ["Replace Highlight Store", "Update / append by Store Name"], horizontal=True, key="highlight_mode")
    uploaded = st.file_uploader("Upload Highlight Store", type=["xlsx", "csv"], key="highlight_upload")
    if uploaded is not None:
        try:
            preview = normalize_highlight_store_df(_load_raw_file(uploaded))
            st.success(f"File valid. Rows ready: {len(preview):,}")
            st.dataframe(preview.head(80), use_container_width=True, height=300)
            if st.button("Save Highlight Store", use_container_width=True, key="save_highlight"):
                save_highlight_store_records(preview, replace_all=(mode == "Replace Highlight Store"))
                st.success("Highlight Store saved.")
                st.rerun()
        except Exception as exc:
            st.error(f"Upload failed: {exc}")

    st.subheader("Manual maintenance")
    editor_df = download_df.copy()
    if editor_df.empty:
        editor_df = pd.DataFrame([{c: "" for c in ["business_name", "highlight_color"]}])
    edited = st.data_editor(editor_df, use_container_width=True, num_rows="dynamic", height=430, hide_index=True)
    if st.button("Save edited Highlight Store", use_container_width=True, key="save_highlight_editor"):
        save_highlight_store_records(edited, replace_all=True)
        st.success("Highlight Store updated.")
        st.rerun()
    render_clear_button("Clear Highlight Store", clear_highlight_store_records, "clear_highlight")

# -----------------------------
# 6. Sales Agent
# -----------------------------
elif selected_menu == "Sales Agent Sales Maintenance":
    st.header("Sales Agent Sales Maintenance")
    st.caption("Current format unchanged. Invalid HAU Models are ignored on save.")
    current = read_sales_agent_records()
    download_df = ensure_download_df(
        current,
        ["sales_date", "channel", "model", "avl_soh_amt", "soo_amt", "daily_sales_amt", "price", "sum_avl_soh", "sum_soo", "sales_qty"],
    )
    st.download_button(
        "Download Current Sales Agent Data",
        data=to_excel_bytes(download_df, "Sales Agent"),
        file_name="sales_agent_current.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
    mode = st.radio("Save mode", ["Append uploaded rows", "Replace all Sales Agent data"], horizontal=True, key="sa_mode")
    uploaded = st.file_uploader("Upload Sales Agent sellout file", type=["xlsx", "csv"], key="sa_upload")
    if uploaded is not None:
        try:
            preview = normalize_sales_agent_df(_load_raw_file(uploaded))
            st.success(f"File valid. Rows ready before model check: {len(preview):,}")
            st.dataframe(preview.head(80), use_container_width=True, height=300)
            if st.button("Save Sales Agent Data", use_container_width=True, key="save_sa"):
                saved, ignored = save_sales_agent_records(preview, replace_all=(mode == "Replace all Sales Agent data"))
                show_upload_result(saved, ignored)
                st.rerun()
        except Exception as exc:
            st.error(f"Upload failed: {exc}")
    st.subheader("Current saved data")
    st.caption(f"Rows: {sales_meta['rows']:,} | Date range: {sales_meta['min_date']} → {sales_meta['max_date']}")
    st.dataframe(current.head(1000), use_container_width=True, height=430, hide_index=True)
    render_clear_button("Clear Sales Agent Sales", clear_sales_agent_records, "clear_sa")

# -----------------------------
# 6. Status
# -----------------------------
elif selected_menu == "Shared Database Status":
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
