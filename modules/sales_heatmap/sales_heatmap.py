
# -*- coding: utf-8 -*-
import os
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

try:
    import folium
    from folium.plugins import MarkerCluster
    from streamlit_folium import st_folium
except Exception:
    folium = None
    MarkerCluster = None
    st_folium = None

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = str(BASE_DIR / "data" / "app_data.db")

st.markdown(
    """
<style>
.block-container {padding-top: 1rem; padding-bottom: 1rem;}
div[data-testid="stMetric"] {
    background-color: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.08);
    padding: 10px 14px;
    border-radius: 12px;
}
div[data-testid="stDataFrame"] {
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 12px;
    overflow: hidden;
}
.small-note {color: #9CA3AF; font-size: 12px;}
/* reduce dark loading overlay feeling */
div[data-testid="stSpinner"] {
    background: transparent !important;
}
div.stSpinner > div {
    background: transparent !important;
    border: none !important;
}
</style>
    """,
    unsafe_allow_html=True,
)


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def ensure_column(conn, table_name, column_name, column_def):
    cols = pd.read_sql_query(f"PRAGMA table_info({table_name})", conn)
    if column_name not in cols["name"].tolist():
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")


def init_db():
    with get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS store_locations (
                business_name TEXT PRIMARY KEY,
                retailer TEXT,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sales_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sales_date TEXT NOT NULL,
                business_name TEXT NOT NULL,
                model TEXT NOT NULL,
                sales REAL NOT NULL,
                uploaded_at TEXT NOT NULL
            )
            """
        )
        ensure_column(conn, "store_locations", "retailer", "TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_date ON sales_records(sales_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_store ON sales_records(business_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sales_model ON sales_records(model)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_store_retailer ON store_locations(retailer)")
        conn.commit()


init_db()


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def rename_columns_safely(df, mapping_options):
    df = df.copy()
    current_cols = set(df.columns)
    rename_map = {}
    for target_col, candidates in mapping_options.items():
        for c in candidates:
            if c.lower() in current_cols:
                rename_map[c.lower()] = target_col
                break
    return df.rename(columns=rename_map)


def clean_business_name(x):
    if pd.isna(x):
        return ""
    return str(x).strip().upper()


def clean_retailer(x):
    if pd.isna(x) or str(x).strip() == "":
        return "Unknown"
    return str(x).strip()


def load_uploaded_file(uploaded_file):
    if uploaded_file is None:
        return None
    name = uploaded_file.name.lower()
    uploaded_file.seek(0)
    if name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    if name.endswith(".xlsx"):
        return pd.read_excel(uploaded_file)
    raise ValueError(f"Unsupported file type: {uploaded_file.name}")


def validate_required_columns(df, required_cols):
    return [c for c in required_cols if c not in df.columns]


def normalize_store_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df_raw)
    df = rename_columns_safely(
        df,
        {
            "business_name": ["business name", "business_name", "store name", "store", "customer", "customer name"],
            "retailer": ["retailer", "channel", "banner", "account", "group"],
            "latitude": ["latitude", "lat", "y"],
            "longitude": ["longitude", "lon", "lng", "long", "x"],
        },
    )
    missing = validate_required_columns(df, ["business_name", "latitude", "longitude"])
    if missing:
        raise ValueError(f"Store location file missing required columns: {missing}")
    if "retailer" not in df.columns:
        df["retailer"] = "Unknown"

    out = df[["business_name", "retailer", "latitude", "longitude"]].copy()
    out["business_name"] = out["business_name"].apply(clean_business_name)
    out["retailer"] = out["retailer"].apply(clean_retailer)
    out["latitude"] = pd.to_numeric(out["latitude"], errors="coerce")
    out["longitude"] = pd.to_numeric(out["longitude"], errors="coerce")
    out = out.dropna(subset=["latitude", "longitude"])
    out = out[out["business_name"] != ""]
    out = out.drop_duplicates(subset=["business_name"], keep="last")
    return out


def _parse_sales_date(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    return parsed.dt.strftime("%Y-%m-%d")


def normalize_sales_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df_raw)
    df = rename_columns_safely(
        df,
        {
            "sales_date": ["date", "sales date", "sales_date", "week", "month", "period"],
            "business_name": ["business name", "business_name", "store name", "store", "customer", "customer name"],
            "model": ["model", "sku", "hau model", "product model"],
            "sales": ["sales", "qty", "quantity", "sell out", "sales qty", "units"],
        },
    )
    missing = validate_required_columns(df, ["sales_date", "business_name", "model", "sales"])
    if missing:
        raise ValueError(f"Sales file missing required columns: {missing}")

    out = df[["sales_date", "business_name", "model", "sales"]].copy()
    out["business_name"] = out["business_name"].apply(clean_business_name)
    out["model"] = out["model"].astype(str).str.strip()
    out["sales"] = pd.to_numeric(out["sales"], errors="coerce").fillna(0)
    out["sales_date"] = _parse_sales_date(out["sales_date"])
    out = out.dropna(subset=["sales_date"])
    out = out[(out["business_name"] != "") & (out["model"] != "")]
    return out


def save_store_locations(df: pd.DataFrame, replace_all: bool = False):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    to_save = df.copy()
    to_save["updated_at"] = now
    with get_conn() as conn:
        if replace_all:
            conn.execute("DELETE FROM store_locations")
        for _, row in to_save.iterrows():
            conn.execute(
                """
                INSERT INTO store_locations (business_name, retailer, latitude, longitude, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(business_name) DO UPDATE SET
                    retailer=excluded.retailer,
                    latitude=excluded.latitude,
                    longitude=excluded.longitude,
                    updated_at=excluded.updated_at
                """,
                (row["business_name"], row["retailer"], float(row["latitude"]), float(row["longitude"]), row["updated_at"]),
            )
        conn.commit()


def save_sales_records(df: pd.DataFrame, replace_all: bool = False):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    to_save = df.copy()
    to_save["uploaded_at"] = now
    with get_conn() as conn:
        if replace_all:
            conn.execute("DELETE FROM sales_records")
        to_save.to_sql("sales_records", conn, if_exists="append", index=False)
        conn.commit()


def read_store_locations() -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql_query(
            "SELECT business_name, retailer, latitude, longitude, updated_at FROM store_locations", conn
        )


def read_sales_records() -> pd.DataFrame:
    with get_conn() as conn:
        return pd.read_sql_query(
            "SELECT id, sales_date, business_name, model, sales, uploaded_at FROM sales_records", conn
        )


def delete_store_locations(business_names):
    if not business_names:
        return
    with get_conn() as conn:
        conn.executemany("DELETE FROM store_locations WHERE business_name = ?", [(x,) for x in business_names])
        conn.commit()


def delete_sales_records(record_ids):
    if not record_ids:
        return
    with get_conn() as conn:
        conn.executemany("DELETE FROM sales_records WHERE id = ?", [(int(x),) for x in record_ids])
        conn.commit()


def clear_all_store_locations():
    with get_conn() as conn:
        conn.execute("DELETE FROM store_locations")
        conn.commit()


def clear_all_sales_records():
    with get_conn() as conn:
        conn.execute("DELETE FROM sales_records")
        conn.commit()


def db_summary():
    with get_conn() as conn:
        store_cnt = conn.execute("SELECT COUNT(*) FROM store_locations").fetchone()[0]
        sales_cnt = conn.execute("SELECT COUNT(*) FROM sales_records").fetchone()[0]
        date_min, date_max = conn.execute("SELECT MIN(sales_date), MAX(sales_date) FROM sales_records").fetchone()
    return store_cnt, sales_cnt, date_min, date_max


def get_color(sales_value, q1, q2, q3):
    if sales_value <= q1:
        return "#00E676"
    if sales_value <= q2:
        return "#FFD740"
    if sales_value <= q3:
        return "#FF6E40"
    return "#FF1744"


def top_models_text(df_group, top_n=5):
    tmp = (
        df_group.groupby("model", dropna=False)["sales"]
        .sum()
        .reset_index()
        .sort_values("sales", ascending=False)
        .head(top_n)
    )
    lines = []
    for _, r in tmp.iterrows():
        model = str(r["model"]) if pd.notna(r["model"]) else "Unknown"
        lines.append(f"{model}: {r['sales']:.0f}")
    return " | ".join(lines)


def prepare_analysis_data(store_df: pd.DataFrame, sales_df: pd.DataFrame):
    store_sales_summary = (
        sales_df.groupby("business_name", as_index=False)
        .agg(total_sales=("sales", "sum"), model_count=("model", "nunique"))
    )
    store_model_sales_summary = (
        sales_df.groupby(["business_name", "model"], as_index=False)["sales"]
        .sum()
        .sort_values(["business_name", "sales"], ascending=[True, False])
    )
    top_model_df = (
        sales_df.groupby("business_name")
        .apply(lambda x: pd.Series({"top_models": top_models_text(x)}), include_groups=False)
        .reset_index()
    ) if not sales_df.empty else pd.DataFrame(columns=["business_name", "top_models"])

    merged_df = (
        store_df.merge(store_sales_summary, on="business_name", how="left")
        .merge(top_model_df, on="business_name", how="left")
    )
    merged_df["retailer"] = merged_df["retailer"].fillna("Unknown")
    merged_df["total_sales"] = merged_df["total_sales"].fillna(0)
    merged_df["model_count"] = merged_df["model_count"].fillna(0).astype(int)
    merged_df["top_models"] = merged_df["top_models"].fillna("No sales data")

    sales_without_location = sales_df[~sales_df["business_name"].isin(store_df["business_name"])].copy()
    sales_without_location_summary = (
        sales_without_location.groupby("business_name", as_index=False)["sales"]
        .sum()
        .sort_values("sales", ascending=False)
    ) if not sales_without_location.empty else pd.DataFrame(columns=["business_name", "sales"])

    return merged_df, store_model_sales_summary, sales_without_location_summary


def add_sales_week_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["sales_date"] = pd.to_datetime(out["sales_date"])
    iso = out["sales_date"].dt.isocalendar()
    out["iso_year"] = iso["year"].astype(int)
    out["iso_week"] = iso["week"].astype(int)
    out["year_week_num"] = out["iso_year"] * 100 + out["iso_week"]
    out["week_label"] = out["iso_year"].astype(str) + "-W" + out["iso_week"].astype(str).str.zfill(2)
    return out


def build_folium_map(df_map: pd.DataFrame):
    if folium is None or st_folium is None:
        st.error("Please install folium and streamlit-folium first: pip install folium streamlit-folium")
        return
    if df_map.empty:
        st.info("No stores to display on the map.")
        return

    positive_sales = df_map.loc[df_map["total_sales"] > 0, "total_sales"]
    if len(positive_sales) > 0:
        q1 = positive_sales.quantile(0.25)
        q2 = positive_sales.quantile(0.50)
        q3 = positive_sales.quantile(0.75)
    else:
        q1 = q2 = q3 = 0

    center_lat = float(df_map["latitude"].mean())
    center_lon = float(df_map["longitude"].mean())

    fmap = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=4,
        tiles="CartoDB dark_matter",
        prefer_canvas=True,
        fade_animation=False,
        zoom_animation=True,
        marker_zoom_animation=True,
    )

    cluster = MarkerCluster(
        name="Stores",
        overlay=True,
        control=False,
        showCoverageOnHover=False,
        spiderfyOnMaxZoom=True,
        disableClusteringAtZoom=8,
        maxClusterRadius=50,
    ).add_to(fmap)

    for _, row in df_map.iterrows():
        sales = float(row["total_sales"])
        color = get_color(sales, q1, q2, q3)
        popup_html = f"""
        <div style="min-width:240px;">
            <b>{row['business_name']}</b><br/>
            Retailer: {row['retailer']}<br/>
            Sales: {sales:,.0f}<br/>
            Model Count: {int(row['model_count'])}<br/>
            Top Models: {row['top_models']}
        </div>
        """
        marker_html = f"""
        <div style="
            width:12px;
            height:12px;
            border-radius:50%;
            background:{color};
            border:2px solid rgba(255,255,255,0.95);
            box-shadow:0 0 8px {color};
        "></div>
        """
        folium.Marker(
            location=[float(row["latitude"]), float(row["longitude"] )],
            tooltip=f"{row['business_name']} | Sales: {sales:,.0f}",
            popup=folium.Popup(popup_html, max_width=320),
            icon=folium.DivIcon(html=marker_html),
        ).add_to(cluster)

    st_folium(
        fmap,
        use_container_width=True,
        height=720,
        key="store_sales_cluster_map",
        returned_objects=[],
    )


st.sidebar.title("Store Sales Map")
page = st.sidebar.radio("Navigation", ["Analysis", "Data Upload & Storage"], index=0)

store_cnt, sales_cnt, date_min, date_max = db_summary()
st.sidebar.markdown("---")
st.sidebar.caption(f"Saved store locations: {store_cnt:,}")
st.sidebar.caption(f"Saved sales rows: {sales_cnt:,}")
if date_min and date_max:
    st.sidebar.caption(f"Sales date range: {date_min} → {date_max}")


if page == "Data Upload & Storage":
    st.title("📥 Data Upload & Storage")
    st.caption("Store locations and sales data are uploaded separately and saved locally for long-term use.")

    a1, a2 = st.columns(2)

    with a1:
        st.subheader("1) Store Location Data")
        st.markdown("Required columns: **business name / latitude / longitude**")
        st.markdown("Optional column: **retailer**")
        store_mode = st.radio("Store location save mode", ["Append / Update existing stores", "Replace all store locations"], key="store_mode")
        store_file = st.file_uploader("Upload Store Location File", type=["csv", "xlsx"], key="store_upload_file")

        if store_file is not None:
            try:
                raw = load_uploaded_file(store_file)
                preview_df = normalize_store_df(raw)
                st.success(f"Store file valid. Rows ready to save: {len(preview_df):,}")
                st.dataframe(preview_df.head(20), use_container_width=True, height=320)
                if st.button("Save Store Location Data", use_container_width=True):
                    save_store_locations(preview_df, replace_all=(store_mode == "Replace all store locations"))
                    st.success("Store location data saved.")
                    st.rerun()
            except Exception as e:
                st.error(str(e))

    with a2:
        st.subheader("2) Sales Data")
        st.markdown("Required columns: **date / business name / model / sales**")
        sales_mode = st.radio("Sales save mode", ["Append sales records", "Replace all sales records"], key="sales_mode")
        sales_file = st.file_uploader("Upload Sales File", type=["csv", "xlsx"], key="sales_upload_file")

        if sales_file is not None:
            try:
                raw = load_uploaded_file(sales_file)
                preview_df = normalize_sales_df(raw)
                st.success(f"Sales file valid. Rows ready to save: {len(preview_df):,}")
                st.dataframe(preview_df.head(20), use_container_width=True, height=320)
                if st.button("Save Sales Data", use_container_width=True):
                    save_sales_records(preview_df, replace_all=(sales_mode == "Replace all sales records"))
                    st.success("Sales data saved.")
                    st.rerun()
            except Exception as e:
                st.error(str(e))

    st.markdown("---")
    st.subheader("Current Saved Data")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Saved Stores", f"{store_cnt:,}")
    c2.metric("Saved Sales Rows", f"{sales_cnt:,}")
    c3.metric("Min Sales Date", date_min if date_min else "-")
    c4.metric("Max Sales Date", date_max if date_max else "-")

    store_saved = read_store_locations()
    sales_saved = read_sales_records()

    tab1, tab2, tab3 = st.tabs(["Store Locations", "Sales Records", "Delete Data"])
    with tab1:
        st.dataframe(store_saved, use_container_width=True, height=420)
    with tab2:
        st.dataframe(sales_saved.sort_values("sales_date", ascending=False), use_container_width=True, height=420)
    with tab3:
        d1, d2 = st.columns(2)
        with d1:
            st.markdown("#### Delete Store Locations")
            store_delete_options = store_saved["business_name"].dropna().astype(str).tolist()
            selected_store_delete = st.multiselect("Select store(s) to delete", options=store_delete_options, key="selected_store_delete")
            if st.button("Delete Selected Store Locations", type="primary", use_container_width=True):
                delete_store_locations(selected_store_delete)
                st.success(f"Deleted {len(selected_store_delete)} store location(s).")
                st.rerun()
            st.markdown("##### Danger Zone")
            if st.button("Clear All Store Locations", use_container_width=True):
                clear_all_store_locations()
                st.success("All store locations deleted.")
                st.rerun()

        with d2:
            st.markdown("#### Delete Sales Records")
            sales_saved_display = sales_saved.copy()
            sales_saved_display["label"] = (
                sales_saved_display["id"].astype(str) + " | " +
                sales_saved_display["sales_date"].astype(str) + " | " +
                sales_saved_display["business_name"].astype(str) + " | " +
                sales_saved_display["model"].astype(str) + " | " +
                sales_saved_display["sales"].astype(str)
            )
            sales_delete_map = dict(zip(sales_saved_display["label"], sales_saved_display["id"]))
            selected_sales_labels = st.multiselect("Select sales row(s) to delete", options=sales_saved_display["label"].tolist(), key="selected_sales_delete")
            if st.button("Delete Selected Sales Records", type="primary", use_container_width=True):
                delete_sales_records([sales_delete_map[x] for x in selected_sales_labels])
                st.success(f"Deleted {len(selected_sales_labels)} sales record(s).")
                st.rerun()
            st.markdown("##### Danger Zone")
            if st.button("Clear All Sales Records", use_container_width=True):
                clear_all_sales_records()
                st.success("All sales records deleted.")
                st.rerun()
    st.stop()


st.title("🗺️ Store Sales Distribution Analysis")
st.caption("Interactive map with retailer/date/week filters and persistent local data storage.")

store_df = read_store_locations()
sales_df = read_sales_records()

if store_df.empty:
    st.warning("No saved store location data yet. Please go to 'Data Upload & Storage' and upload the store file first.")
    st.stop()
if sales_df.empty:
    st.warning("No saved sales data yet. Please go to 'Data Upload & Storage' and upload the sales file first.")
    st.stop()
if folium is None or st_folium is None:
    st.error("This map version needs folium and streamlit-folium. Please install: pip install folium streamlit-folium")
    st.stop()

sales_df = add_sales_week_columns(sales_df)
all_models = sorted(sales_df["model"].dropna().astype(str).unique().tolist())
all_retailers = sorted(store_df["retailer"].fillna("Unknown").astype(str).unique().tolist())
week_lookup = (
    sales_df[["year_week_num", "week_label"]]
    .drop_duplicates()
    .sort_values("year_week_num")
    .reset_index(drop=True)
)
week_labels = week_lookup["week_label"].tolist()
min_date = sales_df["sales_date"].min().date()
max_date = sales_df["sales_date"].max().date()

with st.sidebar:
    st.markdown("---")
    st.subheader("Analysis Filters")
    selected_dates = st.date_input("Sales date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)

    st.markdown("**Week range**")
    if len(week_labels) > 0:
        week_start_idx, week_end_idx = st.select_slider(
            "Week range",
            options=list(range(len(week_labels))),
            value=(0, len(week_labels) - 1),
            format_func=lambda x: week_labels[x],
        )
        selected_week_range_text = f"{week_labels[week_start_idx]} → {week_labels[week_end_idx]}"
    else:
        week_start_idx = week_end_idx = None
        selected_week_range_text = "No weeks"

    selected_retailers = st.multiselect("Retailer", options=all_retailers, default=[])
    show_zero_sales = st.checkbox("Show stores with zero sales", value=True)
    selected_model = st.selectbox("Model", ["All Models"] + all_models)
    top_n_table = st.slider("Top N stores", 10, 100, 20, 5)

if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
    start_date, end_date = selected_dates
else:
    start_date = end_date = selected_dates

filtered_sales = sales_df[
    (sales_df["sales_date"].dt.date >= start_date) &
    (sales_df["sales_date"].dt.date <= end_date)
].copy()

if week_start_idx is not None and week_end_idx is not None:
    start_week_num = int(week_lookup.iloc[week_start_idx]["year_week_num"])
    end_week_num = int(week_lookup.iloc[week_end_idx]["year_week_num"])
    filtered_sales = filtered_sales[
        (filtered_sales["year_week_num"] >= start_week_num) &
        (filtered_sales["year_week_num"] <= end_week_num)
    ].copy()

filtered_store_df = store_df.copy()
if selected_retailers:
    filtered_store_df = filtered_store_df[filtered_store_df["retailer"].isin(selected_retailers)].copy()

if selected_model != "All Models":
    filtered_sales = filtered_sales[filtered_sales["model"].astype(str) == selected_model].copy()

if not filtered_store_df.empty:
    filtered_sales = filtered_sales[filtered_sales["business_name"].isin(filtered_store_df["business_name"])].copy()

merged_df, store_model_sales_summary, sales_without_location_summary = prepare_analysis_data(
    filtered_store_df[["business_name", "retailer", "latitude", "longitude"]],
    filtered_sales[["sales_date", "business_name", "model", "sales"]],
)

if not show_zero_sales:
    merged_df_display = merged_df[merged_df["total_sales"] > 0].copy()
else:
    merged_df_display = merged_df.copy()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Stores with Location", f"{len(filtered_store_df):,}")
col2.metric("Filtered Sales Volume", f"{filtered_sales['sales'].sum():,.0f}")
selling_store_mean = merged_df_display.loc[merged_df_display["total_sales"] > 0, "total_sales"].mean()
col3.metric("Avg Sales / Selling Store", f"{0 if pd.isna(selling_store_mean) else selling_store_mean:,.1f}")
col4.metric("Models in Filter", f"{filtered_sales['model'].nunique():,}")

st.markdown("---")
st.subheader("Overall Store Sales Map")
st.markdown(
    "<div class='small-note'>Loading overlay reduced and week filter changed to a compact range selector.</div>",
    unsafe_allow_html=True,
)
map_key = (
    f"store_sales_cluster_map_"
    f"{start_date}_{end_date}_"
    f"{start_week_num if week_start_idx is not None else 'na'}_"
    f"{end_week_num if week_end_idx is not None else 'na'}_"
    f"{selected_model}_"
    f"{'|'.join(selected_retailers) if selected_retailers else 'all'}_"
    f"{int(show_zero_sales)}_"
    f"{int(round(float(filtered_sales['sales'].sum()), 0))}_"
    f"{len(merged_df_display)}"
)
build_folium_map(merged_df_display, map_key=map_key)

st.markdown("---")
left, right = st.columns([1.2, 1])
with left:
    st.subheader(f"Top Stores - {selected_model}")
    model_rank_df = merged_df_display.sort_values("total_sales", ascending=False).head(top_n_table)
    st.dataframe(
        model_rank_df[["business_name", "retailer", "total_sales", "model_count", "top_models"]].reset_index(drop=True),
        use_container_width=True,
        height=520,
    )
with right:
    st.subheader("Filter Snapshot")
    st.write(f"**Date range:** {start_date} → {end_date}")
    st.write(f"**Week range:** {selected_week_range_text}")
    st.write(f"**Retailer:** {', '.join(selected_retailers) if selected_retailers else 'All Retailers'}")
    st.write(f"**Model:** {selected_model}")
    st.write(f"**Show zero-sales stores:** {'Yes' if show_zero_sales else 'No'}")
    st.write(f"**Matched stores in view:** {len(merged_df_display):,}")
    st.write(f"**Unmatched sales stores:** {len(sales_without_location_summary):,}")

st.markdown("---")
tab1, tab2, tab3 = st.tabs(["Store Summary", "Store-Model Summary", "Unmatched Stores"])
with tab1:
    st.subheader("Store Summary")
    store_summary_display = (
        merged_df[["business_name", "retailer", "latitude", "longitude", "total_sales", "model_count", "top_models"]]
        .sort_values("total_sales", ascending=False)
        .reset_index(drop=True)
    )
    st.dataframe(store_summary_display, use_container_width=True, height=520)
    st.download_button(
        "Download Store Summary CSV",
        data=store_summary_display.to_csv(index=False).encode("utf-8-sig"),
        file_name="store_sales_summary.csv",
        mime="text/csv",
    )

with tab2:
    st.subheader("Store-Model Sales Summary")
    st.dataframe(store_model_sales_summary.reset_index(drop=True), use_container_width=True, height=520)
    st.download_button(
        "Download Store-Model Summary CSV",
        data=store_model_sales_summary.to_csv(index=False).encode("utf-8-sig"),
        file_name="store_model_sales_summary.csv",
        mime="text/csv",
    )

with tab3:
    st.subheader("Sales Stores Without Location Match")
    if sales_without_location_summary.empty:
        st.success("All filtered sales stores matched with location data.")
    else:
        st.warning(f"{len(sales_without_location_summary):,} stores in filtered sales data do not have matching location.")
        st.dataframe(sales_without_location_summary, use_container_width=True, height=520)
        st.download_button(
            "Download Unmatched Stores CSV",
            data=sales_without_location_summary.to_csv(index=False).encode("utf-8-sig"),
            file_name="sales_without_location.csv",
            mime="text/csv",
        )
