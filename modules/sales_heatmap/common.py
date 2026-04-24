# -*- coding: utf-8 -*-
import math
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


def apply_local_style():
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
    div[data-testid="stSpinner"] {background: transparent !important;}
    div.stSpinner > div {background: transparent !important; border: none !important;}
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
    init_db()
    with get_conn() as conn:
        return pd.read_sql_query(
            "SELECT business_name, retailer, latitude, longitude, updated_at FROM store_locations", conn
        )


def read_sales_records() -> pd.DataFrame:
    init_db()
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
        return "#34C759"   # green
    if sales_value <= q2:
        return "#FFD60A"   # yellow
    if sales_value <= q3:
        return "#FF9F0A"   # orange
    return "#FF3B30"       # red


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


def _safe_quantile_thresholds(series: pd.Series):
    s = pd.to_numeric(series, errors="coerce").fillna(0)
    s = s[s > 0]
    if s.empty:
        return 0, 0, 0

    q1 = float(s.quantile(0.65))
    q2 = float(s.quantile(0.70))
    q3 = float(s.quantile(0.725))

    if q2 < q1:
        q2 = q1
    if q3 < q2:
        q3 = q2

    return q1, q2, q3


def _cluster_icon_create_function(q1: float, q2: float, q3: float):
    return f"""
    function(cluster) {{
        var markers = cluster.getAllChildMarkers();
        var totalSales = 0;

        markers.forEach(function(marker) {{
            var sales = 0;

            if (typeof marker.options.salesValue !== 'undefined') {{
                sales = Number(marker.options.salesValue) || 0;
            }} else if (marker.options && marker.options.icon && marker.options.icon.options && marker.options.icon.options.html) {{
                var html = marker.options.icon.options.html || '';
                var match = html.match(/data-sales=["']?([0-9.]+)["']?/i);
                if (match) {{
                    sales = Number(match[1]) || 0;
                }}
            }}

            totalSales += sales;
        }});

        var q1 = {q1};
        var q2 = {q2};
        var q3 = {q3};

        var color = '#34C759';
        if (totalSales <= q1) {{
            color = '#34C759';
        }} else if (totalSales <= q2) {{
            color = '#FFD60A';
        }} else if (totalSales <= q3) {{
            color = '#FF9F0A';
        }} else {{
            color = '#FF3B30';
        }}

        var textColor = '#ffffff';
        if (color === '#FFD60A') {{
            textColor = '#1f2937';
        }}

        var size = 34;
        if (totalSales > q1) size = 40;
        if (totalSales > q2) size = 46;
        if (totalSales > q3) size = 54;

        var fontSize = 12;
        if (size >= 40) fontSize = 13;
        if (size >= 46) fontSize = 14;
        if (size >= 54) fontSize = 15;

        var html = `
            <div style="
                width:${{size}}px;
                height:${{size}}px;
                border-radius:50%;
                background:${{color}};
                border:3px solid rgba(255,255,255,0.95);
                box-shadow:0 0 12px ${{color}};
                display:flex;
                align-items:center;
                justify-content:center;
                color:${{textColor}};
                font-weight:800;
                font-size:${{fontSize}}px;
                line-height:1;
                text-align:center;
            ">${{Math.round(totalSales)}}</div>
        `;

        return L.divIcon({{
            html: html,
            className: 'custom-sales-cluster',
            iconSize: L.point(size, size)
        }});
    }}
    """


def build_folium_map(df_map: pd.DataFrame, map_key: str = "store_sales_cluster_map"):
    if folium is None or st_folium is None:
        st.error("Please install folium and streamlit-folium first: pip install folium streamlit-folium")
        return
    if df_map.empty:
        st.info("No stores to display on the map.")
        return

    df_map = df_map.copy()
    df_map["total_sales"] = pd.to_numeric(df_map["total_sales"], errors="coerce").fillna(0)

    point_q1, point_q2, point_q3 = _safe_quantile_thresholds(df_map["total_sales"])

    positive_points = df_map[df_map["total_sales"] > 0]["total_sales"]
    positive_points_count = len(positive_points)

    if positive_points_count >= 4:
        cluster_base = pd.Series([
            positive_points.quantile(0.25),
            positive_points.quantile(0.50),
            positive_points.quantile(0.75),
            positive_points.sum()
        ])
    elif positive_points_count > 0:
        cluster_base = positive_points
    else:
        cluster_base = pd.Series([0])

    cluster_q1, cluster_q2, cluster_q3 = _safe_quantile_thresholds(cluster_base)

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
        maxClusterRadius=55,
        icon_create_function=_cluster_icon_create_function(cluster_q1, cluster_q2, cluster_q3),
    ).add_to(fmap)

    for _, row in df_map.iterrows():
        sales = float(row["total_sales"])
        color = get_color(sales, point_q1, point_q2, point_q3)
        lat = float(row["latitude"])
        lon = float(row["longitude"])

        popup_html = f"""
        <div style="min-width:240px;">
            <b>{row['business_name']}</b><br/>
            Retailer: {row['retailer']}<br/>
            Sales: {sales:,.0f}<br/>
            Model Count: {int(row['model_count'])}<br/>
            Top Models: {row['top_models']}
        </div>
        """

        if sales <= point_q1:
            marker_size = 8
        elif sales <= point_q2:
            marker_size = 10
        elif sales <= point_q3:
            marker_size = 12
        else:
            marker_size = 14

        glow = 6 if marker_size <= 8 else 8

        marker_html = f"""
        <div data-sales="{sales}" style="
            width:{marker_size}px;
            height:{marker_size}px;
            border-radius:50%;
            background:{color};
            border:2px solid rgba(255,255,255,0.95);
            box-shadow:0 0 {glow}px {color};
        "></div>
        """

        folium.Marker(
            location=[lat, lon],
            tooltip=f"{row['business_name']} | Sales: {sales:,.0f}",
            popup=folium.Popup(popup_html, max_width=320),
            icon=folium.DivIcon(html=marker_html),
            salesValue=sales,
        ).add_to(cluster)

    st_folium(
        fmap,
        use_container_width=True,
        height=600,
        key=map_key,
        returned_objects=[],
    )
