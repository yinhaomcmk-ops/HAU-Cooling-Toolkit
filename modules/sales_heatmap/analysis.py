import pandas as pd
import streamlit as st

try:
    import folium
    from folium.plugins import HeatMap
    from streamlit_folium import st_folium
except Exception:
    folium = None
    HeatMap = None
    st_folium = None

from modules.sales_heatmap.common import (
    apply_local_style,
    init_db,
    read_store_locations,
    read_sales_records,
    db_summary,
    add_sales_week_columns,
    prepare_analysis_data,
    build_folium_map,
)

st.markdown(
    """
<style>
.block-container {
    padding-top: 1.6rem;
    padding-bottom: 1rem;
    padding-left: 1.5rem;
    padding-right: 1.5rem;
    max-width: 2000px;
    background: transparent;
}
h1 { font-size: 24px !important; }
h2 { font-size: 18px !important; }
h3 { font-size: 16px !important; }
.comp-card {
    border: 1px solid #e6eaf1;
    border-radius: 10px;
    padding: 12px 12px 4px 12px;
    margin-bottom: 12px;
    background: #fafbfd;
}
.small-note {
    color: #6b7280;
    font-size: 12px;
}
</style>
""",
    unsafe_allow_html=True,
)
apply_local_style()
init_db()


def build_sales_heatmap(df_map: pd.DataFrame, map_key: str = "sales_heatmap_density_map"):
    if folium is None or HeatMap is None or st_folium is None:
        st.error("Please install folium and streamlit-folium first: pip install folium streamlit-folium")
        return
    if df_map.empty:
        st.info("No stores with sales available for heatmap.")
        return

    df_map = df_map.copy()
    df_map["total_sales"] = pd.to_numeric(df_map["total_sales"], errors="coerce").fillna(0)
    df_map = df_map[df_map["total_sales"] > 0].copy()

    if df_map.empty:
        st.info("No positive sales available for heatmap.")
        return

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

    heat_data = [
        [float(row["latitude"]), float(row["longitude"]), float(row["total_sales"])]
        for _, row in df_map.iterrows()
    ]

    max_sales = max(df_map["total_sales"].max(), 1)

    HeatMap(
        heat_data,
        min_opacity=0.25,
        radius=28,
        blur=22,
        max_zoom=8,
        max_val=max_sales,
    ).add_to(fmap)

    for _, row in df_map.sort_values("total_sales", ascending=False).head(50).iterrows():
        folium.CircleMarker(
            location=[float(row["latitude"]), float(row["longitude"])],
            radius=4,
            weight=1,
            color="rgba(255,255,255,0.85)",
            fill=True,
            fill_color="rgba(255,255,255,0.18)",
            fill_opacity=0.25,
            tooltip=f"{row['business_name']} | Sales: {row['total_sales']:,.0f}",
        ).add_to(fmap)

    st_folium(
        fmap,
        use_container_width=True,
        height=600,
        key=map_key,
        returned_objects=[],
    )


st.title("Sales Heatmap | Analysis")
st.caption("Interactive sales map and heatmap with persistent local data storage.")

store_df = read_store_locations()
sales_df = read_sales_records()

if store_df.empty:
    st.warning("No saved store location data yet. Please go to Database > Sales Heatmap Upload first.")
    st.stop()
if sales_df.empty:
    st.warning("No saved sales data yet. Please go to Database > Sales Heatmap Upload first.")
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

st.sidebar.markdown("## Analysis Filters")
selected_dates = st.sidebar.date_input(
    "Sales date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

if len(week_labels) > 0:
    week_start_idx, week_end_idx = st.sidebar.select_slider(
        "Week range",
        options=list(range(len(week_labels))),
        value=(0, len(week_labels) - 1),
        format_func=lambda x: week_labels[x],
    )
    selected_week_range_text = f"{week_labels[week_start_idx]} → {week_labels[week_end_idx]}"
else:
    week_start_idx = week_end_idx = None
    selected_week_range_text = "No weeks"

selected_retailers = st.sidebar.multiselect("Retailer", options=all_retailers, default=[])
selected_model = st.sidebar.selectbox("Model", ["All Models"] + all_models)
show_zero_sales = st.sidebar.checkbox("Show stores with zero sales", value=True)
show_cluster_map = st.sidebar.checkbox("Show cluster map", value=True)
show_heatmap = st.sidebar.checkbox("Show sales heatmap", value=True)

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

merged_df, _, sales_without_location_summary = prepare_analysis_data(
    filtered_store_df[["business_name", "retailer", "latitude", "longitude"]],
    filtered_sales[["sales_date", "business_name", "model", "sales"]],
)

merged_df_display = merged_df[merged_df["total_sales"] > 0].copy() if not show_zero_sales else merged_df.copy()

snapshot_left, snapshot_right = st.columns([1.3, 1])
with snapshot_left:
    st.subheader("Filter Snapshot")
    st.write(f"**Date range:** {start_date} → {end_date}")
    st.write(f"**Week range:** {selected_week_range_text}")
    st.write(f"**Retailer:** {', '.join(selected_retailers) if selected_retailers else 'All Retailers'}")
    st.write(f"**Model:** {selected_model}")
with snapshot_right:
    st.subheader("View Summary")
    st.write(f"**Show zero-sales stores:** {'Yes' if show_zero_sales else 'No'}")
    st.write(f"**Stores in current view:** {len(merged_df_display):,}")
    st.write(f"**Stores with positive sales:** {(merged_df_display['total_sales'] > 0).sum():,}")
    st.write(f"**Mapped sales volume:** {merged_df_display['total_sales'].sum():,.0f}")

if show_cluster_map:
    st.markdown("---")
    st.subheader("Cluster Sales Map")
    st.markdown(
        "<div class='small-note'>Cluster values represent selected sales total in each area.</div>",
        unsafe_allow_html=True,
    )
    build_folium_map(merged_df_display, map_key="cluster_sales_map")

if show_heatmap:
    st.markdown("---")
    st.subheader("Sales Heatmap")
    st.markdown(
        "<div class='small-note'>Heatmap intensity is weighted by total sales volume, showing where sales are hottest.</div>",
        unsafe_allow_html=True,
    )
    build_sales_heatmap(merged_df_display, map_key="sales_volume_heatmap")
