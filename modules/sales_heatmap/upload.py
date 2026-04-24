import streamlit as st

from modules.sales_heatmap.common import (
    apply_local_style,
    init_db,
    db_summary,
    load_uploaded_file,
    normalize_store_df,
    normalize_sales_df,
    save_store_locations,
    save_sales_records,
    read_store_locations,
    read_sales_records,
    delete_store_locations,
    delete_sales_records,
    clear_all_store_locations,
    clear_all_sales_records,
)

apply_local_style()
init_db()

st.title("Database | Sales Heatmap Upload")
st.caption("Store locations and sales data are uploaded separately and saved locally for long-term use.")

store_cnt, sales_cnt, date_min, date_max = db_summary()

c1, c2 = st.columns(2)
with c1:
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

with c2:
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
m1, m2, m3, m4 = st.columns(4)
m1.metric("Saved Stores", f"{store_cnt:,}")
m2.metric("Saved Sales Rows", f"{sales_cnt:,}")
m3.metric("Min Sales Date", date_min if date_min else "-")
m4.metric("Max Sales Date", date_max if date_max else "-")

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
