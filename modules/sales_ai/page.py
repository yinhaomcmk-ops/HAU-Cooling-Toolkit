from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from services.sales_ai_engine import (
    answer_question,
    apply_year_week_filters,
    build_diagnostic_tables,
    build_summary,
    filter_base_scope,
    filter_period,
    get_openai_model,
    is_openai_ready,
)
from services.sales_data_loader import (
    get_sales_agent_summary,
    init_all_shared_db,
    load_sales_agent_data,
    read_model_master_records,
    summarize_dataset,
)

BASE_DIR = Path(__file__).resolve().parents[2]

st.title("Sales Agent")
st.caption(
    "AI-powered sales diagnosis using shared platform data: sellout, Sales Heatmap distribution, "
    "and Value Chain profitability context. Data upload and master data maintenance are handled in Database."
)

init_all_shared_db()
db_meta = get_sales_agent_summary()
df = load_sales_agent_data()
model_master = read_model_master_records()
ai_ready = is_openai_ready()
openai_model = get_openai_model()
use_openai = ai_ready

# =========================
# Sidebar: status + filters only
# =========================
with st.sidebar:
    st.markdown("### Sales Agent")
    st.caption(f"Sellout rows: {db_meta['rows']:,}")
    st.caption(f"Sellout date range: {db_meta['min_date']} → {db_meta['max_date']}")
    st.caption(f"Shared model master: {len(model_master):,} models")
    st.caption("OpenAI: " + ("Connected" if ai_ready else "Not configured"))

    if not ai_ready:
        st.info("Add OPENAI_API_KEY to .streamlit/secrets.toml to enable AI analysis.")

    st.markdown("---")
    st.markdown("### Filters")

    if not df.empty:
        channels = sorted(df["channel"].dropna().astype(str).unique().tolist())
        product_lines = sorted(df["product_line"].dropna().astype(str).unique().tolist())
        categories = sorted(df["category"].dropna().astype(str).unique().tolist())
        series_names = sorted(df["series_name"].dropna().astype(str).unique().tolist()) if "series_name" in df.columns else []
        models = sorted(df["model"].dropna().astype(str).unique().tolist())
        years = sorted(df["year"].dropna().astype(int).unique().tolist())
        weeks = sorted(df["week"].dropna().astype(int).unique().tolist())

        selected_channels = st.multiselect("Retailer / Channel", channels, default=[])
        selected_product_lines = st.multiselect("Product Line", product_lines, default=[])
        selected_categories = st.multiselect("Category", categories, default=[])
        selected_series = st.multiselect("Series", series_names, default=[])
        selected_models = st.multiselect("Model", models, default=[])

        period_mode = st.radio(
            "Analysis period",
            ["Latest available week", "Custom date range"],
            index=0,
        )

        min_date = df["sales_date"].min().date()
        max_date = df["sales_date"].max().date()

        if period_mode == "Latest available week":
            latest_dates = sorted(df["sales_date"].dt.date.dropna().unique().tolist())
            latest_date = latest_dates[-1]
            prev_date = latest_dates[-2] if len(latest_dates) >= 2 else latest_date - timedelta(days=7)
            if (latest_date - prev_date).days >= 6:
                start_date = latest_date
                end_date = latest_date
            else:
                start_date = max(min_date, latest_date - timedelta(days=6))
                end_date = latest_date
            st.caption(f"Using latest available sales period: {start_date} → {end_date}")
        else:
            selected_dates = st.date_input(
                "Sales date range",
                value=(max(min_date, max_date - timedelta(days=6)), max_date),
                min_value=min_date,
                max_value=max_date,
            )
            if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
                start_date, end_date = selected_dates
            else:
                start_date = end_date = selected_dates

        selected_years = st.multiselect("Year", years, default=[])
        selected_weeks = st.multiselect("Week", weeks, default=[])

        if st.button("Reset filters", use_container_width=True):
            for key in [
                "sales_agent_summary",
                "sales_agent_answer",
                "sales_agent_summary_signature",
            ]:
                st.session_state.pop(key, None)
            st.rerun()
    else:
        selected_channels = []
        selected_product_lines = []
        selected_categories = []
        selected_series = []
        selected_models = []
        selected_years = []
        selected_weeks = []
        start_date = end_date = None

# =========================
# Empty state
# =========================
if df.empty:
    st.warning("No Sales Agent sellout data found. Please upload and maintain data in the Database module first.")
    st.stop()

# =========================
# Apply filters
# =========================
base_scope = filter_base_scope(
    df,
    channels=selected_channels,
    product_lines=selected_product_lines,
    categories=selected_categories,
    models=selected_models,
)

if selected_series and "series_name" in base_scope.columns:
    base_scope = base_scope[base_scope["series_name"].astype(str).isin(selected_series)]

filtered = filter_period(base_scope, start_date, end_date)
filtered = apply_year_week_filters(filtered, years=selected_years, weeks=selected_weeks)

active_filters = {
    "Retailer / Channel": selected_channels or "All",
    "Product Line": selected_product_lines or "All",
    "Categories": selected_categories or "All",
    "Series": selected_series or "All",
    "Models": selected_models or "All",
    "Years": selected_years or "All",
    "Weeks": selected_weeks or "All",
    "Sales Date Range": f"{start_date} → {end_date}",
    "Shared Context": "Sales Heatmap + Value Chain are automatically included when available",
}

diagnostic_tables = build_diagnostic_tables(base_scope, filtered, start_date, end_date)
meta = summarize_dataset(df)

# =========================
# KPI cards
# =========================
k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Rows", f"{meta['rows']:,}")
k2.metric("Channels", meta["channels"])
k3.metric("Models", meta["models"])
k4.metric("Product Lines", meta["product_lines"])
k5.metric("Categories", meta["categories"])
k6.metric("Series", meta.get("series", 0))

with st.expander("Detected fields and mapping", expanded=False):
    st.write(meta["columns"])

st.markdown("---")
period_info = diagnostic_tables.get("periods", pd.DataFrame())
if not period_info.empty:
    with st.expander("Comparison period definition", expanded=False):
        st.dataframe(period_info, use_container_width=True, hide_index=True)

# =========================
# Full-width AI Summary
# =========================
st.subheader("1) AI Summary")
st.caption(
    "Full-page diagnosis by channel, category and key model. Focus: latest week, WoW / YoY, ASP, turnover, margin risk and next-week actions."
)

btn_col, info_col = st.columns([0.35, 0.65])
with btn_col:
    refresh_summary = st.button("Generate / Refresh AI Summary", use_container_width=True)
with info_col:
    st.caption(
        f"Current filtered rows: {len(filtered):,} | Period: {start_date} → {end_date} | "
        f"OpenAI: {'On' if use_openai else 'Off'} | Shared context: Auto"
    )

summary_signature = str(active_filters) + str(len(filtered)) + str(start_date) + str(end_date)
if refresh_summary or st.session_state.get("sales_agent_summary_signature") != summary_signature:
    with st.spinner("Generating AI sales diagnosis..."):
        st.session_state["sales_agent_summary"] = build_summary(
            filtered,
            diagnostic_tables=diagnostic_tables,
            start_date=start_date,
            end_date=end_date,
            filters=active_filters,
            use_openai=use_openai,
            model=openai_model,
            include_heatmap=True,
            include_value_chain=True,
        )
        st.session_state["sales_agent_summary_signature"] = summary_signature

st.markdown(st.session_state.get("sales_agent_summary", "No summary generated yet."))

# =========================
# Diagnostic tables
# =========================
st.markdown("---")
st.subheader("2) Diagnostic Tables")
st.caption("Verification tables. Sorted by combined WoW / YoY risk and model-level drag impact.")

t1, t2, t3 = st.tabs(["Channel Diagnosis", "Category Diagnosis", "Model Impact"])
with t1:
    st.dataframe(diagnostic_tables["channel"], use_container_width=True, height=360)
with t2:
    st.dataframe(diagnostic_tables["category"], use_container_width=True, height=360)
with t3:
    st.dataframe(diagnostic_tables["model"], use_container_width=True, height=420)

# =========================
# Q&A below
# =========================
st.markdown("---")
st.subheader("3) Free Q&A")
st.caption(
    "Ask any sales question. The AI automatically uses current Sales Agent filters plus available Sales Heatmap and Value Chain database context."
)

question = st.text_area(
    "Ask a question",
    placeholder=(
        "Example: Which channel caused last week's decline? Which category should we recover first? "
        "Which models are risky after considering profitability? Where should field team focus based on heatmap?"
    ),
    height=130,
)

qa_col1, qa_col2 = st.columns([0.25, 0.75])
with qa_col1:
    run_qa = st.button("Run AI analysis", use_container_width=True)
with qa_col2:
    st.caption("Tip: ask in Chinese or English. Data context is shared across Database, Sales Heatmap, Value Chain and Sales Agent.")

if run_qa:
    prompt = question.strip() if question and question.strip() else "Give me a clear diagnosis and next-week sales action plan."
    with st.spinner("Running AI analysis..."):
        st.session_state["sales_agent_answer"] = answer_question(
            filtered,
            prompt,
            diagnostic_tables=diagnostic_tables,
            start_date=start_date,
            end_date=end_date,
            filters=active_filters,
            use_openai=use_openai,
            model=openai_model,
            include_heatmap=True,
            include_value_chain=True,
        )

st.markdown(st.session_state.get("sales_agent_answer", "Ask a question and click **Run AI analysis**."))

# =========================
# Preview
# =========================
st.markdown("---")
with st.expander("Filtered data preview", expanded=False):
    preview_cols = [
        "sales_date", "channel", "product_line", "category", "series_name", "model",
        "price", "sales_qty", "sales_value_est", "sum_avl_soh", "sum_soo",
    ]
    keep_cols = [c for c in preview_cols if c in filtered.columns]
    st.dataframe(filtered[keep_cols].head(500), use_container_width=True, height=420)
