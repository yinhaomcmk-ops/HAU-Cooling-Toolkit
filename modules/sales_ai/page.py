from __future__ import annotations

import json
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
SUMMARY_CACHE_PATH = BASE_DIR / "data" / "sales_agent_summary_cache.json"

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
default_openai_model = get_openai_model()


def _load_summary_cache() -> None:
    """Keep the last generated AI summary after page refresh / rerun."""
    if "sales_agent_summary" in st.session_state:
        return
    try:
        if SUMMARY_CACHE_PATH.exists():
            payload = json.loads(SUMMARY_CACHE_PATH.read_text(encoding="utf-8"))
            st.session_state["sales_agent_summary"] = payload.get("summary", "")
            st.session_state["sales_agent_summary_meta"] = payload.get("meta", "")
    except Exception:
        pass


def _save_summary_cache(summary: str, meta: str) -> None:
    try:
        SUMMARY_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        SUMMARY_CACHE_PATH.write_text(
            json.dumps({"summary": summary, "meta": meta}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass


_load_summary_cache()


# =========================
# AI guidance helpers
# =========================
BUSINESS_RULES = """
Critical business rules for Hisense AU cooling sales analysis:

1. Model matching and data linkage:
- When the user mentions a model, normalize model text by uppercasing and removing spaces/hyphens.
- Match the mentioned model against Sales Agent data, Sales Heatmap data, Product Master, EXW Cost and Landed Cost.
- Use the matched model rows first for trend, store ranking, channel comparison, ASP and action recommendations.
- Do not say there is no sales data unless the matched model rows are actually empty after applying the requested year/week/date filters.
- If exact model is not found, show closest available model names and explain that the answer is based on those matches.

2. Gross margin and promo price calculation:
- Retail price includes GST.
- Invoice ex GST = Retail Price / 1.1.
- Channel rebate must be deducted before calculating gross margin.
- Net Sales = Invoice ex GST × (1 - Channel Rebate).
- Gross Margin = (Net Sales - Landed Cost) / Net Sales.
- Required retail promo price for target gross margin:
  Retail Price = Landed Cost × 1.1 / ((1 - Channel Rebate) × (1 - Target Gross Margin)).
- Never use Landed Cost / (1 - Margin), because that ignores GST and channel rebate.
- If the user says "35% gross margin and 35% channel rebate", use both 35% target GM and 35% rebate.

3. Year/week filtering:
- If the user asks for year/week, filter by year and week before calculating sales_qty, sales_value, ASP, trend, store ranking and worst stores.
- For weeks 13-16, compare the selected weeks week by week and identify the lowest sales / weakest trend.

4. Store ranking:
- For worst-performing stores, rank by sales_qty for the matched model and selected period.
- Separate Harvey Norman and The Good Guys if the user asks for both.
- Do not mix total category/store heatmap volume with the target model sales unless explicitly requested.
"""


def _norm_model_text(value: object) -> str:
    return (
        str(value)
        .upper()
        .replace(" ", "")
        .replace("-", "")
        .replace("_", "")
        .strip()
    )


def _find_models_in_question(question_text: str, sales_data: pd.DataFrame, master_data: pd.DataFrame) -> list[str]:
    q_norm = _norm_model_text(question_text)
    candidates: list[str] = []

    if sales_data is not None and not sales_data.empty and "model" in sales_data.columns:
        candidates.extend(sales_data["model"].dropna().astype(str).unique().tolist())

    if master_data is not None and not master_data.empty:
        for c in ["hau_model", "model", "model_id"]:
            if c in master_data.columns:
                candidates.extend(master_data[c].dropna().astype(str).unique().tolist())

    seen = set()
    matched = []
    for model in candidates:
        model_norm = _norm_model_text(model)
        if not model_norm or model_norm in seen:
            continue
        seen.add(model_norm)
        if model_norm in q_norm:
            matched.append(str(model))
    return matched


def _apply_model_scope_from_question(data: pd.DataFrame, matched_models: list[str]) -> pd.DataFrame:
    if data is None or data.empty or not matched_models or "model" not in data.columns:
        return data
    matched_norm = {_norm_model_text(m) for m in matched_models}
    scoped = data.copy()
    scoped["_model_norm"] = scoped["model"].map(_norm_model_text)
    scoped = scoped[scoped["_model_norm"].isin(matched_norm)].drop(columns=["_model_norm"])
    return scoped


def _build_qa_prompt(user_prompt: str, matched_models: list[str], qa_df: pd.DataFrame) -> str:
    if matched_models:
        model_note = f"Matched target model(s): {', '.join(sorted(set(map(str, matched_models))))}."
    else:
        model_note = "No exact model mention was auto-detected; use the currently filtered dataset."

    if qa_df is not None and not qa_df.empty:
        quick_context = []
        if "model" in qa_df.columns:
            quick_context.append("Rows by model:\\n" + qa_df.groupby("model", dropna=False).size().sort_values(ascending=False).head(20).to_string())
        if "channel" in qa_df.columns and "sales_qty" in qa_df.columns:
            channel_sales = qa_df.groupby("channel", dropna=False)["sales_qty"].sum().sort_values(ascending=True).head(20)
            quick_context.append("Lowest channel sales_qty in scoped data:\\n" + channel_sales.to_string())
        if all(c in qa_df.columns for c in ["week", "sales_qty"]):
            week_sales = qa_df.groupby("week", dropna=False)["sales_qty"].sum().sort_index()
            quick_context.append("Sales_qty by week in scoped data:\\n" + week_sales.to_string())
        if all(c in qa_df.columns for c in ["channel", "model", "sales_qty"]):
            quick_context.append(f"Scoped rows available: {len(qa_df):,}")
        scoped_context = "\\n\\n".join(quick_context)
    else:
        scoped_context = "Scoped data is empty after applying current filters and detected model matching."

    return f"""
{BUSINESS_RULES}

Current auto-detected scope:
{model_note}

Scoped data quick check:
{scoped_context}

User question:
{user_prompt}
""".strip()


def _safe_build_diagnostic_tables(base_scope: pd.DataFrame, filtered_scope: pd.DataFrame, start_date, end_date) -> dict:
    """Fallback when LY comparison columns are unavailable for narrow model/week scopes."""

    current = filtered_scope.copy() if filtered_scope is not None else pd.DataFrame()

    def _summary(group_cols: list[str]) -> pd.DataFrame:
        if current.empty:
            return pd.DataFrame(columns=group_cols + ["rows", "sales_qty", "sales_value_est", "avg_price"])
        existing_groups = [c for c in group_cols if c in current.columns]
        if not existing_groups:
            return pd.DataFrame(columns=group_cols + ["rows", "sales_qty", "sales_value_est", "avg_price"])

        temp = current.copy()
        if "sales_qty" not in temp.columns:
            temp["sales_qty"] = 0
        if "sales_value_est" not in temp.columns:
            if "sales_value" in temp.columns:
                temp["sales_value_est"] = temp["sales_value"]
            elif "price" in temp.columns:
                temp["sales_value_est"] = pd.to_numeric(temp["sales_qty"], errors="coerce").fillna(0) * pd.to_numeric(temp["price"], errors="coerce").fillna(0)
            else:
                temp["sales_value_est"] = 0
        if "price" not in temp.columns:
            temp["price"] = 0

        return (
            temp.groupby(existing_groups, dropna=False)
            .agg(
                rows=(existing_groups[0], "size"),
                sales_qty=("sales_qty", "sum"),
                sales_value_est=("sales_value_est", "sum"),
                avg_price=("price", "mean"),
            )
            .reset_index()
            .sort_values("sales_qty", ascending=True)
        )

    return {
        "periods": pd.DataFrame([{"scope": "current", "start_date": start_date, "end_date": end_date}]),
        "channel": _summary(["channel"]),
        "category": _summary(["category"]),
        "model": _summary(["model"]),
    }


# =========================
# Sidebar: AI settings + filters
# =========================
with st.sidebar:
    st.markdown("### 🤖 AI Settings")

    if not ai_ready:
        st.info("Add OPENAI_API_KEY to .streamlit/secrets.toml to enable OpenAI analysis.")

    use_openai = st.toggle(
        "Enable OpenAI",
        value=bool(st.session_state.get("sales_agent_use_openai", ai_ready)),
        key="sales_agent_use_openai",
    )

    model_options = ["gpt-4o-mini", "gpt-4o", "gpt-4.1-mini", "gpt-4.1"]
    if default_openai_model not in model_options:
        model_options.insert(0, default_openai_model)

    default_model = st.session_state.get("sales_agent_openai_model", default_openai_model)
    model_index = model_options.index(default_model) if default_model in model_options else 0
    openai_model = st.selectbox(
        "Model",
        model_options,
        index=model_index,
        key="sales_agent_openai_model",
    )

    st.markdown("---")
    st.markdown("### Filters")

    if not df.empty:
        channels = sorted(df["channel"].dropna().astype(str).unique().tolist())
        years = sorted(df["year"].dropna().astype(int).unique().tolist())
        weeks = sorted(df["week"].dropna().astype(int).unique().tolist())
        series_names = sorted(df["series_name"].dropna().astype(str).unique().tolist()) if "series_name" in df.columns else []

        selected_channels = st.multiselect("Retailer / Channel", channels, default=[])

        # Product Line -> Category -> Model cascade
        product_lines = sorted(df["product_line"].dropna().astype(str).unique().tolist()) if "product_line" in df.columns else []
        selected_product_lines = st.multiselect("Product Line", product_lines, default=[])

        category_scope = df.copy()
        if selected_product_lines and "product_line" in category_scope.columns:
            category_scope = category_scope[category_scope["product_line"].astype(str).isin(selected_product_lines)]
        categories = sorted(category_scope["category"].dropna().astype(str).unique().tolist()) if "category" in category_scope.columns else []
        selected_categories = st.multiselect("Category", categories, default=[])

        model_scope = category_scope.copy()
        if selected_categories and "category" in model_scope.columns:
            model_scope = model_scope[model_scope["category"].astype(str).isin(selected_categories)]
        models = sorted(model_scope["model"].dropna().astype(str).unique().tolist()) if "model" in model_scope.columns else []
        selected_models = st.multiselect("Model", models, default=[])

        selected_series = st.multiselect("Series", series_names, default=[])

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
            st.session_state.pop("sales_agent_answer", None)
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

summary_meta = (
    f"Rows: {len(filtered):,} | Period: {start_date} → {end_date} | "
    f"OpenAI: {'On' if use_openai else 'Off'} | Model: {openai_model}"
)

diagnostic_tables = _safe_build_diagnostic_tables(base_scope, filtered, start_date, end_date)
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

        
from services.sales_data_loader import read_sales_by_stores_records, read_store_master_records

store_sales = read_sales_by_stores_records()
store_master = read_store_master_records()

store_sales["model_norm"] = store_sales["model"].astype(str).str.upper().str.replace(" ", "", regex=False)
target_model_norm = "HRCD615TBWV"

model_store_sales = store_sales[
    store_sales["model_norm"] == target_model_norm
].copy()

model_store_sales = model_store_sales.merge(
    store_master[["business_name", "retailer", "region"]],
    on="business_name",
    how="left"
)

model_store_sales = model_store_sales[
    model_store_sales["retailer"].str.contains("Harvey Norman|The Good Guys", case=False, na=False)
]

store_rank = (
    model_store_sales
    .groupby(["retailer", "business_name"], as_index=False)["sales"]
    .sum()
    .sort_values(["retailer", "sales"], ascending=[True, True])
)
# =========================
# Q&A first
# =========================
st.subheader("1) GPT Q&A")
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
    user_prompt = question.strip() if question and question.strip() else "Give me a clear diagnosis and next-week sales action plan."

    matched_models = _find_models_in_question(user_prompt, df, model_master)
    qa_base_scope = _apply_model_scope_from_question(base_scope, matched_models)
    qa_filtered = _apply_model_scope_from_question(filtered, matched_models)

    if matched_models and qa_filtered.empty:
        st.warning(
            "Matched model in your question, but no rows were found after current filters. "
            "Please check Year / Week / Date filters or the uploaded sales model naming."
        )

    qa_diagnostic_tables = _safe_build_diagnostic_tables(qa_base_scope, qa_filtered, start_date, end_date)
    qa_prompt = _build_qa_prompt(user_prompt, matched_models, qa_filtered)

    with st.spinner("Running AI analysis..."):
        st.session_state["sales_agent_answer"] = answer_question(
            qa_filtered,
            qa_prompt,
            diagnostic_tables=qa_diagnostic_tables,
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
# AI Summary second, manually refreshed + persistent cache
# =========================
st.markdown("---")
st.subheader("2) AI Summary")
st.caption(
    "Full-page diagnosis by channel, category and key model. Focus: latest week, WoW / YoY, ASP, turnover, margin risk and next-week actions."
)

btn_col, info_col = st.columns([0.35, 0.65])
with btn_col:
    refresh_summary = st.button("Generate / Refresh AI Summary", use_container_width=True)
with info_col:
    st.caption(summary_meta)
    if st.session_state.get("sales_agent_summary_meta"):
        st.caption(f"Cached summary: {st.session_state['sales_agent_summary_meta']}")

if refresh_summary:
    with st.spinner("Generating AI sales diagnosis..."):
        summary_text = build_summary(
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
        st.session_state["sales_agent_summary"] = summary_text
        st.session_state["sales_agent_summary_meta"] = summary_meta
        _save_summary_cache(summary_text, summary_meta)

st.markdown(st.session_state.get("sales_agent_summary", "No summary generated yet."))

# =========================
# Diagnostic tables
# =========================
st.markdown("---")
st.subheader("3) Diagnostic Tables")
st.caption("Verification tables. Sorted by combined WoW / YoY risk and model-level drag impact.")

t1, t2, t3 = st.tabs(["Channel Diagnosis", "Category Diagnosis", "Model Impact"])
with t1:
    st.dataframe(diagnostic_tables["channel"], use_container_width=True, height=360)
with t2:
    st.dataframe(diagnostic_tables["category"], use_container_width=True, height=360)
with t3:
    st.dataframe(diagnostic_tables["model"], use_container_width=True, height=420)

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
