from __future__ import annotations

import io
from datetime import date

import pandas as pd
import streamlit as st

from services.sales_data_loader import (
    init_all_shared_db,
    load_sales_agent_data,
    read_model_master_records,
)
from services.sales_ai_engine import (
    answer_question,
    get_openai_model,
    is_openai_ready,
)

st.title("Sales Dashboard")
st.caption("PBI-style sellout dashboard rebuilt from the Sales Agent data in the shared database.")

init_all_shared_db()


# -----------------------------
# Basic helpers
# -----------------------------
def _to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Sales Dashboard") -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    return buffer.getvalue()


def _fmt_k(value: float | int | None, prefix: str = "") -> str:
    if pd.isna(value):
        return "-"
    value = float(value)
    sign = "-" if value < 0 else ""
    value_abs = abs(value)
    if value_abs >= 1_000_000:
        return f"{sign}{prefix}{value_abs / 1_000_000:.1f}M"
    if value_abs >= 1_000:
        return f"{sign}{prefix}{value_abs / 1_000:.0f}K"
    return f"{sign}{prefix}{value_abs:,.0f}"


def _fmt_pct(value: float | int | None) -> str:
    if pd.isna(value):
        return "-"
    return f"{float(value):+.1%}"


def _select_options(series: pd.Series) -> list[str]:
    return sorted([x for x in series.dropna().astype(str).unique().tolist() if x and x.lower() != "nan"])


def _normalise_model(value: object) -> str:
    return str(value).upper().replace(" ", "").replace("-", "").replace("_", "").strip()


def _safe_numeric(df: pd.DataFrame, col: str, default: float = 0) -> pd.Series:
    if col not in df.columns:
        return pd.Series(default, index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce").fillna(default)


# -----------------------------
# Load and enrich data
# -----------------------------
@st.cache_data(show_spinner=False)
def _load_data() -> pd.DataFrame:
    data = load_sales_agent_data()
    if data is None or data.empty:
        return pd.DataFrame()

    df = data.copy()
    df.columns = [str(c).strip() for c in df.columns]

    if "sales_date" in df.columns:
        df["sales_date"] = pd.to_datetime(df["sales_date"], errors="coerce")
    else:
        df["sales_date"] = pd.NaT

    df = df.dropna(subset=["sales_date"])
    df["year"] = df["sales_date"].dt.isocalendar().year.astype(int)
    df["week"] = df["sales_date"].dt.isocalendar().week.astype(int)
    df["month"] = df["sales_date"].dt.to_period("M").astype(str)

    if "channel" not in df.columns:
        df["channel"] = "Unknown"
    if "model" not in df.columns:
        df["model"] = "Unknown"

    df["channel"] = df["channel"].fillna("Unknown").astype(str).str.strip()
    df["model"] = df["model"].fillna("Unknown").astype(str).str.strip().str.upper()
    df["model_norm"] = df["model"].map(_normalise_model)

    df["sales_qty"] = _safe_numeric(df, "sales_qty")
    df["price"] = _safe_numeric(df, "price")
    df["sum_avl_soh"] = _safe_numeric(df, "sum_avl_soh")
    df["sum_soo"] = _safe_numeric(df, "sum_soo")

    if "sales_value_est" in df.columns:
        df["sales_value"] = _safe_numeric(df, "sales_value_est")
    elif "daily_sales_amt" in df.columns:
        df["sales_value"] = _safe_numeric(df, "daily_sales_amt")
    else:
        df["sales_value"] = df["sales_qty"] * df["price"]

    # Enrich from product master when loader has not already added these fields.
    for col in ["product_line", "category", "series_name"]:
        if col not in df.columns:
            df[col] = ""

    try:
        master = read_model_master_records()
    except Exception:
        master = pd.DataFrame()

    if master is not None and not master.empty:
        m = master.copy()
        m.columns = [str(c).strip() for c in m.columns]
        model_col = None
        for c in ["model", "hau_model", "model_id", "hq_model"]:
            if c in m.columns:
                model_col = c
                break
        if model_col:
            m["model_norm"] = m[model_col].map(_normalise_model)
            rename_map = {}
            if "series" in m.columns and "series_name" not in m.columns:
                rename_map["series"] = "series_name"
            m = m.rename(columns=rename_map)
            keep = [c for c in ["model_norm", "product_line", "category", "series_name"] if c in m.columns]
            m = m[keep].drop_duplicates("model_norm", keep="last")
            df = df.merge(m, on="model_norm", how="left", suffixes=("", "_master"))
            for col in ["product_line", "category", "series_name"]:
                master_col = f"{col}_master"
                if master_col in df.columns:
                    df[col] = df[col].replace("", pd.NA).fillna(df[master_col]).fillna("")
                    df = df.drop(columns=[master_col])

    # If no product master mapping is available, infer a rough category from model text.
    if df["product_line"].fillna("").eq("").all():
        df["product_line"] = "Refrigerator"
    if df["category"].fillna("").eq("").all():
        def infer_category(model: str) -> str:
            m = str(model).upper()
            if "HRVF" in m or "HRCF" in m:
                return "Freezer"
            if "HRWC" in m:
                return "Wine Cabinet"
            if "HRTF" in m:
                return "Top Mount"
            if "HRBM" in m or "HRBF" in m:
                return "Bottom Mount"
            if "HRCD" in m:
                return "French Door"
            if "HRSBS" in m:
                return "Side by Side"
            return "Other"
        df["category"] = df["model"].map(infer_category)

    df["product_line"] = df["product_line"].fillna("Other").replace("", "Other").astype(str)
    df["category"] = df["category"].fillna("Other").replace("", "Other").astype(str)
    df["series_name"] = df["series_name"].fillna("Other").replace("", "Other").astype(str)
    return df


def _period_years(df: pd.DataFrame, selected_years: list[int]) -> tuple[int, int, int]:
    years = selected_years or sorted(df["year"].dropna().astype(int).unique().tolist())
    ty = max(years)
    return ty, ty - 1, ty - 2


def _filter_scope(df: pd.DataFrame) -> tuple[pd.DataFrame, dict, tuple[int, int, int]]:
    if df.empty:
        return df, {}, (0, 0, 0)

    with st.sidebar:
        st.markdown("### Dashboard Filters")

        channels = st.multiselect("Retailer", _select_options(df["channel"]), default=[])
        price_min, price_max = int(max(0, df["price"].min())), int(max(df["price"].max(), 1))
        threshold = st.number_input("Price threshold", min_value=0, max_value=max(price_max, 10000), value=500, step=50)
        price_scope_only = st.checkbox("Only show models above threshold", value=False)

        product_lines = st.multiselect("Product Line", _select_options(df["product_line"]), default=[])
        category_base = df[df["product_line"].isin(product_lines)] if product_lines else df
        categories = st.multiselect("Category", _select_options(category_base["category"]), default=[])

        model_base = category_base[category_base["category"].isin(categories)] if categories else category_base
        models = st.multiselect("Model", _select_options(model_base["model"]), default=[])

        available_years = sorted(df["year"].dropna().astype(int).unique().tolist())
        years = st.multiselect("Year", available_years, default=[max(available_years)] if available_years else [])
        ty, ly, tya = _period_years(df, years)

        current_year_scope = df[df["year"] == ty]
        weeks_available = sorted(current_year_scope["week"].dropna().astype(int).unique().tolist())
        if weeks_available:
            week_min_default, week_max_default = min(weeks_available), max(weeks_available)
            week_range = st.slider(
                "Week",
                min_value=1,
                max_value=53,
                value=(week_min_default, week_max_default),
                step=1,
            )
        else:
            week_range = (1, 53)

        if st.button("Reset dashboard filters", use_container_width=True):
            for key in list(st.session_state.keys()):
                if key.startswith("sales_dashboard"):
                    st.session_state.pop(key, None)
            st.rerun()

    out = df.copy()
    if channels:
        out = out[out["channel"].isin(channels)]
    if product_lines:
        out = out[out["product_line"].isin(product_lines)]
    if categories:
        out = out[out["category"].isin(categories)]
    if models:
        out = out[out["model"].isin(models)]
    if price_scope_only:
        out = out[out["price"] >= threshold]

    out = out[out["week"].between(week_range[0], week_range[1])]
    # keep TY/LY/TYA for comparison even when only TY is selected
    out = out[out["year"].isin([ty, ly, tya])]

    filters = {
        "Retailer": channels or "All",
        "Product Line": product_lines or "All",
        "Category": categories or "All",
        "Model": models or "All",
        "Price Threshold": threshold,
        "Only above threshold": price_scope_only,
        "TY/LY/TYA": f"{ty}/{ly}/{tya}",
        "Week Range": f"{week_range[0]}–{week_range[1]}",
    }
    return out, filters, (ty, ly, tya)


# -----------------------------
# Aggregation
# -----------------------------
def _year_agg(df: pd.DataFrame, group_cols: list[str], years: tuple[int, int, int]) -> pd.DataFrame:
    ty, ly, tya = years
    if df.empty:
        return pd.DataFrame(columns=group_cols)

    temp = df.copy()
    temp["asp_x_qty"] = temp["price"] * temp["sales_qty"]

    g = (
        temp.groupby(group_cols + ["year"], dropna=False)
        .agg(
            amount=("sales_value", "sum"),
            qty=("sales_qty", "sum"),
            asp_num=("asp_x_qty", "sum"),
            soh=("sum_avl_soh", "sum"),
            soo=("sum_soo", "sum"),
            rows=("model", "size"),
        )
        .reset_index()
    )
    g["asp"] = g["asp_num"] / g["qty"].replace(0, pd.NA)
    g["wos"] = g["soh"] / g["qty"].replace(0, pd.NA)

    def pivot_metric(metric: str, label: str) -> pd.DataFrame:
        p = g.pivot_table(index=group_cols, columns="year", values=metric, aggfunc="sum")
        for y in [ty, ly, tya]:
            if y not in p.columns:
                p[y] = 0
        return p[[ty, ly, tya]].rename(columns={ty: f"{label}-TY", ly: f"{label}-LY", tya: f"{label}-TYA"})

    amount = pivot_metric("amount", "AMT")
    qty = pivot_metric("qty", "QTY")
    soh = pivot_metric("soh", "SOH")
    soo = pivot_metric("soo", "SOO")

    # ASP and WOS should be recomputed, not summed from group rows.
    asp = g.pivot_table(index=group_cols, columns="year", values="asp", aggfunc="mean")
    wos = g.pivot_table(index=group_cols, columns="year", values="wos", aggfunc="mean")
    for p in [asp, wos]:
        for y in [ty, ly, tya]:
            if y not in p.columns:
                p[y] = pd.NA
    asp = asp[[ty, ly, tya]].rename(columns={ty: "ASP-TY", ly: "ASP-LY", tya: "ASP-TYA"})
    wos = wos[[ty, ly, tya]].rename(columns={ty: "WOS-TY", ly: "WOS-LY", tya: "WOS-TYA"})

    out = pd.concat([amount, qty, asp, soh, wos, soo], axis=1).reset_index()
    out["AMT-DIFF"] = out["AMT-TY"] - out["AMT-LY"]
    out["AMT-YOY"] = out["AMT-DIFF"] / out["AMT-LY"].replace(0, pd.NA)
    out["QTY-DIFF"] = out["QTY-TY"] - out["QTY-LY"]
    out["QTY-YOY"] = out["QTY-DIFF"] / out["QTY-LY"].replace(0, pd.NA)
    out["ASP-YOY"] = (out["ASP-TY"] - out["ASP-LY"]) / out["ASP-LY"].replace(0, pd.NA)
    out["WOS-YOY"] = out["WOS-TY"] - out["WOS-LY"]
    out["SOO-YOY"] = (out["SOO-TY"] - out["SOO-LY"]) / out["SOO-LY"].replace(0, pd.NA)
    return out.sort_values("AMT-TY", ascending=False)


def _total_row(table: pd.DataFrame, label_col: str, label: str = "Total") -> pd.DataFrame:
    if table.empty or label_col not in table.columns:
        return table
    numeric_cols = table.select_dtypes(include="number").columns.tolist()
    total = {label_col: label}
    for c in numeric_cols:
        if c.endswith("YOY") or c.endswith("%-YOY"):
            continue
        total[c] = table[c].sum(skipna=True)
    if "AMT-LY" in total and total.get("AMT-LY", 0):
        total["AMT-YOY"] = (total.get("AMT-TY", 0) - total.get("AMT-LY", 0)) / total.get("AMT-LY", 0)
    if "QTY-LY" in total and total.get("QTY-LY", 0):
        total["QTY-YOY"] = (total.get("QTY-TY", 0) - total.get("QTY-LY", 0)) / total.get("QTY-LY", 0)
    if "ASP-LY" in table.columns:
        total["ASP-TY"] = total.get("AMT-TY", 0) / total.get("QTY-TY", pd.NA) if total.get("QTY-TY", 0) else pd.NA
        total["ASP-LY"] = total.get("AMT-LY", 0) / total.get("QTY-LY", pd.NA) if total.get("QTY-LY", 0) else pd.NA
        total["ASP-TYA"] = total.get("AMT-TYA", 0) / total.get("QTY-TYA", pd.NA) if total.get("QTY-TYA", 0) else pd.NA
        total["ASP-YOY"] = (total["ASP-TY"] - total["ASP-LY"]) / total["ASP-LY"] if total.get("ASP-LY", 0) else pd.NA
    return pd.concat([table, pd.DataFrame([total])], ignore_index=True)


def _style_summary(df: pd.DataFrame):
    if df.empty:
        return df

    def color_yoy(v):
        if pd.isna(v):
            return ""
        if v > 0:
            return "color:#FF8A3D; font-weight:800;"
        if v < 0:
            return "color:#38A8FF; font-weight:800;"
        return "color:#E5EEF5;"

    format_map = {}
    for c in df.columns:
        if c.startswith("AMT") or c.startswith("SOH") or c.startswith("SOO"):
            format_map[c] = "{:,.0f}"
        elif c.startswith("QTY"):
            format_map[c] = "{:,.0f}"
        elif c.startswith("ASP"):
            format_map[c] = "${:,.0f}"
        elif c.startswith("WOS"):
            format_map[c] = "{:,.2f}"
        elif c.endswith("YOY"):
            format_map[c] = "{:+.1%}"

    styler = df.style.format(format_map, na_rep="-")
    yoy_cols = [c for c in df.columns if c.endswith("YOY") or c.endswith("DIFF")]
    if hasattr(styler, "map"):
        return styler.map(color_yoy, subset=yoy_cols)
    return styler.applymap(color_yoy, subset=yoy_cols)


def _render_metric_card(label: str, value: str, delta: str | None = None):
    st.metric(label, value, delta=delta)


def _render_dashboard(df: pd.DataFrame, years: tuple[int, int, int], filters: dict) -> None:
    ty, ly, tya = years
    ty_df = df[df["year"] == ty].copy()
    ly_df = df[df["year"] == ly].copy()

    amt_ty = ty_df["sales_value"].sum()
    amt_ly = ly_df["sales_value"].sum()
    qty_ty = ty_df["sales_qty"].sum()
    qty_ly = ly_df["sales_qty"].sum()
    asp_ty = amt_ty / qty_ty if qty_ty else pd.NA
    asp_ly = amt_ly / qty_ly if qty_ly else pd.NA
    wos_ty = ty_df["sum_avl_soh"].sum() / qty_ty if qty_ty else pd.NA
    wos_ly = ly_df["sum_avl_soh"].sum() / qty_ly if qty_ly else pd.NA

    st.markdown(
        """
        <style>
        div[data-testid="stMetric"] {background:rgba(255,255,255,.045); border:1px solid rgba(255,255,255,.09); padding:12px 14px; border-radius:14px;}
        </style>
        """,
        unsafe_allow_html=True,
    )

    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        _render_metric_card("AMT-TY", _fmt_k(amt_ty, "$"), _fmt_pct((amt_ty - amt_ly) / amt_ly) if amt_ly else None)
    with k2:
        _render_metric_card("QTY-TY", _fmt_k(qty_ty), _fmt_pct((qty_ty - qty_ly) / qty_ly) if qty_ly else None)
    with k3:
        _render_metric_card("ASP-TY", _fmt_k(asp_ty, "$"), _fmt_pct((asp_ty - asp_ly) / asp_ly) if pd.notna(asp_ly) and asp_ly else None)
    with k4:
        _render_metric_card("WOS-TY", f"{wos_ty:,.2f}" if pd.notna(wos_ty) else "-", f"{wos_ty - wos_ly:+.2f}" if pd.notna(wos_ly) else None)
    with k5:
        _render_metric_card("Models", f"{ty_df['model'].nunique():,}", f"Retailers {ty_df['channel'].nunique():,}")

    st.caption("Current scope: " + " | ".join([f"{k}: {v}" for k, v in filters.items()]))

    st.markdown("---")
    c1, c2, c3 = st.columns([1.1, 1.4, 1.1])

    with c1:
        st.subheader("Category Share")
        category_share = ty_df.groupby("category", dropna=False)["sales_value"].sum().sort_values(ascending=False).head(12)
        if category_share.empty:
            st.info("No category data.")
        else:
            st.bar_chart(category_share, height=280, use_container_width=True)

    with c2:
        st.subheader("$500+ Proportion")
        threshold = float(filters.get("Price Threshold", 500))
        prop = (
            df.assign(price_band=lambda x: x["price"].ge(threshold).map({True: f"${int(threshold)}+", False: f"Below ${int(threshold)}"}))
            .groupby(["year", "price_band"], dropna=False)["sales_value"]
            .sum()
            .reset_index()
        )
        prop = prop[prop["year"].isin([ty, ly, tya])]
        if prop.empty:
            st.info("No price band data.")
        else:
            st.bar_chart(prop, x="year", y="sales_value", color="price_band", height=280, use_container_width=True)

    with c3:
        st.subheader("Efficiency")
        eff = (
            df.groupby(["year", "channel"], dropna=False)
            .agg(qty=("sales_qty", "sum"), soh=("sum_avl_soh", "sum"))
            .reset_index()
        )
        eff["turnover"] = eff["qty"] / eff["soh"].replace(0, pd.NA)
        eff = eff[eff["year"] == ty].sort_values("turnover", ascending=False).head(8)
        if eff.empty:
            st.info("No efficiency data.")
        else:
            st.bar_chart(eff.set_index("channel")["turnover"], height=280, use_container_width=True)

    c4, c5 = st.columns([1.2, 1])
    with c4:
        st.subheader("Sales Trend")
        trend = (
            df.groupby(["year", "week"], dropna=False)["sales_value"]
            .sum()
            .reset_index()
        )
        trend["year"] = trend["year"].astype(str)
        if trend.empty:
            st.info("No trend data.")
        else:
            st.line_chart(trend, x="week", y="sales_value", color="year", height=300, use_container_width=True)

    with c5:
        st.subheader("Growth & Decline")
        model_summary = _year_agg(df, ["model"], years)
        if model_summary.empty:
            st.info("No model comparison data.")
        else:
            model_summary["Impact"] = model_summary["AMT-DIFF"]
            growth_decline = model_summary.sort_values("Impact", ascending=True).head(6)
            growth_decline = pd.concat([growth_decline, model_summary.sort_values("Impact", ascending=False).head(6)])
            growth_decline = growth_decline.drop_duplicates("model")
            st.bar_chart(growth_decline.set_index("model")["Impact"], height=300, use_container_width=True)

    st.markdown("---")
    t1, t2, t3, t4 = st.tabs(["Product Line", "Category", "Retailer", "Model"])

    with t1:
        table = _total_row(_year_agg(df, ["product_line"], years), "product_line")
        st.dataframe(_style_summary(table), use_container_width=True, height=360, hide_index=True)
    with t2:
        table = _total_row(_year_agg(df, ["category"], years), "category")
        st.dataframe(_style_summary(table), use_container_width=True, height=420, hide_index=True)
    with t3:
        table = _total_row(_year_agg(df, ["channel"], years), "channel")
        st.dataframe(_style_summary(table), use_container_width=True, height=360, hide_index=True)
    with t4:
        table = _year_agg(df, ["model"], years)
        st.dataframe(_style_summary(table), use_container_width=True, height=520, hide_index=True)

    st.markdown("---")
    d1, d2 = st.columns([0.25, 0.75])
    with d1:
        st.download_button(
            "Download filtered data",
            data=_to_excel_bytes(df.drop(columns=["model_norm"], errors="ignore"), "Filtered Sales"),
            file_name="sales_dashboard_filtered.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    with d2:
        st.caption("AMT is estimated from Sales Agent value fields; ASP = AMT / QTY; WOS = SOH / QTY. TY/LY/TYA are based on ISO year and selected week range.")



# -----------------------------
# AI assistant beside dashboard
# -----------------------------
def _build_ai_tables(df: pd.DataFrame, years: tuple[int, int, int]) -> dict:
    if df.empty:
        return {}
    tables = {}
    for name, cols in {
        "retailer": ["channel"],
        "category": ["category"],
        "product_line": ["product_line"],
        "model": ["model"],
    }.items():
        try:
            tables[name] = _year_agg(df, cols, years).head(80)
        except Exception:
            tables[name] = pd.DataFrame()
    return tables


def _build_ai_prompt(question: str, df: pd.DataFrame, filters: dict, years: tuple[int, int, int]) -> str:
    ty, ly, tya = years
    ty_df = df[df["year"] == ty]
    ly_df = df[df["year"] == ly]

    amt_ty = ty_df["sales_value"].sum()
    amt_ly = ly_df["sales_value"].sum()
    qty_ty = ty_df["sales_qty"].sum()
    qty_ly = ly_df["sales_qty"].sum()
    asp_ty = amt_ty / qty_ty if qty_ty else pd.NA
    asp_ly = amt_ly / qty_ly if qty_ly else pd.NA

    top_decline = pd.DataFrame()
    try:
        model_table = _year_agg(df, ["model"], years)
        if not model_table.empty:
            model_table["AMT-IMPACT"] = model_table["AMT-TY"] - model_table["AMT-LY"]
            top_decline = model_table.sort_values("AMT-IMPACT").head(10)
    except Exception:
        pass

    filters_text = " | ".join([f"{k}: {v}" for k, v in filters.items()])
    decline_text = top_decline.to_string(index=False) if not top_decline.empty else "No model impact table available."

    return f"""
You are a Hisense AU Cooling sales analyst. Answer in Chinese unless the user asks English.
Use the currently visible dashboard data and filters only.

Current filters:
{filters_text}

Current comparison years:
TY={ty}, LY={ly}, TYA={tya}

Current KPI snapshot:
- AMT TY: {amt_ty:,.0f}
- AMT LY: {amt_ly:,.0f}
- AMT YoY: {((amt_ty - amt_ly) / amt_ly) if amt_ly else float('nan'):.1%}
- QTY TY: {qty_ty:,.0f}
- QTY LY: {qty_ly:,.0f}
- QTY YoY: {((qty_ty - qty_ly) / qty_ly) if qty_ly else float('nan'):.1%}
- ASP TY: {asp_ty:,.0f}
- ASP LY: {asp_ly:,.0f}

Top model growth / decline impact table:
{decline_text}

User question:
{question}

Output format:
1. 直接结论
2. 数据依据
3. 建议动作
""".strip()


def _render_ai_chatbox(df: pd.DataFrame, years: tuple[int, int, int], filters: dict) -> None:
    """Bottom-right AI agent without wrapping the whole Streamlit page.

    Important:
    Do NOT use `div[data-testid="stVerticalBlock"]:has(...)` here.
    In Streamlit it can match the parent page block and force the whole dashboard
    into the floating card.
    """

    st.markdown(
        """
        <style>
        .block-container {padding-bottom: 10rem !important;}

        .sales-ai-spacer {
            height: 0 !important;
            margin: 0 !important;
            padding: 0 !important;
        }

        /* Only float the immediate element that follows our marker. */
        .st-key-sales_ai_float_card {
            position: fixed !important;
            right: 1.25rem !important;
            bottom: 1.15rem !important;
            width: min(440px, calc(100vw - 2rem)) !important;
            max-height: 58vh !important;
            overflow: auto !important;
            z-index: 999999 !important;
            padding: 14px 16px 12px 16px !important;
            border-radius: 18px !important;
            background: rgba(7, 10, 18, 0.76) !important;
            border: 1.5px solid rgba(98, 222, 231, 0.82) !important;
            box-shadow: 0 0 0 1px rgba(255,255,255,.08) inset, 0 14px 42px rgba(0,0,0,.48) !important;
            backdrop-filter: blur(14px) saturate(120%) !important;
        }

        .st-key-sales_ai_float_launcher {
            position: fixed !important;
            right: 1.25rem !important;
            bottom: 1.15rem !important;
            z-index: 999999 !important;
            width: 172px !important;
            padding: 8px 10px !important;
            border-radius: 999px !important;
            background: rgba(7, 10, 18, 0.76) !important;
            border: 1.5px solid rgba(98, 222, 231, 0.82) !important;
            box-shadow: 0 12px 32px rgba(0,0,0,.44) !important;
            backdrop-filter: blur(14px) !important;
        }

        .st-key-sales_ai_float_card [data-testid="stMarkdownContainer"] p {
            margin: 0 !important;
        }

        .st-key-sales_ai_float_card h3,
        .st-key-sales_ai_float_card label,
        .st-key-sales_ai_float_card p,
        .st-key-sales_ai_float_card span,
        .st-key-sales_ai_float_launcher label,
        .st-key-sales_ai_float_launcher span {
            color: #E8F3F7 !important;
        }

        .st-key-sales_ai_float_card div[data-testid="stTextInput"] input {
            height: 34px !important;
            border-radius: 999px !important;
            background: rgba(255,255,255,.06) !important;
            border: 1px solid rgba(185, 206, 220, .72) !important;
            color: #F4FAFF !important;
            padding-left: 14px !important;
            padding-right: 42px !important;
            font-size: 0.88rem !important;
        }

        .st-key-sales_ai_float_card div[data-testid="stTextInput"] input::placeholder {
            color: rgba(230, 242, 250, .54) !important;
        }

        .st-key-sales_ai_float_card div[data-testid="stForm"] {
            border: 0 !important;
            padding: 0 !important;
            background: transparent !important;
        }

        .st-key-sales_ai_float_card button,
        .st-key-sales_ai_float_launcher button {
            border-radius: 999px !important;
            border: 1px solid rgba(92, 205, 215, .65) !important;
            background: rgba(14, 29, 39, .72) !important;
            color: #E8F7FB !important;
            min-height: 34px !important;
        }
        .st-key-sales_ai_float_card button:hover,
        .st-key-sales_ai_float_launcher button:hover {
            border-color: rgba(114, 235, 244, 1) !important;
            background: rgba(24, 53, 67, .92) !important;
        }

        .st-key-sales_ai_float_card div[data-testid="stExpander"] {
            border: 1px solid rgba(255,255,255,.10) !important;
            border-radius: 12px !important;
            background: rgba(255,255,255,.035) !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if "sales_dashboard_chat_history" not in st.session_state:
        st.session_state["sales_dashboard_chat_history"] = []
    if "sales_dashboard_ai_visible" not in st.session_state:
        st.session_state["sales_dashboard_ai_visible"] = True

    ai_ready = is_openai_ready()
    default_model = get_openai_model()
    model_options = ["gpt-4o-mini", "gpt-4o", "gpt-4.1-mini", "gpt-4.1"]
    if default_model not in model_options:
        model_options.insert(0, default_model)
    current_model = st.session_state.get("sales_dashboard_openai_model", default_model)

    if not st.session_state["sales_dashboard_ai_visible"]:
        st.markdown('<div id="sales-ai-launcher-anchor" class="sales-ai-spacer"></div>', unsafe_allow_html=True)
        with st.container(border=False, key="sales_ai_float_launcher"):
            if st.button("✨ Ask AI", key="sales_dashboard_show_ai_chat", use_container_width=True):
                st.session_state["sales_dashboard_ai_visible"] = True
                st.rerun()
        return

    st.markdown('<div id="sales-ai-agent-anchor" class="sales-ai-spacer"></div>', unsafe_allow_html=True)
    with st.container(border=False, key="sales_ai_float_card"):
        h1, h2 = st.columns([0.84, 0.16], vertical_alignment="center")
        with h1:
            st.markdown(
                """
                <div style="display:flex;align-items:center;gap:10px;margin-bottom:2px;">
                    <div style="width:30px;height:30px;border-radius:50%;display:flex;align-items:center;justify-content:center;background:rgba(50,230,198,.18);border:1px solid rgba(89,239,220,.82);box-shadow:0 0 12px rgba(89,239,220,.35);">💡</div>
                    <div>
                        <div style="font-size:1.28rem;font-weight:800;letter-spacing:.2px;line-height:1.1;color:#F3FAFF;">Ask AI</div>
                        <div style="font-size:.82rem;color:rgba(222,238,247,.72);margin-top:4px;">Ask AI about the current sales data</div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        with h2:
            if st.button("×", key="sales_dashboard_hide_ai_chat", use_container_width=True):
                st.session_state["sales_dashboard_ai_visible"] = False
                st.rerun()

        history = st.session_state["sales_dashboard_chat_history"]
        if history:
            with st.expander("Recent answer", expanded=True):
                for item in history[-4:]:
                    role = item.get("role", "assistant")
                    content = item.get("content", "")
                    if role == "user":
                        st.markdown(f"**You:** {content}")
                    else:
                        st.markdown(f"**AI:** {content}")

        with st.form("sales_dashboard_ai_form", clear_on_submit=True):
            user_question = st.text_input(
                "AI question",
                placeholder="Ask your question about the current dashboard...",
                key="sales_dashboard_ai_text_input",
                label_visibility="collapsed",
            )
            b1, b2, b3, b4 = st.columns([0.16, 0.16, 0.40, 0.28], vertical_alignment="center")
            with b1:
                clear_chat = st.form_submit_button("🧹", use_container_width=True)
            with b2:
                use_openai = st.checkbox(
                    "AI",
                    value=bool(st.session_state.get("sales_dashboard_use_openai", ai_ready)),
                    key="sales_dashboard_use_openai",
                    label_visibility="collapsed",
                )
            with b3:
                selected_model = st.selectbox(
                    "Model",
                    model_options,
                    index=model_options.index(current_model) if current_model in model_options else 0,
                    key="sales_dashboard_openai_model",
                    label_visibility="collapsed",
                )
            with b4:
                submitted = st.form_submit_button("Ask AI", use_container_width=True)

        if clear_chat:
            st.session_state["sales_dashboard_chat_history"] = []
            st.rerun()

        if not ai_ready:
            st.caption("OpenAI API key is not configured; AI will use local fallback analysis.")

        if submitted and user_question:
            prompt = _build_ai_prompt(user_question, df, filters, years)
            st.session_state["sales_dashboard_chat_history"].append({"role": "user", "content": user_question})
            diagnostic_tables = _build_ai_tables(df, years)
            with st.spinner("Analysing current dashboard data..."):
                answer = answer_question(
                    df,
                    prompt,
                    diagnostic_tables=diagnostic_tables,
                    filters=filters,
                    use_openai=use_openai,
                    model=selected_model,
                    include_heatmap=True,
                    include_value_chain=True,
                )
            st.session_state["sales_dashboard_chat_history"].append({"role": "assistant", "content": answer})
            st.rerun()

sales = _load_data()
if sales.empty:
    st.warning("No Sales Agent data found. Please maintain Sales Agent data in Database first.")
    st.stop()

filtered, active_filters, year_tuple = _filter_scope(sales)
if filtered.empty:
    st.warning("No sales records match the selected filters.")
    st.stop()

_render_dashboard(filtered, year_tuple, active_filters)
_render_ai_chatbox(filtered, year_tuple, active_filters)
