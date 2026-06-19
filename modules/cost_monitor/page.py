from __future__ import annotations

import io

import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from services.sales_data_loader import (
    init_all_shared_db,
    read_exw_cost_records,
    read_landed_cost_records,
    read_product_master_records,
)


st.title("成本监控 / Cost Monitor")
st.caption("Track collected EXW and Landed Cost by time, model, category and series.")

init_all_shared_db()


EXW_COST_TYPE = "EXW Cost"
LANDED_COST_TYPE = "Landed Cost"


# -----------------------------
# Helpers
# -----------------------------
def _to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Cost Monitor") -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
    return buffer.getvalue()


def _normalise_month(value) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.notna(parsed):
        return parsed.strftime("%Y-%m")
    return text[:7]


def _select_options(series: pd.Series) -> list[str]:
    return sorted([x for x in series.dropna().astype(str).unique().tolist() if x])


def _safe_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col not in out.columns:
            out[col] = ""
    return out[cols]


# -----------------------------
# Data loading
# -----------------------------
def _load_cost_history() -> pd.DataFrame:
    exw = read_exw_cost_records()
    landed = read_landed_cost_records()

    frames: list[pd.DataFrame] = []

    if exw is not None and not exw.empty:
        x = exw.copy()
        x["cost_type"] = EXW_COST_TYPE
        x["cost_value"] = pd.to_numeric(x.get("exw_cost"), errors="coerce")
        if "currency" not in x.columns:
            x["currency"] = "CNY"
        x["currency"] = "CNY"  # EXW fixed as CNY
        frames.append(_safe_cols(x, ["model_id", "cost_type", "cost_value", "currency", "cost_month", "uploaded_at"]))

    if landed is not None and not landed.empty:
        l = landed.copy()
        l["cost_type"] = LANDED_COST_TYPE
        l["cost_value"] = pd.to_numeric(l.get("landed_cost"), errors="coerce")
        if "currency" not in l.columns:
            l["currency"] = "AUD"
        l["currency"] = "AUD"  # Landed fixed as AUD
        frames.append(_safe_cols(l, ["model_id", "cost_type", "cost_value", "currency", "cost_month", "uploaded_at"]))

    if not frames:
        return pd.DataFrame(columns=[
            "model_id", "cost_type", "cost_value", "currency", "cost_month", "uploaded_at",
            "product_line", "category", "hau_model", "hq_model", "series", "cost_date"
        ])

    df = pd.concat(frames, ignore_index=True)
    df["model_id"] = df["model_id"].fillna("").astype(str).str.strip().str.upper()
    df["currency"] = df["currency"].fillna("").astype(str).str.strip().str.upper()
    df["cost_month"] = df["cost_month"].apply(_normalise_month)
    df["cost_date"] = pd.to_datetime(df["cost_month"].astype(str) + "-01", errors="coerce")
    df = df.dropna(subset=["cost_value", "cost_date"])

    df = df.sort_values(["cost_date", "model_id", "cost_type", "uploaded_at"]).drop_duplicates(
        subset=["model_id", "cost_type", "currency", "cost_month"], keep="last"
    )

    try:
        product = read_product_master_records()
    except Exception:
        product = pd.DataFrame()

    if product is not None and not product.empty:
        p = product.copy()
        if "model" in p.columns and "model_id" not in p.columns:
            p = p.rename(columns={"model": "model_id"})
        if "hau_model" in p.columns:
            p["model_id"] = p.get("model_id", p["hau_model"]).fillna(p["hau_model"]).astype(str).str.strip().str.upper()
        elif "model_id" in p.columns:
            p["model_id"] = p["model_id"].fillna("").astype(str).str.strip().str.upper()

        product_cols = [c for c in ["model_id", "product_line", "category", "hau_model", "hq_model", "series"] if c in p.columns]
        if "model_id" in product_cols:
            p = p[product_cols].drop_duplicates(subset=["model_id"], keep="last")
            df = df.merge(p, on="model_id", how="left")

    for col in ["product_line", "category", "hau_model", "hq_model", "series"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str)

    return df.sort_values(["cost_date", "model_id", "cost_type"]).reset_index(drop=True)


# -----------------------------
# Filtering per tab
# -----------------------------
def _filter_df_for_tab(df: pd.DataFrame, cost_type: str) -> pd.DataFrame:
    tab_base = df[df["cost_type"] == cost_type].copy()
    if tab_base.empty:
        return tab_base

    fixed_currency = "CNY" if cost_type == EXW_COST_TYPE else "AUD"
    tab_base = tab_base[tab_base["currency"] == fixed_currency].copy()

    st.caption(f"Currency fixed for this tab: **{fixed_currency}**")

    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        category = st.multiselect(
            "Category",
            _select_options(tab_base["category"]),
            default=[],
            key=f"cm_category_{cost_type}",
            placeholder="All categories",
        )
    with c2:
        series = st.multiselect(
            "Series",
            _select_options(tab_base["series"]),
            default=[],
            key=f"cm_series_{cost_type}",
            placeholder="All series",
        )
    with c3:
        selected_models = st.multiselect(
            "Model",
            _select_options(tab_base["model_id"]),
            default=[],
            key=f"cm_models_filter_{cost_type}",
            placeholder="All models",
        )

    min_date = tab_base["cost_date"].min().date()
    max_date = tab_base["cost_date"].max().date()
    c4, c5 = st.columns([1, 1])
    with c4:
        start_date = st.date_input(
            "From",
            value=min_date,
            min_value=min_date,
            max_value=max_date,
            key=f"cm_start_date_{cost_type}",
        )
    with c5:
        end_date = st.date_input(
            "To",
            value=max_date,
            min_value=min_date,
            max_value=max_date,
            key=f"cm_end_date_{cost_type}",
        )

    if start_date > end_date:
        st.warning("From date cannot be later than To date.")
        return tab_base.head(0).copy()

    out = tab_base.copy()
    if category:
        out = out[out["category"].isin(category)]
    if series:
        out = out[out["series"].isin(series)]
    if selected_models:
        out = out[out["model_id"].isin(selected_models)]
    out = out[(out["cost_date"].dt.date >= start_date) & (out["cost_date"].dt.date <= end_date)]
    return out


# -----------------------------
# Summary / styling
# -----------------------------
def _make_change_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    rows = []
    grouped = df.sort_values("cost_date").groupby(["model_id", "cost_type", "currency"], dropna=False)
    for (model, cost_type, currency), g in grouped:
        if g.empty:
            continue
        latest = g.iloc[-1]
        prev = g.iloc[-2] if len(g) >= 2 else None
        latest_cost = float(latest["cost_value"])
        prev_cost = float(prev["cost_value"]) if prev is not None else None
        change = latest_cost - prev_cost if prev_cost is not None else None
        change_pct = change / prev_cost if prev_cost not in [None, 0] else None
        rows.append({
            "model_id": model,
            "cost_type": cost_type,
            "currency": currency,
            "category": latest.get("category", ""),
            "series": latest.get("series", ""),
            "latest_month": latest.get("cost_month", ""),
            "latest_cost": latest_cost,
            "previous_cost": prev_cost,
            "change": change,
            "change_pct": change_pct,
            "min_cost": float(g["cost_value"].min()),
            "max_cost": float(g["cost_value"].max()),
            "avg_cost": float(g["cost_value"].mean()),
            "records": int(len(g)),
        })

    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["cost_type", "currency", "change_pct"], ascending=[True, True, False], na_position="last")
    return out


def _style_change_table(df: pd.DataFrame):
    if df.empty:
        return df

    def color_change(val):
        if pd.isna(val):
            return ""
        if val > 0.03:
            return "color: #FF6B6B; font-weight: 800;"
        if val < -0.03:
            return "color: #34D399; font-weight: 800;"
        return "color: #CBD5E1;"

    styler = df.style.format({
        "latest_cost": "{:,.2f}",
        "previous_cost": "{:,.2f}",
        "change": "{:+,.2f}",
        "change_pct": "{:+.1%}",
        "min_cost": "{:,.2f}",
        "max_cost": "{:,.2f}",
        "avg_cost": "{:,.2f}",
    }, na_rep="-")

    # pandas 2.1+ uses Styler.map; older versions use Styler.applymap.
    if hasattr(styler, "map"):
        return styler.map(color_change, subset=["change_pct"])
    return styler.applymap(color_change, subset=["change_pct"])



# -----------------------------
# Chart rendering
# -----------------------------
def _render_cost_trend_chart(chart_df: pd.DataFrame, cost_type: str) -> None:
    if chart_df.empty:
        st.info("No chart data available under the selected filters.")
        return

    monthly = chart_df.copy()
    monthly["cost_month"] = monthly["cost_month"].apply(_normalise_month)
    monthly = monthly[monthly["cost_month"].astype(str).str.len() >= 7]

    monthly = (
        monthly
        .groupby(["cost_month", "series_key"], as_index=False)["cost_value"]
        .mean()
        .sort_values(["cost_month", "series_key"])
    )
    monthly["cost_date"] = pd.to_datetime(monthly["cost_month"] + "-01", errors="coerce")
    monthly = monthly.dropna(subset=["cost_date", "cost_value"])

    if monthly.empty:
        st.info("No chart data available under the selected filters.")
        return

    pivot = monthly.pivot_table(
        index="cost_date",
        columns="series_key",
        values="cost_value",
        aggfunc="mean",
    ).sort_index()

    fig = go.Figure()
    for col in pivot.columns:
        line = pivot[col].dropna()
        if line.empty:
            continue
        fig.add_trace(
            go.Scatter(
                x=line.index,
                y=line.values,
                mode="lines+markers",
                name=str(col),
                hovertemplate="%{x|%Y-%m}<br>Cost: %{y:,.2f}<extra>%{fullData.name}</extra>",
            )
        )

    all_values = pd.Series(pivot.to_numpy().ravel()).dropna()
    if not all_values.empty:
        y_min = float(all_values.min())
        y_max = float(all_values.max())
        if y_min == y_max:
            gap = max(abs(y_max) * 0.03, 10)
        else:
            gap = max((y_max - y_min) * 0.15, 10)
        fig.update_yaxes(range=[y_min - gap, y_max + gap])

    x_min = pivot.index.min()
    x_max = pivot.index.max()
    if pd.notna(x_min) and pd.notna(x_max):
        fig.update_xaxes(range=[x_min, x_max])

    fig.update_layout(
        height=460,
        margin=dict(l=20, r=20, t=20, b=20),
        xaxis=dict(
            title="Month",
            tickformat="%Y-%m",
            dtick="M1",
            showgrid=True,
        ),
        yaxis=dict(
            title="Cost",
            tickformat=",.0f",
            showgrid=True,
            zeroline=False,
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)

# -----------------------------
# Render tab
# -----------------------------
def _render_cost_type_tab(df: pd.DataFrame, cost_type: str) -> None:
    filtered_df = _filter_df_for_tab(df, cost_type)

    if filtered_df.empty:
        st.warning(f"No {cost_type} records match the selected filters.")
        return

    latest_month = filtered_df["cost_month"].max()
    latest_avg = filtered_df[filtered_df["cost_month"] == latest_month]["cost_value"].mean()
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Models", f"{filtered_df['model_id'].nunique():,}")
    m2.metric("Records", f"{len(filtered_df):,}")
    m3.metric("Latest Month", latest_month)
    m4.metric("Latest Avg Cost", f"{latest_avg:,.2f}")

    st.markdown("---")
    st.subheader(f"{cost_type} Trend")
    chart_mode = st.radio(
        "Chart mode",
        ["By Model", "Average"],
        horizontal=True,
        key=f"cm_chart_mode_{cost_type}",
    )

    if chart_mode == "By Model":
        # Draw directly from the model filter above. If Model filter is blank, draw all filtered models.
        chart_df = filtered_df.copy()
        chart_df["series_key"] = chart_df["model_id"]
    else:
        chart_df = filtered_df.groupby(["cost_date", "cost_month"], as_index=False)["cost_value"].mean()
        chart_df["series_key"] = f"{cost_type} Average"

    _render_cost_trend_chart(chart_df, cost_type)

    change_table = _make_change_table(filtered_df)
    st.subheader("Latest Change Summary")
    st.caption("Red = cost increased over 3%; Green = cost decreased over 3%; grey = within ±3% or no previous record.")
    st.dataframe(_style_change_table(change_table), use_container_width=True, height=420, hide_index=True)

    st.subheader("Filtered Detail")
    detail_cols = [
        "cost_month", "model_id", "cost_type", "cost_value", "currency",
        "product_line", "category", "series", "hau_model", "hq_model", "uploaded_at"
    ]
    detail = _safe_cols(filtered_df, detail_cols).sort_values(["cost_month", "model_id"], ascending=[False, True])
    file_prefix = "exw" if cost_type == EXW_COST_TYPE else "landed"
    st.download_button(
        f"Download Filtered {cost_type} Data",
        data=_to_excel_bytes(detail, cost_type),
        file_name=f"cost_monitor_{file_prefix}_filtered.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        key=f"cm_download_filtered_{file_prefix}",
    )
    st.dataframe(detail, use_container_width=True, height=520, hide_index=True)


all_cost = _load_cost_history()

if all_cost.empty:
    st.warning("No cost history found. Please maintain EXW / Landed Cost in Database > Cost DB first.")
    st.stop()

tab_exw, tab_landed = st.tabs(["EXW Cost Trend", "Landed Cost Trend"])
with tab_exw:
    _render_cost_type_tab(all_cost, EXW_COST_TYPE)
with tab_landed:
    _render_cost_type_tab(all_cost, LANDED_COST_TYPE)
