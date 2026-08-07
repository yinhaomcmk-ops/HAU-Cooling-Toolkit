from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

from services.sales_ai_engine import (
    answer_question,
)

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "app_data.db"

st.title("KPI Dashboard")
st.caption("DN / POD reporting layer from SQLite. Filters apply to both the dashboard and the floating AI panel.")

st.markdown(
    """
    <style>
    .kpi-hero {padding:18px 20px;border:1px solid rgba(255,255,255,.10);border-radius:22px;background:linear-gradient(135deg,rgba(6,182,212,.12),rgba(15,23,42,.28));box-shadow:0 18px 38px rgba(0,0,0,.22);margin-bottom:18px;}
    .kpi-section {margin:26px 0 12px 0;font-size:1.35rem;font-weight:900;color:#F2F9FF;letter-spacing:.2px;}
    .kpi-card {border:1px solid rgba(255,255,255,.11);border-radius:18px;padding:15px 16px;background:linear-gradient(180deg,rgba(255,255,255,.065),rgba(255,255,255,.025));box-shadow:0 12px 32px rgba(0,0,0,.20);min-height:98px;}
    .kpi-card-label {font-size:.78rem;font-weight:800;color:rgba(226,241,248,.68);text-transform:uppercase;letter-spacing:.06em;}
    .kpi-card-value {font-size:2rem;font-weight:900;color:#F5FBFF;line-height:1.15;margin-top:7px;}
    .kpi-card-sub {font-size:.82rem;color:rgba(183,206,219,.78);margin-top:6px;}
    .kpi-panel {border:1px solid rgba(255,255,255,.09);border-radius:18px;padding:14px 16px;background:rgba(255,255,255,.028);}
    .block-container {padding-bottom:10rem!important;}
    [class*="kpi_ai_float_card"] {position:fixed!important;right:1.25rem!important;bottom:1.15rem!important;width:min(440px,calc(100vw - 2rem))!important;max-height:58vh!important;overflow:auto!important;z-index:999999!important;padding:14px 16px 12px!important;border-radius:18px!important;background:rgba(7,10,18,.76)!important;border:1.5px solid rgba(98,222,231,.82)!important;box-shadow:0 0 0 1px rgba(255,255,255,.08) inset,0 14px 42px rgba(0,0,0,.48)!important;backdrop-filter:blur(14px) saturate(120%)!important;}
    [class*="kpi_ai_float_launcher"] {position:fixed!important;right:1.25rem!important;bottom:1.15rem!important;z-index:999999!important;width:172px!important;padding:8px 10px!important;border-radius:999px!important;background:rgba(7,10,18,.76)!important;border:1.5px solid rgba(98,222,231,.82)!important;box-shadow:0 12px 32px rgba(0,0,0,.44)!important;backdrop-filter:blur(14px)!important;}
    [class*="kpi_ai_float_card"] div[data-testid="stForm"] {border:0!important;padding:0!important;background:transparent!important;}
    [class*="kpi_ai_float_card"] button,[class*="kpi_ai_float_launcher"] button {border-radius:999px!important;border:1px solid rgba(92,205,215,.65)!important;background:rgba(14,29,39,.72)!important;color:#E8F7FB!important;min-height:34px!important;}
    [class*="kpi_ai_float_card"] div[data-testid="stTextInput"] input {height:34px!important;border-radius:999px!important;background:rgba(255,255,255,.06)!important;border:1px solid rgba(185,206,220,.72)!important;color:#F4FAFF!important;padding-left:14px!important;font-size:.88rem!important;}
    </style>
    """,
    unsafe_allow_html=True,
)


def _table_exists(table: str) -> bool:
    if not DB_PATH.exists():
        return False
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


@st.cache_data(show_spinner=False)
def _read_table(table: str) -> pd.DataFrame:
    if not DB_PATH.exists() or not _table_exists(table):
        return pd.DataFrame()
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(f'SELECT * FROM "{table}"', conn)


def _num(s) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0) if isinstance(s, pd.Series) else pd.Series(dtype=float)


def _fmt_money(v: float) -> str:
    return f"${v/1_000_000:,.1f}M" if abs(v) >= 1_000_000 else f"${v:,.0f}"


def _fmt_qty(v: float) -> str:
    return f"{v:,.0f}"


def _pick_col(df: pd.DataFrame, names: list[str]) -> str | None:
    low = {str(c).strip().lower(): c for c in df.columns}
    for n in names:
        if n.lower() in low:
            return low[n.lower()]
    return None


def _metric_card(label: str, value: str, sub: str = "") -> None:
    st.markdown(
        f"""
        <div class="kpi-card">
          <div class="kpi-card-label">{label}</div>
          <div class="kpi-card-value">{value}</div>
          <div class="kpi-card-sub">{sub}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _section(title: str) -> None:
    st.markdown(f'<div class="kpi-section">{title}</div>', unsafe_allow_html=True)


def _top_text(df: pd.DataFrame, group: str | None, value: str | None, label: str) -> list[str]:
    if df.empty or not group or not value or group not in df.columns or value not in df.columns:
        return []
    g = df.groupby(group, dropna=False)[value].sum().sort_values(ascending=False).head(5)
    return [f"{label} Top {i+1}: {idx} ({_fmt_qty(val) if 'qty' in value.lower() else _fmt_money(val)})" for i, (idx, val) in enumerate(g.items())]


def _render_ai_chatbox(summary_lines: list[str], df: pd.DataFrame, value_cols: list[str], key_prefix: str) -> None:
    if f"{key_prefix}_kpi_chat_history" not in st.session_state:
        st.session_state[f"{key_prefix}_kpi_chat_history"] = []
    if f"{key_prefix}_kpi_ai_visible" not in st.session_state:
        st.session_state[f"{key_prefix}_kpi_ai_visible"] = True

    if not st.session_state[f"{key_prefix}_kpi_ai_visible"]:
        with st.container(
        border=False,
        key=f"{key_prefix}_kpi_ai_float_launcher"
        ):
            if st.button("✨ Ask AI", key=f"{key_prefix}_kpi_show_ai", use_container_width=True):
                st.session_state[f"{key_prefix}_kpi_ai_visible"] = True
                st.rerun()
        return

    with st.container(
    border=False,
    key=f"{key_prefix}_kpi_ai_float_card"
        ):
        h1, h2 = st.columns([0.84, 0.16], vertical_alignment="center")
        with h1:
            st.markdown(
                """
                <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px;">
                    <div style="width:30px;height:30px;border-radius:50%;display:flex;align-items:center;justify-content:center;background:rgba(50,230,198,.18);border:1px solid rgba(89,239,220,.82);box-shadow:0 0 12px rgba(89,239,220,.35);">💡</div>
                    <div><div style="font-size:1.28rem;font-weight:800;color:#F3FAFF;line-height:1.1;">Ask AI</div><div style="font-size:.82rem;color:rgba(222,238,247,.72);margin-top:4px;">Ask AI about the current KPI data</div></div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        with h2:
            if st.button("×", key=f"{key_prefix}_kpi_hide_ai", use_container_width=True):
                st.session_state[f"{key_prefix}_kpi_ai_visible"] = False
                st.rerun()

        history = st.session_state[f"{key_prefix}_kpi_chat_history"]
        if history:
            with st.expander("Recent answer", expanded=True):
                for item in history[-4:]:
                    st.markdown(f"**{item['role']}:** {item['content']}")
        else:
            with st.expander("Current snapshot", expanded=True):
                st.markdown("\n".join([f"- {x}" for x in summary_lines]) if summary_lines else "No available KPI summary.")

        analysis_mode = st.radio(
        "",
        ["Quick Ask", "Deep Analysis"],
        horizontal=True,
        key=f"{key_prefix}_analysis_mode",
        )

        q1, q2, q3, q4 = st.columns(4)

        growth = q1.button(
            "📈 Growth",
            key=f"{key_prefix}_growth"
        )
                
        decline = q2.button(
            "📉 Decline",
            key=f"{key_prefix}_decline"
        )

        opp = q3.button(
            "🎯 Opportunity",
            key=f"{key_prefix}_opp"
        )

        risk = q4.button(
            "⚠ Risk",
            key=f"{key_prefix}_risk"
        )   
        with st.form(f"{key_prefix}_kpi_ai_form", clear_on_submit=True):
            q = st.text_input("Question", placeholder="Ask your question about the current KPI dashboard...", key=f"{key_prefix}_kpi_q", label_visibility="collapsed")
            b1, b2 = st.columns([0.25, 0.75])
            clear = b1.form_submit_button("🧹", use_container_width=True)
            ask = b2.form_submit_button("Ask AI", use_container_width=True)

        preset_q = None

        if clear:
            st.session_state[
                f"{key_prefix}_kpi_chat_history"
            ] = []
            st.rerun()

        if growth:
            preset_q = "What are the biggest growth drivers?"

        elif decline:
            preset_q = "What are the biggest decline drivers?"

        elif opp:
            preset_q = "What are the biggest opportunities?"

        elif risk:
            preset_q = "What are the biggest risks?"

        final_question = preset_q or q.strip()

        if final_question:

            with st.spinner("Analysing KPI data..."):

                answer = _ask_gpt(
                    final_question,
                    df,
                    summary_lines,
                    analysis_mode,
                )

            st.session_state[
                f"{key_prefix}_kpi_chat_history"
            ].append(
                {
                    "role": "You",
                    "content": final_question,
                }
            )

            st.session_state[
                f"{key_prefix}_kpi_chat_history"
            ].append(
                {
                    "role": "AI",
                    "content": answer,
                }
            )

            st.rerun()
            st.session_state[f"{key_prefix}_kpi_chat_history"].append({"role": "You", "content": q.strip()})
            st.session_state[f"{key_prefix}_kpi_chat_history"].append({"role": "AI", "content": answer})
            st.rerun()


def _rule_answer(q: str, df: pd.DataFrame, value_cols: list[str]) -> str:
    q_low = q.lower()
    if df.empty:
        return "当前筛选下没有数据。"
    if any(x in q_low for x in ["month", "月", "trend", "趋势"]):
        return "趋势请看当前页面的 Month Trend。建议重点关注最近 3 个月是否连续上升/下降，以及是否由单一客户或型号拉动。"
    candidates = [c for c in df.columns if any(k in str(c).lower() for k in ["model", "型号", "region", "brand", "channel", "group", "customer"])]
    value = value_cols[0] if value_cols else None
    if candidates and value:
        gcol = candidates[0]
        top = df.groupby(gcol, dropna=False)[value].sum().sort_values(ascending=False).head(5)
        lines = [f"{idx}: {_fmt_qty(val) if 'qty' in value.lower() else _fmt_money(val)}" for idx, val in top.items()]
        return "当前贡献最高的项目：\n" + "\n".join(lines) + "\n\n建议进一步看这些项目的月度趋势，确认是持续增长还是一次性波动。"
    return "当前数据已按页面筛选。建议从贡献 Top、月度趋势、客户/型号结构三个角度判断 KPI 变化。"

def _ask_gpt(
    question: str,
    df: pd.DataFrame,
    summary_lines: list[str],
    mode: str = "Quick Ask",
):
    prompt = f"""
KPI Dashboard Analysis

Current KPI Snapshot:
{chr(10).join(summary_lines)}

User Question:
{question}

Provide:
1. Executive Summary
2. Key Drivers
3. Risks
4. Recommended Actions
"""

    return answer_question(
        filtered_current=df,
        question=prompt,
        diagnostic_tables={},
        use_openai=True,
        model="gpt-5" if mode == "Deep Analysis" else "gpt-5-mini",
        include_heatmap=False,
        include_value_chain=False,
    )

def _prep_dn() -> tuple[pd.DataFrame, dict]:
    df = _read_table("kpi_dn_records").copy()
    if df.empty:
        return df, {}
    cols = {
        "qty": _pick_col(df, ["Deliv. QTY", "Deliv QTY", "QTY", "Qty"]),
        "amount": _pick_col(df, ["Amount", "DN Amount", "AMTAUD-TY"]),
        "cost": _pick_col(df, ["Cost"]),
        "date": _pick_col(df, ["Created on", "Date", "created_on"]),
        "region": _pick_col(df, ["Region Description", "Region"]),
        "group": _pick_col(df, ["Cust. grp 2 desc", "Customer Group", "Customer Group 2"]),
    }
    if not cols["qty"]:
        df["DN Qty"] = 1; cols["qty"] = "DN Qty"
    if not cols["amount"]:
        df["Amount"] = 0; cols["amount"] = "Amount"
    for c in [cols["qty"], cols["amount"], cols["cost"]]:
        if c:
            df[c] = _num(df[c])
    if cols["date"]:
        df[cols["date"]] = pd.to_datetime(df[cols["date"]], errors="coerce")
        df["Month"] = df[cols["date"]].dt.to_period("M").astype(str)
    else:
        df["Month"] = "-"
    return df, cols


def render_dn():
    df, cols = _prep_dn()
    if df.empty:
        st.warning("kpi_dn_records is empty. Please run: python scripts/build_database.py")
        return
    date_col, qty_col, amount_col = cols["date"], cols["qty"], cols["amount"]
    years = sorted(df[date_col].dt.year.dropna().astype(int).unique().tolist()) if date_col else []
    f1, f2, f3 = st.columns(3)
    y = f1.multiselect("Year", years, default=years[-1:] if years else [], key="dn_year")
    regions = sorted(df[cols["region"]].dropna().astype(str).unique().tolist()) if cols["region"] else []
    reg = f2.multiselect("Region", regions, default=[], key="dn_region")
    groups = sorted(df[cols["group"]].dropna().astype(str).unique().tolist()) if cols["group"] else []
    grp = f3.multiselect("Customer Group", groups, default=[], key="dn_group")
    f = df.copy()
    if y and date_col: f = f[f[date_col].dt.year.isin(y)]
    if reg and cols["region"]: f = f[f[cols["region"]].astype(str).isin(reg)]
    if grp and cols["group"]: f = f[f[cols["group"]].astype(str).isin(grp)]

    a, b, c, d = st.columns(4)
    a.markdown(_metric_html("DN Qty", _fmt_qty(f[qty_col].sum()), "Filtered delivery quantity"), unsafe_allow_html=True)
    b.markdown(_metric_html("DN Amount", _fmt_money(f[amount_col].sum()), "Filtered amount"), unsafe_allow_html=True)
    c.markdown(_metric_html("Avg Price", _fmt_money(f[amount_col].sum() / max(f[qty_col].sum(), 1)), "Amount / Qty"), unsafe_allow_html=True)
    d.markdown(_metric_html("Rows", _fmt_qty(len(f)), "Filtered records"), unsafe_allow_html=True)

    _section("Executive View")
    c1, c2, c3 = st.columns([1.25, 1, 1])
    with c1:
        st.markdown('<div class="kpi-panel">', unsafe_allow_html=True)
        st.subheader("Month Trend")
        st.bar_chart(f.groupby("Month")[[qty_col, amount_col]].sum(), height=300, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    with c2:
        st.subheader("Region Contribution")
        if cols["region"]:
            st.dataframe(f.groupby(cols["region"])[[qty_col, amount_col]].sum().sort_values(amount_col, ascending=False).reset_index(), use_container_width=True, hide_index=True, height=310)
    with c3:
        st.subheader("Customer Group")
        if cols["group"]:
            st.dataframe(f.groupby(cols["group"])[[qty_col, amount_col]].sum().sort_values(amount_col, ascending=False).reset_index(), use_container_width=True, hide_index=True, height=310)
    value_cols = [c for c in [qty_col, amount_col, cols["cost"]] if c]
    lines = [f"Filtered DN Qty: {_fmt_qty(f[qty_col].sum())}", f"Filtered DN Amount: {_fmt_money(f[amount_col].sum())}"] + _top_text(f, cols["region"], amount_col, "Region") + _top_text(f, cols["group"], amount_col, "Customer Group")
    _render_ai_chatbox(lines, f, value_cols, "dn")


def _metric_html(label: str, value: str, sub: str = "") -> str:
    return f'<div class="kpi-card"><div class="kpi-card-label">{label}</div><div class="kpi-card-value">{value}</div><div class="kpi-card-sub">{sub}</div></div>'


def render_pod():
    df = _read_table("kpi_pod_records").copy()
    if df.empty:
        st.warning("kpi_pod_records is empty. Please run: python scripts/build_database.py")
        return
    date_col = _pick_col(df, ["Date", "Created on", "created_on"])
    model_col = _pick_col(df, ["客户型号", "Customer Model", "model"])
    brand_col = _pick_col(df, ["品牌", "Brand"])
    channel_col = _pick_col(df, ["电商标识", "Channel Type", "Channel"])
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df["Month"] = df[date_col].dt.to_period("M").astype(str)
        years = sorted(df[date_col].dt.year.dropna().astype(int).unique().tolist())
    else:
        df["Month"] = "-"; years = []
    df["POD Qty"] = 1
    if model_col:
        df = df[df[model_col].astype(str).str.upper().ne("0")]
    f1, f2, f3 = st.columns(3)
    y = f1.multiselect("Year", years, default=years[-1:] if years else [], key="pod_year")
    brands = sorted(df[brand_col].dropna().astype(str).unique().tolist()) if brand_col else []
    brand = f2.multiselect("Brand", brands, default=[], key="pod_brand")
    channels = sorted(df[channel_col].dropna().astype(str).unique().tolist()) if channel_col else []
    channel = f3.multiselect("Channel Type", channels, default=[], key="pod_ec")
    f = df.copy()
    if y and date_col: f = f[f[date_col].dt.year.isin(y)]
    if brand and brand_col: f = f[f[brand_col].astype(str).isin(brand)]
    if channel and channel_col: f = f[f[channel_col].astype(str).isin(channel)]

    a, b, c = st.columns(3)
    a.markdown(_metric_html("POD Qty", _fmt_qty(f["POD Qty"].sum()), "Filtered POD records"), unsafe_allow_html=True)
    b.markdown(_metric_html("Models", _fmt_qty(f[model_col].nunique() if model_col else 0), "Active models"), unsafe_allow_html=True)
    c.markdown(_metric_html("Rows", _fmt_qty(len(f)), "Filtered records"), unsafe_allow_html=True)

    _section("Executive View")
    c1, c2, c3 = st.columns([1.25, 1, 1])
    with c1:
        st.subheader("Month Trend")
        st.bar_chart(f.groupby("Month")[["POD Qty"]].sum(), height=300, use_container_width=True)
    with c2:
        st.subheader("Model Contribution")
        if model_col:
            st.dataframe(f.groupby(model_col)[["POD Qty"]].sum().sort_values("POD Qty", ascending=False).head(30).reset_index(), use_container_width=True, hide_index=True, height=310)
    with c3:
        st.subheader("Brand / Channel")
        cols = [c for c in [brand_col, channel_col] if c]
        if cols:
            st.dataframe(f.groupby(cols)[["POD Qty"]].sum().sort_values("POD Qty", ascending=False).reset_index(), use_container_width=True, hide_index=True, height=310)
    lines = [f"Filtered POD Qty: {_fmt_qty(f['POD Qty'].sum())}"] + _top_text(f, model_col, "POD Qty", "Model")
    _render_ai_chatbox(lines, f, ["POD Qty"], "pod")


st.markdown('<div class="kpi-hero"><b>DN / POD KPI</b><br><span style="color:rgba(225,240,248,.72);">High-level delivery and proof-of-delivery performance view with a shared floating AI assistant.</span></div>', unsafe_allow_html=True)
dn_tab, pod_tab = st.tabs(["DN", "POD"])
with dn_tab:
    render_dn()
with pod_tab:
    render_pod()
