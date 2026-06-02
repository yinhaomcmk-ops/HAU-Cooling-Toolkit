# overall.py
# AU Cooling Toolkit - Value Chain / Overall
# Fixes included:
# 1) Upload edited Excel/CSV correctly loads into session_state and can be saved to data/overall_state.json
# 2) Editable table uses st.form(), so editing one cell will not rerun and jump back to the first row
# 3) Customer model columns have wider display width

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import streamlit as st


DATA_DIR = Path("data")
OVERALL_STATE_PATH = DATA_DIR / "overall_state.json"
MODEL_MASTER_PATH = DATA_DIR / "model_master.json"
EXW_COST_PATH = DATA_DIR / "exw_cost.json"
LANDED_COST_PATH = DATA_DIR / "landed_cost.json"


# -----------------------------
# Basic helpers
# -----------------------------
def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _safe_num(value, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        if isinstance(value, str):
            value = value.replace("$", "").replace(",", "").replace("%", "").strip()
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _read_json_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_json(path)
    except ValueError:
        try:
            return pd.read_json(path, orient="records")
        except Exception:
            return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _save_json_df(df: pd.DataFrame, path: Path) -> None:
    _ensure_data_dir()
    clean_df = df.copy()
    clean_df = clean_df.replace([np.inf, -np.inf], np.nan)
    clean_df.to_json(path, orient="records", force_ascii=False, indent=2)


def _excel_bytes(df: pd.DataFrame, sheet_name: str = "Overall") -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()


def _read_upload(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    return pd.read_excel(uploaded_file)


def _fmt_currency(v) -> str:
    try:
        return f"$ {float(v):,.0f}"
    except Exception:
        return ""


def _fmt_pct(v) -> str:
    try:
        return f"{float(v):.1f}%"
    except Exception:
        return ""


# -----------------------------
# Default data
# -----------------------------
def _default_overall_df() -> pd.DataFrame:
    # Fallback data only. If data/overall_state.json exists, app uses saved data instead.
    rows = [
        ["HRBM418S", "Refrigerator", "Bottom Mount", "2026/04", 67, 1099, 899, 799, 45, 55, 0, None, None, 0, 0, None, None, 0, 0],
        ["HRBM419B", "Refrigerator", "Bottom Mount", "2026/04", 100, 1399, 1299, 1199, 45, 55, 0, None, None, 0, 0, None, None, 0, 0],
        ["HRBM419BW", "Refrigerator", "Bottom Mount", "2026/04", 100, 1399, 1299, 1199, 45, 55, 0, None, None, 0, 0, None, None, 0, 0],
        ["HRBM500TBW", "Refrigerator", "Bottom Mount", "2026/04", 63, 1299, 1199, 1199, 45, 55, 0, None, None, 0, 0, None, None, 0, 0],
        ["HRBM503E", "Refrigerator", "Bottom Mount", "2025/11", 63, 1799, 1499, 1499, 45, 55, 0, None, None, 0, 0, None, None, 0, 0],
        ["HRCD483G", "Refrigerator", "Quad Door", "2026/04", 58, 1699, 1499, 1499, 45, 55, 0, None, None, 0, 0, None, None, 0, 0],
        ["HRCD483TBW", "Refrigerator", "Quad Door", "2026/04", 58, 1899, 1699, 1699, 45, 55, 0, None, None, 0, 0, None, None, 0, 0],
        ["HRCD483TS", "Refrigerator", "Quad Door", "2026/04", 58, 1599, 1299, 1299, 45, 55, 0, None, None, 0, 0, None, None, 0, 0],
        ["HRCD483TSW", "Refrigerator", "Quad Door", "2026/04", 58, 1799, 1599, 1499, 45, 55, 0, None, None, 0, 0, None, None, 0, 0],
    ]
    cols = [
        "客户型号", "产品线", "品类", "成本月份", "柜量",
        "常规价", "促销价", "大促价", "常规%", "促销%", "大促%",
        "A品牌", "A型号", "A常规", "A促销",
        "B品牌", "B型号", "B常规", "B促销",
    ]
    return pd.DataFrame(rows, columns=cols)


def load_overall_state() -> pd.DataFrame:
    df = _read_json_df(OVERALL_STATE_PATH)
    if df.empty:
        df = _default_overall_df()
    return normalize_overall_df(df)


def normalize_overall_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    required_cols = [
        "客户型号", "产品线", "品类", "成本月份", "柜量",
        "常规价", "促销价", "大促价", "常规%", "促销%", "大促%",
        "A品牌", "A型号", "A常规", "A促销",
        "B品牌", "B型号", "B常规", "B促销",
    ]
    for col in required_cols:
        if col not in df.columns:
            if col in ["A品牌", "A型号", "B品牌", "B型号"]:
                df[col] = None
            else:
                df[col] = 0

    text_cols = ["客户型号", "产品线", "品类", "成本月份", "A品牌", "A型号", "B品牌", "B型号"]
    num_cols = [c for c in required_cols if c not in text_cols]

    for col in text_cols:
        df[col] = df[col].where(df[col].notna(), None)

    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Keep original extra columns after required columns.
    extra_cols = [c for c in df.columns if c not in required_cols]
    return df[required_cols + extra_cols]


# -----------------------------
# Cost lookup helpers
# -----------------------------
def _get_latest_cost_map(path: Path, model_col_candidates: List[str], cost_col_candidates: List[str]) -> Dict[str, float]:
    df = _read_json_df(path)
    if df.empty:
        return {}

    model_col = next((c for c in model_col_candidates if c in df.columns), None)
    cost_col = next((c for c in cost_col_candidates if c in df.columns), None)
    if not model_col or not cost_col:
        return {}

    df = df.copy()
    df[model_col] = df[model_col].astype(str).str.strip()
    df[cost_col] = pd.to_numeric(df[cost_col], errors="coerce")

    date_col = next((c for c in ["成本时间", "成本月份", "date", "Date", "Month"] if c in df.columns), None)
    if date_col:
        df = df.sort_values(date_col)

    df = df.dropna(subset=[model_col, cost_col])
    latest = df.groupby(model_col, as_index=False).tail(1)
    return dict(zip(latest[model_col], latest[cost_col]))


def get_cost_maps() -> Tuple[Dict[str, float], Dict[str, float]]:
    exw_map = _get_latest_cost_map(
        EXW_COST_PATH,
        ["客户型号", "HAU Model", "HAU model", "型号", "model", "Model"],
        ["EXW", "exw", "成本EXW", "exw cost", "EXW Cost"],
    )
    landed_map = _get_latest_cost_map(
        LANDED_COST_PATH,
        ["客户型号", "HAU Model", "HAU model", "型号", "model", "Model"],
        ["Landed Cost", "landed cost", "landed_cost", "到岸成本", "landed"],
    )
    return exw_map, landed_map


# -----------------------------
# Calculation
# -----------------------------
def calculate_overall(df: pd.DataFrame, params: Dict[str, float]) -> pd.DataFrame:
    df = normalize_overall_df(df)
    exw_map, landed_map = get_cost_maps()

    aud_cny = _safe_num(params.get("AUD_CNY", 4.8), 4.8)
    aud_usd = _safe_num(params.get("AUD_USD", 0.63), 0.63)
    sea_freight_usd = _safe_num(params.get("SEA_FREIGHT_40HQ_USD", 1750), 1750)
    clearance_aud = _safe_num(params.get("CLEARANCE_AUD", 3000), 3000)
    insurance_pct = _safe_num(params.get("INSURANCE_PCT", 0.3), 0.3) / 100
    singa_upcost_pct = _safe_num(params.get("SINGA_UPCOST_PCT", 0.3), 0.3) / 100
    regular_rebate = _safe_num(params.get("REGULAR_REBATE_PCT", 40), 40) / 100
    promo_rebate = _safe_num(params.get("PROMO_REBATE_PCT", 35), 35) / 100
    big_promo_rebate = _safe_num(params.get("BIG_PROMO_REBATE_PCT", 32), 32) / 100

    result = df.copy()

    regular_mix = result["常规%"].apply(_safe_num) / 100
    promo_mix = result["促销%"].apply(_safe_num) / 100
    big_mix = result["大促%"].apply(_safe_num) / 100
    mix_sum = regular_mix + promo_mix + big_mix
    mix_sum = mix_sum.replace(0, 1)
    regular_mix = regular_mix / mix_sum
    promo_mix = promo_mix / mix_sum
    big_mix = big_mix / mix_sum

    regular_price = result["常规价"].apply(_safe_num)
    promo_price = result["促销价"].apply(_safe_num)
    big_price = result["大促价"].apply(_safe_num)

    weighted_rrp = regular_price * regular_mix + promo_price * promo_mix + big_price * big_mix
    invoice = weighted_rrp / 1.1

    rebate_amount = (
        regular_price / 1.1 * regular_rebate * regular_mix
        + promo_price / 1.1 * promo_rebate * promo_mix
        + big_price / 1.1 * big_promo_rebate * big_mix
    )
    netnet = invoice - rebate_amount

    model_series = result["客户型号"].astype(str).str.strip()
    landed_cost = model_series.map(landed_map).fillna(0).astype(float)
    exw_cost = model_series.map(exw_map).fillna(0).astype(float)

    cabinet_count = result["柜量"].apply(_safe_num).replace(0, np.nan)
    freight_per_unit_aud = (sea_freight_usd / max(aud_usd, 0.0001)) / cabinet_count
    clearance_per_unit_aud = clearance_aud / cabinet_count

    # If landed cost exists, use landed cost as base. Otherwise estimate from EXW.
    estimated_fob_aud = (exw_cost / max(aud_cny, 0.0001)) * (1 + singa_upcost_pct)
    insurance_aud = estimated_fob_aud * insurance_pct
    estimated_total_cost = estimated_fob_aud + insurance_aud + freight_per_unit_aud.fillna(0) + clearance_per_unit_aud.fillna(0)
    total_cost = landed_cost.where(landed_cost > 0, estimated_total_cost)

    variable_rate_map = {
        "Refrigerator": 0.29,
        "Wine Cabinet": 0.28,
        "Freezer": 0.20,
    }
    variable_rate = result["产品线"].map(variable_rate_map).fillna(0.29)
    variable_cost = netnet * variable_rate
    total_cost_with_variable = total_cost + variable_cost

    gross_margin = np.where(netnet > 0, (netnet - total_cost_with_variable) / netnet * 100, 0)
    net_margin = np.where(netnet > 0, (netnet - total_cost) / netnet * 100, 0)

    result["毛利率"] = np.round(gross_margin, 1)
    result["净利率"] = np.round(net_margin, 1)
    result["常规价显示"] = weighted_rrp.apply(_fmt_currency)
    result["NETNET"] = np.round(netnet, 0)
    result["到岸成本"] = np.round(total_cost, 0)
    result["变动费用"] = np.round(variable_cost, 0)
    result["总成本"] = np.round(total_cost_with_variable, 0)
    result["FOB(AUD)"] = np.round(estimated_fob_aud, 0)
    result["综合运费"] = np.round(freight_per_unit_aud.fillna(0) + clearance_per_unit_aud.fillna(0), 0)

    return result


# -----------------------------
# UI parts
# -----------------------------
def get_params_from_session() -> Dict[str, float]:
    defaults = {
        "AUD_CNY": 4.8,
        "AUD_USD": 0.63,
        "SEA_FREIGHT_40HQ_USD": 1750,
        "CLEARANCE_AUD": 3000,
        "INSURANCE_PCT": 0.3,
        "SINGA_UPCOST_PCT": 0.3,
        "REGULAR_REBATE_PCT": 40.0,
        "PROMO_REBATE_PCT": 35.0,
        "BIG_PROMO_REBATE_PCT": 32.0,
    }
    params = {}
    for k, v in defaults.items():
        params[k] = _safe_num(st.session_state.get(k, v), v)
    return params


def render_global_params_sidebar() -> None:
    st.sidebar.markdown("### Global Parameters")

    controls = [
        ("AUD_CNY", "1 AUD = CNY", 0.1),
        ("AUD_USD", "1 AUD = USD", 0.01),
        ("SEA_FREIGHT_40HQ_USD", "Sea Freight / 40HQ (USD)", 50.0),
        ("CLEARANCE_AUD", "Custom Clearance & Cartage (AUD)", 100.0),
        ("INSURANCE_PCT", "Insurance (%)", 0.1),
        ("SINGA_UPCOST_PCT", "Singa Upcost (%)", 0.1),
        ("REGULAR_REBATE_PCT", "Regular Rebate (%)", 1.0),
        ("PROMO_REBATE_PCT", "Promo Rebate (%)", 1.0),
        ("BIG_PROMO_REBATE_PCT", "Big Promo Rebate (%)", 1.0),
    ]

    defaults = {
        "AUD_CNY": 4.8,
        "AUD_USD": 0.63,
        "SEA_FREIGHT_40HQ_USD": 1750,
        "CLEARANCE_AUD": 3000,
        "INSURANCE_PCT": 0.3,
        "SINGA_UPCOST_PCT": 0.3,
        "REGULAR_REBATE_PCT": 40.0,
        "PROMO_REBATE_PCT": 35.0,
        "BIG_PROMO_REBATE_PCT": 32.0,
    }

    for key, label, step in controls:
        if key not in st.session_state:
            st.session_state[key] = defaults[key]
        st.number_input(label, key=key, step=step, format="%.4f" if step < 0.1 else "%.1f")


def render_batch_editor() -> pd.DataFrame:
    st.markdown("#### Batch Download / Upload")
    st.caption(
        "Download the current editable table, edit Overall fields and Competitor fields, then upload it back. "
        "Changes are saved into data/overall_state.json."
    )

    if "overall_edit_df" not in st.session_state:
        st.session_state["overall_edit_df"] = load_overall_state()

    current_df = normalize_overall_df(st.session_state["overall_edit_df"])

    download_col, upload_col = st.columns([1, 2])
    with download_col:
        st.download_button(
            "Download Current Editable Table",
            data=_excel_bytes(current_df, "Overall"),
            file_name="overall_editable_table.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="overall_download_current_table",
        )

    with upload_col:
        uploaded_file = st.file_uploader(
            "Upload Edited Table",
            type=["xlsx", "xls", "csv"],
            key="overall_upload_file",
        )

    if uploaded_file is not None:
        upload_key = f"{uploaded_file.name}_{uploaded_file.size}"
        if st.session_state.get("overall_last_upload_key") != upload_key:
            try:
                uploaded_df = normalize_overall_df(_read_upload(uploaded_file))
                st.session_state["overall_edit_df"] = uploaded_df.copy()
                st.session_state["overall_last_upload_key"] = upload_key
                st.session_state.pop("overall_data_editor", None)
                st.success("Uploaded table loaded. Review it below, then click Save Uploaded Changes.")
                st.rerun()
            except Exception as e:
                st.error(f"Upload failed: {e}")

    editable_df = normalize_overall_df(st.session_state["overall_edit_df"])

    st.info(
        "Saved columns: 柜量, 常规价, 促销价, 大促价, 常规%, 促销%, 大促%, "
        "A品牌, A型号, A常规, A促销, B品牌, B型号, B常规, B促销"
    )

    editable_columns = [
        "柜量", "常规价", "促销价", "大促价", "常规%", "促销%", "大促%",
        "A品牌", "A型号", "A常规", "A促销",
        "B品牌", "B型号", "B常规", "B促销",
    ]

    disabled_cols = [c for c in editable_df.columns if c not in editable_columns]

    column_config = {
        "客户型号": st.column_config.TextColumn("客户型号", width="large", disabled=True),
        "产品线": st.column_config.TextColumn("产品线", width="medium", disabled=True),
        "品类": st.column_config.TextColumn("品类", width="medium", disabled=True),
        "成本月份": st.column_config.TextColumn("成本月份", width="small", disabled=True),
        "柜量": st.column_config.NumberColumn("柜量", width="small", step=1),
        "常规价": st.column_config.NumberColumn("常规价", width="small", step=1),
        "促销价": st.column_config.NumberColumn("促销价", width="small", step=1),
        "大促价": st.column_config.NumberColumn("大促价", width="small", step=1),
        "常规%": st.column_config.NumberColumn("常规%", width="small", step=1),
        "促销%": st.column_config.NumberColumn("促销%", width="small", step=1),
        "大促%": st.column_config.NumberColumn("大促%", width="small", step=1),
        "A品牌": st.column_config.TextColumn("A品牌", width="medium"),
        "A型号": st.column_config.TextColumn("A型号", width="large"),
        "A常规": st.column_config.NumberColumn("A常规", width="small", step=1),
        "A促销": st.column_config.NumberColumn("A促销", width="small", step=1),
        "B品牌": st.column_config.TextColumn("B品牌", width="medium"),
        "B型号": st.column_config.TextColumn("B型号", width="large"),
        "B常规": st.column_config.NumberColumn("B常规", width="small", step=1),
        "B促销": st.column_config.NumberColumn("B促销", width="small", step=1),
    }

    # Important: st.form prevents rerun on every single cell edit, so the row position is kept while editing.
    with st.form("overall_batch_edit_form", clear_on_submit=False):
        edited_df = st.data_editor(
            editable_df,
            key="overall_data_editor",
            use_container_width=True,
            height=520,
            hide_index=True,
            num_rows="fixed",
            disabled=disabled_cols,
            column_config=column_config,
        )
        save_clicked = st.form_submit_button("Save Uploaded Changes", use_container_width=True)

    if save_clicked:
        edited_df = normalize_overall_df(edited_df)
        st.session_state["overall_edit_df"] = edited_df.copy()
        _save_json_df(edited_df, OVERALL_STATE_PATH)
        st.success("Changes saved successfully.")
        st.rerun()

    return normalize_overall_df(st.session_state["overall_edit_df"])


def render_results(df: pd.DataFrame, params: Dict[str, float]) -> None:
    result = calculate_overall(df, params)

    overall_cols = [
        "客户型号", "产品线", "品类", "柜量", "毛利率", "净利率",
        "常规价", "促销价", "大促价", "NETNET", "到岸成本", "变动费用", "总成本", "FOB(AUD)", "综合运费",
    ]
    competitor_cols = [
        "客户型号", "A品牌", "A型号", "A常规", "A促销", "B品牌", "B型号", "B常规", "B促销",
    ]

    left, right = st.columns([1.4, 1])
    with left:
        st.markdown("#### Overall Result")
        show_cols = [c for c in overall_cols if c in result.columns]
        st.dataframe(
            result[show_cols],
            use_container_width=True,
            hide_index=True,
            height=420,
            column_config={
                "客户型号": st.column_config.TextColumn("客户型号", width="large"),
                "毛利率": st.column_config.NumberColumn("毛利率", format="%.1f%%"),
                "净利率": st.column_config.NumberColumn("净利率", format="%.1f%%"),
                "常规价": st.column_config.NumberColumn("常规价", format="$ %.0f"),
                "促销价": st.column_config.NumberColumn("促销价", format="$ %.0f"),
                "大促价": st.column_config.NumberColumn("大促价", format="$ %.0f"),
                "NETNET": st.column_config.NumberColumn("NETNET", format="%.0f"),
                "到岸成本": st.column_config.NumberColumn("到岸成本", format="%.0f"),
                "变动费用": st.column_config.NumberColumn("变动费用", format="%.0f"),
                "总成本": st.column_config.NumberColumn("总成本", format="%.0f"),
                "FOB(AUD)": st.column_config.NumberColumn("FOB(AUD)", format="%.0f"),
                "综合运费": st.column_config.NumberColumn("综合运费", format="%.0f"),
            },
        )

    with right:
        st.markdown("#### Competitor Table")
        show_cols = [c for c in competitor_cols if c in result.columns]
        st.dataframe(
            result[show_cols],
            use_container_width=True,
            hide_index=True,
            height=420,
            column_config={
                "客户型号": st.column_config.TextColumn("客户型号", width="large"),
                "A型号": st.column_config.TextColumn("A型号", width="large"),
                "B型号": st.column_config.TextColumn("B型号", width="large"),
            },
        )


def run() -> None:
    st.markdown("## Overall")
    render_global_params_sidebar()
    params = get_params_from_session()
    edited_df = render_batch_editor()
    st.divider()
    render_results(edited_df, params)


if __name__ == "__main__":
    st.set_page_config(page_title="Overall", layout="wide")
    run()
