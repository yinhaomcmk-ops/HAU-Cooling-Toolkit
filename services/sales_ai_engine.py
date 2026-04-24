from __future__ import annotations

import json
import os
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st


# =========================
# Paths / DB
# =========================

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "app_data.db"


# =========================
# Formatting helpers
# =========================

def _fmt(n: float) -> str:
    if pd.isna(n):
        return "-"
    return f"{n:,.2f}"


def _fmt_int(n: float) -> str:
    if pd.isna(n):
        return "-"
    return f"{n:,.0f}"


def _fmt_pct(n: float) -> str:
    if pd.isna(n):
        return "-"
    return f"{n:+.1%}"


def _safe_div(n: float, d: float) -> float:
    if d is None or pd.isna(d) or d == 0:
        return float("nan")
    return n / d


def _to_json_safe(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str, indent=2)


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [] if value == "All" else [value]
    if isinstance(value, (list, tuple, set)):
        return [str(v) for v in value if str(v) != "All"]
    return [str(value)]


# =========================
# OpenAI config
# =========================

def get_openai_api_key() -> str | None:
    try:
        key = st.secrets.get("OPENAI_API_KEY")
        if key:
            return str(key)
    except Exception:
        pass
    return os.getenv("OPENAI_API_KEY")


def get_openai_model(default: str = "gpt-4.1-mini") -> str:
    try:
        model = st.secrets.get("OPENAI_MODEL")
        if model:
            return str(model)
    except Exception:
        pass
    return os.getenv("OPENAI_MODEL", default)


def is_openai_ready() -> bool:
    return bool(get_openai_api_key())


# =========================
# Filter helpers
# =========================

def _period_label(start_date: date | None, end_date: date | None) -> str:
    if not start_date or not end_date:
        return "Current filtered period"
    if start_date == end_date:
        return str(start_date)
    return f"{start_date} → {end_date}"


def _same_period_last_year(start_date: date, end_date: date) -> tuple[date, date]:
    try:
        return start_date.replace(year=start_date.year - 1), end_date.replace(year=end_date.year - 1)
    except ValueError:
        return start_date - timedelta(days=365), end_date - timedelta(days=365)


def _previous_period(start_date: date, end_date: date) -> tuple[date, date]:
    days = max((end_date - start_date).days + 1, 1)
    prev_end = start_date - timedelta(days=1)
    prev_start = prev_end - timedelta(days=days - 1)
    return prev_start, prev_end


def filter_base_scope(
    df: pd.DataFrame,
    channels: list[str] | str | None = None,
    product_lines: list[str] | str | None = None,
    categories: list[str] | str | None = None,
    models: list[str] | str | None = None,
    # Backward compatible names
    channel: str = "All",
    product_line: str = "All",
    category: str = "All",
    model: str = "All",
) -> pd.DataFrame:
    """Apply non-date filters. Year/week are excluded so WoW/YoY still work."""
    out = df.copy()
    chs = _as_list(channels) or _as_list(channel)
    pls = _as_list(product_lines) or _as_list(product_line)
    cats = _as_list(categories) or _as_list(category)
    mods = _as_list(models) or _as_list(model)

    if chs:
        out = out[out["channel"].astype(str).isin(chs)]
    if pls:
        out = out[out["product_line"].astype(str).isin(pls)]
    if cats:
        out = out[out["category"].astype(str).isin(cats)]
    if mods:
        out = out[out["model"].astype(str).isin(mods)]
    return out


def filter_period(df: pd.DataFrame, start_date: date, end_date: date) -> pd.DataFrame:
    return df[
        (df["sales_date"].dt.date >= start_date)
        & (df["sales_date"].dt.date <= end_date)
    ].copy()


def apply_year_week_filters(
    df: pd.DataFrame,
    years: list[int] | int | str | None = None,
    weeks: list[int] | int | str | None = None,
    year: str | int = "All",
    week: str | int = "All",
) -> pd.DataFrame:
    out = df.copy()
    ys = _as_list(years) or _as_list(year)
    ws = _as_list(weeks) or _as_list(week)
    if ys:
        out = out[out["year"].astype(int).isin([int(y) for y in ys])]
    if ws:
        out = out[out["week"].astype(int).isin([int(w) for w in ws])]
    return out


# =========================
# Diagnostic table builders
# =========================

def _agg_period(df: pd.DataFrame, group_cols: list[str], prefix: str, weeks_count: float) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=group_cols)

    out = (
        df.groupby(group_cols, dropna=True)
        .agg(
            sales_qty=("sales_qty", "sum"),
            sales_value=("sales_value_est", "sum"),
            avg_price=("price", "mean"),
            avg_soh=("sum_avl_soh", "mean"),
            avg_soo=("sum_soo", "mean"),
        )
        .reset_index()
    )

    weekly_sales = out["sales_qty"] / max(weeks_count, 1)
    out["wos"] = out["avg_soh"] / weekly_sales.replace(0, pd.NA)
    out["sell_through"] = out["sales_qty"] / (out["sales_qty"] + out["avg_soh"])

    rename_map = {
        "sales_qty": f"{prefix}_qty",
        "sales_value": f"{prefix}_value",
        "avg_price": f"{prefix}_asp",
        "avg_soh": f"{prefix}_soh",
        "avg_soo": f"{prefix}_soo",
        "wos": f"{prefix}_wos",
        "sell_through": f"{prefix}_sell_through",
    }
    return out.rename(columns=rename_map)


def _join_periods(ty: pd.DataFrame, wow: pd.DataFrame, ly: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    out = ty.merge(wow, on=group_cols, how="outer").merge(ly, on=group_cols, how="outer")
    numeric_cols = [c for c in out.columns if c not in group_cols]
    out[numeric_cols] = out[numeric_cols].fillna(0)

    out["wow_qty_gap"] = out["ty_qty"] - out["wow_qty"]
    out["yoy_qty_gap"] = out["ty_qty"] - out["ly_qty"]
    out["wow_value_gap"] = out["ty_value"] - out["wow_value"]
    out["yoy_value_gap"] = out["ty_value"] - out["ly_value"]

    out["qty_wow"] = out.apply(lambda r: _safe_div(r["wow_qty_gap"], r["wow_qty"]), axis=1)
    out["qty_yoy"] = out.apply(lambda r: _safe_div(r["yoy_qty_gap"], r["ly_qty"]), axis=1)
    out["value_wow"] = out.apply(lambda r: _safe_div(r["wow_value_gap"], r["wow_value"]), axis=1)
    out["value_yoy"] = out.apply(lambda r: _safe_div(r["yoy_value_gap"], r["ly_value"]), axis=1)

    out["wow_asp_gap"] = out["ty_asp"] - out["wow_asp"]
    out["yoy_asp_gap"] = out["ty_asp"] - out["ly_asp"]
    out["asp_wow"] = out.apply(lambda r: _safe_div(r["wow_asp_gap"], r["wow_asp"]), axis=1)
    out["asp_yoy"] = out.apply(lambda r: _safe_div(r["yoy_asp_gap"], r["ly_asp"]), axis=1)

    total_yoy_decline = abs(out.loc[out["yoy_qty_gap"] < 0, "yoy_qty_gap"].sum())
    total_wow_decline = abs(out.loc[out["wow_qty_gap"] < 0, "wow_qty_gap"].sum())
    out["decline_impact_yoy"] = out["yoy_qty_gap"].apply(
        lambda x: abs(x) / total_yoy_decline if x < 0 and total_yoy_decline > 0 else 0
    )
    out["decline_impact_wow"] = out["wow_qty_gap"].apply(
        lambda x: abs(x) / total_wow_decline if x < 0 and total_wow_decline > 0 else 0
    )
    out["combined_risk_score"] = (
        out["decline_impact_yoy"].fillna(0) * 0.55
        + out["decline_impact_wow"].fillna(0) * 0.35
        + (out["asp_yoy"].fillna(0).lt(0).astype(float) * 0.10)
    )
    return out


def build_diagnostic_tables(
    base_scope_df: pd.DataFrame,
    filtered_current: pd.DataFrame,
    start_date: date,
    end_date: date,
) -> dict[str, pd.DataFrame]:
    """Build Channel / Category / Model tables with TY vs WoW and YoY."""
    if filtered_current.empty:
        empty = pd.DataFrame()
        return {"channel": empty, "category": empty, "model": empty, "raw_channel": empty, "raw_category": empty, "raw_model": empty}

    ly_start, ly_end = _same_period_last_year(start_date, end_date)
    wow_start, wow_end = _previous_period(start_date, end_date)
    ly_df = filter_period(base_scope_df, ly_start, ly_end)
    wow_df = filter_period(base_scope_df, wow_start, wow_end)
    weeks_count = max(((end_date - start_date).days + 1) / 7, 1)

    channel = _join_periods(
        _agg_period(filtered_current, ["channel"], "ty", weeks_count),
        _agg_period(wow_df, ["channel"], "wow", weeks_count),
        _agg_period(ly_df, ["channel"], "ly", weeks_count),
        ["channel"],
    )
    channel["issue"] = channel.apply(_channel_issue, axis=1)
    channel["suggested_action"] = channel.apply(_channel_action, axis=1)
    channel = channel.sort_values(["combined_risk_score", "yoy_qty_gap", "wow_qty_gap"], ascending=[False, True, True])

    category = _join_periods(
        _agg_period(filtered_current, ["category"], "ty", weeks_count),
        _agg_period(wow_df, ["category"], "wow", weeks_count),
        _agg_period(ly_df, ["category"], "ly", weeks_count),
        ["category"],
    )
    category["issue"] = category.apply(_category_issue, axis=1)
    category["suggested_action"] = category.apply(_category_action, axis=1)
    category = category.sort_values(["combined_risk_score", "yoy_qty_gap", "wow_qty_gap"], ascending=[False, True, True])

    model = _join_periods(
        _agg_period(filtered_current, ["category", "model", "channel"], "ty", weeks_count),
        _agg_period(wow_df, ["category", "model", "channel"], "wow", weeks_count),
        _agg_period(ly_df, ["category", "model", "channel"], "ly", weeks_count),
        ["category", "model", "channel"],
    )
    model["issue"] = model.apply(_model_issue, axis=1)
    model["suggested_action"] = model.apply(_model_action, axis=1)
    model = model.sort_values(["combined_risk_score", "yoy_qty_gap", "wow_qty_gap"], ascending=[False, True, True])

    return {
        "channel": _format_table(channel, "channel"),
        "category": _format_table(category, "category"),
        "model": _format_table(model, "model"),
        "raw_channel": channel,
        "raw_category": category,
        "raw_model": model,
        "periods": pd.DataFrame([
            {"period": "Current", "start": start_date, "end": end_date},
            {"period": "Previous period / WoW", "start": wow_start, "end": wow_end},
            {"period": "Same period last year / YoY", "start": ly_start, "end": ly_end},
        ]),
    }


def _channel_issue(row: pd.Series) -> str:
    yoy_down = row.get("yoy_qty_gap", 0) < 0
    wow_down = row.get("wow_qty_gap", 0) < 0
    asp_down = row.get("yoy_asp_gap", 0) < 0 or row.get("wow_asp_gap", 0) < 0
    high_wos = row.get("ty_wos", 0) > 8
    if yoy_down and wow_down and high_wos:
        return "YoY and WoW decline with slow turnover"
    if yoy_down and wow_down:
        return "YoY and WoW volume decline"
    if yoy_down and asp_down:
        return "YoY volume decline with ASP pressure"
    if yoy_down:
        return "YoY volume decline"
    if wow_down:
        return "WoW momentum decline"
    if asp_down:
        return "ASP pressure"
    return "Stable / positive"


def _channel_action(row: pd.Series) -> str:
    if row.get("ty_wos", 0) > 8 and row.get("yoy_qty_gap", 0) < 0:
        return "Prioritise sell-through plan, store execution, and stock balancing."
    if row.get("yoy_qty_gap", 0) < 0 and row.get("wow_qty_gap", 0) < 0:
        return "Immediate account recovery: check ranging, display, price position, and stock availability."
    if row.get("yoy_qty_gap", 0) < 0:
        return "Find missing hero SKUs and recover channel mix."
    if row.get("wow_qty_gap", 0) < 0:
        return "Protect weekly run-rate with short-term promo / field focus."
    return "Maintain momentum and protect ASP."


def _category_issue(row: pd.Series) -> str:
    if row.get("yoy_qty_gap", 0) < 0 and row.get("wow_qty_gap", 0) < 0 and row.get("asp_yoy", 0) < 0:
        return "Category declined with ASP erosion"
    if row.get("yoy_qty_gap", 0) < 0 and row.get("wow_qty_gap", 0) < 0:
        return "Category declining on YoY and WoW"
    if row.get("yoy_qty_gap", 0) < 0:
        return "Category YoY decline"
    if row.get("wow_qty_gap", 0) < 0:
        return "Category WoW softness"
    if row.get("asp_yoy", 0) < 0:
        return "Category ASP softened"
    return "Stable / positive"


def _category_action(row: pd.Series) -> str:
    if row.get("yoy_qty_gap", 0) < 0 and row.get("asp_yoy", 0) < 0:
        return "Review competitor pricing and rebuild price ladder / hero offer."
    if row.get("yoy_qty_gap", 0) < 0:
        return "Drill into model/channel drag and create targeted recovery plan."
    if row.get("wow_qty_gap", 0) < 0:
        return "Check weekly promo execution and channel sell-through."
    if row.get("asp_yoy", 0) < 0:
        return "Protect premium mix and reduce unnecessary discount depth."
    return "Use as growth pool and expand best-performing models."


def _model_issue(row: pd.Series) -> str:
    if row.get("decline_impact_yoy", 0) >= 0.15 or row.get("decline_impact_wow", 0) >= 0.15:
        return "Major drag item"
    if row.get("yoy_qty_gap", 0) < 0 and row.get("wow_qty_gap", 0) < 0:
        return "Declining model on YoY and WoW"
    if row.get("yoy_qty_gap", 0) < 0:
        return "YoY declining model"
    if row.get("wow_qty_gap", 0) < 0:
        return "WoW declining model"
    if row.get("asp_yoy", 0) < 0:
        return "ASP dilution"
    return "Stable / positive"


def _model_action(row: pd.Series) -> str:
    if row.get("decline_impact_yoy", 0) >= 0.15 or row.get("decline_impact_wow", 0) >= 0.15:
        return "Priority recovery SKU: check stock, display, promo, price, and account execution."
    if row.get("yoy_qty_gap", 0) < 0 or row.get("wow_qty_gap", 0) < 0:
        return "Review retailer execution and compare price against key competing models."
    if row.get("asp_yoy", 0) < 0:
        return "Check whether lower ASP is generating enough incremental volume."
    return "Keep supporting if margin and stock position are healthy."


def _format_table(df: pd.DataFrame, table_type: str) -> pd.DataFrame:
    if df.empty:
        return df

    common_rename = {
        "ty_qty": "TY Sales",
        "wow_qty": "LW Sales",
        "ly_qty": "LY Sales",
        "wow_qty_gap": "WoW Gap",
        "yoy_qty_gap": "YoY Gap",
        "qty_wow": "Qty WoW %",
        "qty_yoy": "Qty YoY %",
        "ty_value": "TY Value",
        "wow_value": "LW Value",
        "ly_value": "LY Value",
        "value_wow": "Value WoW %",
        "value_yoy": "Value YoY %",
        "ty_asp": "TY ASP",
        "wow_asp": "LW ASP",
        "ly_asp": "LY ASP",
        "asp_wow": "ASP WoW %",
        "asp_yoy": "ASP YoY %",
        "ty_wos": "WoS",
        "ty_sell_through": "Sell-through",
        "decline_impact_yoy": "YoY Impact %",
        "decline_impact_wow": "WoW Impact %",
        "issue": "Main Issue",
        "suggested_action": "Suggested Action",
    }

    if table_type == "channel":
        cols = [
            "channel", "ty_qty", "wow_qty", "ly_qty", "wow_qty_gap", "yoy_qty_gap",
            "qty_wow", "qty_yoy", "ty_asp", "asp_wow", "asp_yoy", "ty_wos",
            "ty_sell_through", "issue", "suggested_action",
        ]
        renamed_first = {"channel": "Retailer / Channel"}
    elif table_type == "category":
        cols = [
            "category", "ty_qty", "wow_qty", "ly_qty", "wow_qty_gap", "yoy_qty_gap",
            "qty_wow", "qty_yoy", "ty_asp", "asp_wow", "asp_yoy", "decline_impact_yoy",
            "issue", "suggested_action",
        ]
        renamed_first = {"category": "Category"}
    else:
        cols = [
            "category", "model", "channel", "ty_qty", "wow_qty", "ly_qty",
            "wow_qty_gap", "yoy_qty_gap", "qty_wow", "qty_yoy", "ty_asp",
            "asp_wow", "asp_yoy", "decline_impact_yoy", "decline_impact_wow",
            "issue", "suggested_action",
        ]
        renamed_first = {"category": "Category", "model": "Model", "channel": "Retailer / Channel"}

    out = df[[c for c in cols if c in df.columns]].copy()
    out = out.rename(columns={**renamed_first, **common_rename})

    for col in ["Qty WoW %", "Qty YoY %", "Value WoW %", "Value YoY %", "ASP WoW %", "ASP YoY %", "Sell-through", "YoY Impact %", "WoW Impact %"]:
        if col in out.columns:
            out[col] = out[col].apply(_fmt_pct)
    for col in ["TY Sales", "LW Sales", "LY Sales", "WoW Gap", "YoY Gap"]:
        if col in out.columns:
            out[col] = out[col].apply(_fmt_int)
    for col in ["TY ASP", "LW ASP", "LY ASP", "WoS"]:
        if col in out.columns:
            out[col] = out[col].apply(_fmt)
    return out


# =========================
# Cross-module context
# =========================

def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)).fetchone()
    return row is not None


def _read_table_if_exists(conn: sqlite3.Connection, table_name: str, limit: int = 50000) -> pd.DataFrame:
    if not _table_exists(conn, table_name):
        return pd.DataFrame()
    try:
        return pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT {int(limit)}", conn)
    except Exception:
        return pd.DataFrame()


def _find_col(cols: list[str], candidates: list[str]) -> str | None:
    lower = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    for c in cols:
        lc = c.lower().replace(" ", "_")
        for cand in candidates:
            if cand.lower().replace(" ", "_") in lc:
                return c
    return None


def load_heatmap_context(filters: dict[str, Any] | None = None, limit: int = 20) -> dict[str, Any]:
    """Best-effort summary from Sales Heatmap DB tables. Safe if tables do not exist."""
    if not DB_PATH.exists():
        return {"available": False, "reason": "Database not found."}

    possible_tables = [
        "heatmap_sales_records", "sales_heatmap_records", "heatmap_records",
        "store_sales_records", "sales_records", "store_locations", "stores",
    ]
    try:
        with sqlite3.connect(DB_PATH) as conn:
            tables = [t for t in possible_tables if _table_exists(conn, t)]
            if not tables:
                return {"available": False, "reason": "No known heatmap tables found."}
            summaries = []
            for table in tables:
                df = _read_table_if_exists(conn, table)
                if df.empty:
                    continue
                cols = df.columns.tolist()
                store_col = _find_col(cols, ["store", "store_name", "business_name", "name"])
                suburb_col = _find_col(cols, ["suburb", "city", "postcode"])
                region_col = _find_col(cols, ["region", "state", "area"])
                retailer_col = _find_col(cols, ["retailer", "channel", "banner", "account"])
                qty_col = _find_col(cols, ["sales_qty", "qty", "units", "sales", "volume", "sumdailysales_qty"])
                value_col = _find_col(cols, ["sales_value", "value", "amount", "revenue"])
                if qty_col:
                    group_cols = [c for c in [retailer_col, region_col, suburb_col, store_col] if c]
                    if group_cols:
                        tmp = (
                            df.groupby(group_cols, dropna=True)[qty_col]
                            .sum()
                            .sort_values(ascending=False)
                            .head(limit)
                            .reset_index()
                        )
                        summaries.append({
                            "table": table,
                            "metric": qty_col,
                            "top_locations": tmp.to_dict("records"),
                            "total_volume": float(pd.to_numeric(df[qty_col], errors="coerce").fillna(0).sum()),
                        })
                elif value_col:
                    group_cols = [c for c in [retailer_col, region_col, suburb_col, store_col] if c]
                    if group_cols:
                        tmp = (
                            df.groupby(group_cols, dropna=True)[value_col]
                            .sum()
                            .sort_values(ascending=False)
                            .head(limit)
                            .reset_index()
                        )
                        summaries.append({"table": table, "metric": value_col, "top_locations": tmp.to_dict("records")})
            return {"available": bool(summaries), "tables_found": tables, "summaries": summaries[:3]}
    except Exception as exc:
        return {"available": False, "reason": str(exc)}


def load_value_chain_context(filters: dict[str, Any] | None = None, limit: int = 50) -> dict[str, Any]:
    """Best-effort value-chain profitability context from existing DB tables."""
    if not DB_PATH.exists():
        return {"available": False, "reason": "Database not found."}

    possible_tables = [
        "model_master", "cost_master", "value_chain_records", "value_chain_overall",
        "pricing_records", "competitor_records", "vc_overall_records",
    ]
    model_filter = set(_as_list((filters or {}).get("Models")))
    category_filter = set(_as_list((filters or {}).get("Categories")))

    try:
        with sqlite3.connect(DB_PATH) as conn:
            tables = [t for t in possible_tables if _table_exists(conn, t)]
            if not tables:
                return {"available": False, "reason": "No known value-chain tables found."}

            summaries = []
            for table in tables:
                df = _read_table_if_exists(conn, table, limit=100000)
                if df.empty:
                    continue
                cols = df.columns.tolist()
                model_col = _find_col(cols, ["model", "model_id", "hau_model", "sku"])
                cat_col = _find_col(cols, ["category", "product_category"])
                price_col = _find_col(cols, ["price", "regular_price", "promo_price", "selling_price"])
                cost_col = _find_col(cols, ["cost", "upcost", "landed_cost", "latest_cost"])
                gm_col = _find_col(cols, ["gross_margin", "gm", "margin", "gross_margin_pct"])
                net_col = _find_col(cols, ["net_profit", "net_margin", "np", "profit"])

                tmp = df.copy()
                if model_filter and model_col:
                    tmp = tmp[tmp[model_col].astype(str).str.upper().isin({m.upper() for m in model_filter})]
                if category_filter and cat_col:
                    tmp = tmp[tmp[cat_col].astype(str).isin(category_filter)]
                if tmp.empty:
                    continue

                keep = [c for c in [model_col, cat_col, price_col, cost_col, gm_col, net_col] if c]
                sample = tmp[keep].head(limit).to_dict("records") if keep else []

                summary: dict[str, Any] = {"table": table, "rows": int(len(tmp)), "sample": sample}
                if gm_col:
                    summary["average_gross_margin"] = float(pd.to_numeric(tmp[gm_col], errors="coerce").mean())
                if net_col:
                    summary["average_net_profit_or_margin"] = float(pd.to_numeric(tmp[net_col], errors="coerce").mean())
                if cost_col:
                    summary["average_cost"] = float(pd.to_numeric(tmp[cost_col], errors="coerce").mean())
                summaries.append(summary)

            return {"available": bool(summaries), "tables_found": tables, "summaries": summaries[:5]}
    except Exception as exc:
        return {"available": False, "reason": str(exc)}


# =========================
# AI context and prompts
# =========================

def build_analysis_context(
    filtered_current: pd.DataFrame,
    diagnostic_tables: dict[str, pd.DataFrame],
    start_date: date | None = None,
    end_date: date | None = None,
    filters: dict[str, Any] | None = None,
    include_heatmap: bool = False,
    include_value_chain: bool = False,
) -> dict[str, Any]:
    if filtered_current.empty:
        return {"error": "No data under current filters."}

    total_qty = float(filtered_current["sales_qty"].sum())
    total_value = float(filtered_current["sales_value_est"].sum())
    avg_price = float(filtered_current["price"].mean()) if "price" in filtered_current else float("nan")
    avg_soh = float(filtered_current["sum_avl_soh"].mean()) if "sum_avl_soh" in filtered_current else float("nan")

    context: dict[str, Any] = {
        "period": _period_label(start_date, end_date),
        "filters": filters or {},
        "overall": {
            "total_sales_volume": total_qty,
            "estimated_sales_value": total_value,
            "average_price": avg_price,
            "average_soh": avg_soh,
            "rows": int(len(filtered_current)),
            "channels": int(filtered_current["channel"].nunique(dropna=True)),
            "categories": int(filtered_current["category"].nunique(dropna=True)),
            "models": int(filtered_current["model"].nunique(dropna=True)),
        },
        "period_comparison_definition": diagnostic_tables.get("periods", pd.DataFrame()).to_dict("records"),
        "channel_diagnosis": diagnostic_tables.get("channel", pd.DataFrame()).head(15).to_dict("records"),
        "category_diagnosis": diagnostic_tables.get("category", pd.DataFrame()).head(15).to_dict("records"),
        "model_impact": diagnostic_tables.get("model", pd.DataFrame()).head(30).to_dict("records"),
        "top_models": (
            filtered_current.groupby(["category", "model"], dropna=True)
            .agg(sales_qty=("sales_qty", "sum"), avg_price=("price", "mean"), sales_value=("sales_value_est", "sum"))
            .sort_values("sales_qty", ascending=False)
            .head(25)
            .reset_index()
            .to_dict("records")
        ),
    }

    if include_heatmap:
        context["sales_heatmap_context"] = load_heatmap_context(filters)
    if include_value_chain:
        context["value_chain_profitability_context"] = load_value_chain_context(filters)
    return context


def _local_fallback_summary(context: dict[str, Any]) -> str:
    if "error" in context:
        return context["error"]

    overall = context["overall"]
    channel = context.get("channel_diagnosis", [])[:8]
    category = context.get("category_diagnosis", [])[:8]
    model = context.get("model_impact", [])[:10]

    lines = [
        "# AI Summary",
        f"**Period:** {context.get('period', '-')}",
        f"**Total sales volume:** {_fmt_int(overall.get('total_sales_volume'))} units  ",
        f"**Estimated sales value:** {_fmt(overall.get('estimated_sales_value'))}  ",
        f"**Average price:** {_fmt(overall.get('average_price'))}",
        "",
        "## 1) Channel Diagnosis",
    ]
    if channel:
        for r in channel:
            lines.append(
                f"- **{r.get('Retailer / Channel', '-')}** — TY {r.get('TY Sales', '-')}, WoW {r.get('Qty WoW %', '-')}, YoY {r.get('Qty YoY %', '-')}, ASP YoY {r.get('ASP YoY %', '-')}. "
                f"Issue: {r.get('Main Issue', '-')}. Action: {r.get('Suggested Action', '-')}"
            )
    else:
        lines.append("- No channel diagnosis available.")

    lines.append("\n## 2) Category Diagnosis")
    if category:
        for r in category:
            lines.append(
                f"- **{r.get('Category', '-')}** — TY {r.get('TY Sales', '-')}, WoW {r.get('Qty WoW %', '-')}, YoY {r.get('Qty YoY %', '-')}, ASP YoY {r.get('ASP YoY %', '-')}. "
                f"Issue: {r.get('Main Issue', '-')}. Action: {r.get('Suggested Action', '-')}"
            )
    else:
        lines.append("- No category diagnosis available.")

    lines.append("\n## 3) Key Model Impact")
    if model:
        for r in model:
            lines.append(
                f"- **{r.get('Model', '-')} / {r.get('Retailer / Channel', '-')} / {r.get('Category', '-')}** — WoW Gap {r.get('WoW Gap', '-')}, YoY Gap {r.get('YoY Gap', '-')}, "
                f"YoY Impact {r.get('YoY Impact %', '-')}. Action: {r.get('Suggested Action', '-')}"
            )
    else:
        lines.append("- No model impact available.")

    lines.append("\n## 4) Recommended Commercial Actions")
    lines.append("- Prioritise channels/categories with both WoW and YoY decline.")
    lines.append("- Check stock/display/price execution for top drag models before broad discounting.")
    lines.append("- Use heatmap and profitability context in Q&A to decide where to push volume without damaging margin.")
    return "\n".join(lines)


def call_openai_sales_agent(
    prompt: str,
    context: dict[str, Any],
    model: str | None = None,
    temperature: float = 0.2,
) -> str:
    api_key = get_openai_api_key()
    if not api_key:
        return _local_fallback_summary(context)

    try:
        from openai import OpenAI
    except Exception as exc:
        return (
            "### OpenAI package is not installed\n\n"
            "Please run `pip install openai` in your environment.\n\n"
            f"Error detail: `{exc}`"
        )

    client = OpenAI(api_key=api_key)
    selected_model = model or get_openai_model()

    system_instructions = """
You are a senior sales strategy analyst for Hisense Australia Cooling business.
Analyze the provided filtered sellout context only. Do not invent unavailable facts.
Your default analytical priority is: last week performance, WoW movement, YoY movement, ASP trend, stock/turnover risk, channel execution, category drag, and model-level impact.
When heatmap context is available, use it to explain where sales are geographically concentrated or weak, and suggest store/region focus.
When value-chain profitability context is available, use it to balance sales recovery with gross margin / net profit risk.
Be commercial, concise, and action-oriented. Use headings and short tables where helpful.
If the user asks in Chinese, answer in Chinese. Otherwise use business English.
""".strip()

    user_input = f"""
Current filtered sales context JSON:
{_to_json_safe(context)}

User request:
{prompt}
""".strip()

    try:
        response = client.responses.create(
            model=selected_model,
            instructions=system_instructions,
            input=user_input,
            temperature=temperature,
        )
        return response.output_text
    except Exception as exc:
        return (
            "### OpenAI analysis failed\n\n"
            "The local rule-based summary is shown below instead.\n\n"
            f"Error detail: `{exc}`\n\n"
            + _local_fallback_summary(context)
        )


def build_summary(
    filtered_current: pd.DataFrame,
    diagnostic_tables: dict[str, pd.DataFrame] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    filters: dict[str, Any] | None = None,
    use_openai: bool = True,
    model: str | None = None,
    include_heatmap: bool = False,
    include_value_chain: bool = False,
) -> str:
    if filtered_current.empty:
        return "No data is available under the current filters."

    diagnostic_tables = diagnostic_tables or {"channel": pd.DataFrame(), "category": pd.DataFrame(), "model": pd.DataFrame()}
    context = build_analysis_context(
        filtered_current,
        diagnostic_tables,
        start_date,
        end_date,
        filters,
        include_heatmap=include_heatmap,
        include_value_chain=include_value_chain,
    )

    prompt = """
Create a full-page executive Sales Agent summary for last week / current selected period.
Must include these sections:
1) Executive headline: what happened, main risk, biggest opportunity.
2) Channel Diagnosis: table-style bullets showing which channels declined WoW and YoY, turnover/WoS, ASP trend, and action.
3) Category Diagnosis: which categories declined WoW/YoY and likely commercial reason.
4) Key Model Impact: which products within the weak categories caused the biggest decline, by channel where possible.
5) Sales Actions: specific next-week actions, separating volume recovery, ASP protection, stock/turnover, and account execution.
If heatmap context exists, add store/region distribution implications.
If value-chain context exists, add margin/profitability consideration.
""".strip()

    if use_openai and is_openai_ready():
        return call_openai_sales_agent(prompt, context, model=model)
    return _local_fallback_summary(context)


def answer_question(
    filtered_current: pd.DataFrame,
    question: str,
    diagnostic_tables: dict[str, pd.DataFrame] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    filters: dict[str, Any] | None = None,
    use_openai: bool = True,
    model: str | None = None,
    include_heatmap: bool = False,
    include_value_chain: bool = False,
) -> str:
    if filtered_current.empty:
        return "No data is available under the current filters."

    diagnostic_tables = diagnostic_tables or {"channel": pd.DataFrame(), "category": pd.DataFrame(), "model": pd.DataFrame()}
    context = build_analysis_context(
        filtered_current,
        diagnostic_tables,
        start_date,
        end_date,
        filters,
        include_heatmap=include_heatmap,
        include_value_chain=include_value_chain,
    )

    if use_openai and is_openai_ready():
        return call_openai_sales_agent(question, context, model=model)

    return (
        _local_fallback_summary(context)
        + "\n\n---\n\n"
        + "### Note\nOpenAI API key is not configured, so this is the local fallback analysis. "
        + "Add `OPENAI_API_KEY` to `.streamlit/secrets.toml` to enable free-form AI Q&A."
    )

# ============================================================
# Overrides for current shared Database schema
# ============================================================
def load_heatmap_context(filters: dict[str, Any] | None = None, limit: int = 20) -> dict[str, Any]:
    """Store distribution context from shared Store Master + Sales by Stores tables."""
    if not DB_PATH.exists():
        return {"available": False, "reason": "Database not found."}
    try:
        with sqlite3.connect(DB_PATH) as conn:
            if not (_table_exists(conn, "sales_records") and _table_exists(conn, "store_locations")):
                return {"available": False, "reason": "Sales by Stores or Store Master table not found."}
            df = pd.read_sql_query(
                """
                SELECT s.sales_date, s.business_name, s.model, s.sales,
                       l.retailer, l.region, l.latitude, l.longitude
                FROM sales_records s
                LEFT JOIN store_locations l ON s.business_name = l.business_name
                LIMIT 100000
                """,
                conn,
            )
        if df.empty:
            return {"available": False, "reason": "No Sales by Stores data."}
        model_filter = set(m.upper() for m in _as_list((filters or {}).get("Models")))
        if model_filter:
            df = df[df["model"].astype(str).str.upper().isin(model_filter)]
        if df.empty:
            return {"available": False, "reason": "No Sales by Stores data after filter."}
        df["sales"] = pd.to_numeric(df["sales"], errors="coerce").fillna(0)
        by_retailer = df.groupby("retailer", dropna=False)["sales"].sum().sort_values(ascending=False).head(limit).reset_index()
        by_region = df.groupby("region", dropna=False)["sales"].sum().sort_values(ascending=False).head(limit).reset_index()
        by_store = df.groupby(["retailer", "region", "business_name"], dropna=False)["sales"].sum().sort_values(ascending=False).head(limit).reset_index()
        unmapped_stores = int(df["latitude"].isna().sum())
        return {
            "available": True,
            "total_store_sales_volume": float(df["sales"].sum()),
            "unmapped_sales_rows": unmapped_stores,
            "top_retailers": by_retailer.to_dict("records"),
            "top_regions": by_region.to_dict("records"),
            "top_stores": by_store.to_dict("records"),
        }
    except Exception as exc:
        return {"available": False, "reason": str(exc)}


def load_value_chain_context(filters: dict[str, Any] | None = None, limit: int = 50) -> dict[str, Any]:
    """Cost and product hierarchy context from shared Product Master + EXW + Landed Cost tables."""
    if not DB_PATH.exists():
        return {"available": False, "reason": "Database not found."}
    try:
        with sqlite3.connect(DB_PATH) as conn:
            if not _table_exists(conn, "model_master"):
                return {"available": False, "reason": "Product Master table not found."}
            product = pd.read_sql_query(
                "SELECT model_id, product_line, category, hq_model, COALESCE(series, series_name, '') AS series FROM model_master",
                conn,
            )
            exw = pd.read_sql_query(
                """
                SELECT e.model_id, e.exw_cost, e.currency AS exw_currency, e.cost_month AS exw_cost_month
                FROM exw_cost e
                INNER JOIN (SELECT model_id, MAX(cost_month) AS max_month FROM exw_cost GROUP BY model_id) t
                    ON e.model_id=t.model_id AND e.cost_month=t.max_month
                INNER JOIN (SELECT model_id, cost_month, MAX(id) AS max_id FROM exw_cost GROUP BY model_id, cost_month) x
                    ON e.model_id=x.model_id AND e.cost_month=x.cost_month AND e.id=x.max_id
                """,
                conn,
            ) if _table_exists(conn, "exw_cost") else pd.DataFrame()
            landed = pd.read_sql_query(
                """
                SELECT l.model_id, l.landed_cost, l.currency AS landed_currency, l.cost_month AS landed_cost_month
                FROM landed_cost l
                INNER JOIN (SELECT model_id, MAX(cost_month) AS max_month FROM landed_cost GROUP BY model_id) t
                    ON l.model_id=t.model_id AND l.cost_month=t.max_month
                INNER JOIN (SELECT model_id, cost_month, MAX(id) AS max_id FROM landed_cost GROUP BY model_id, cost_month) x
                    ON l.model_id=x.model_id AND l.cost_month=x.cost_month AND l.id=x.max_id
                """,
                conn,
            ) if _table_exists(conn, "landed_cost") else pd.DataFrame()
        tmp = product.copy()
        if not exw.empty:
            tmp = tmp.merge(exw, on="model_id", how="left")
        if not landed.empty:
            tmp = tmp.merge(landed, on="model_id", how="left")
        model_filter = set(m.upper() for m in _as_list((filters or {}).get("Models")))
        category_filter = set(_as_list((filters or {}).get("Categories")))
        if model_filter:
            tmp = tmp[tmp["model_id"].astype(str).str.upper().isin(model_filter)]
        if category_filter:
            tmp = tmp[tmp["category"].astype(str).isin(category_filter)]
        if tmp.empty:
            return {"available": False, "reason": "No Value Chain data after filter."}
        summary = {
            "available": True,
            "product_master_rows": int(len(product)),
            "models_with_exw": int(tmp["exw_cost"].notna().sum()) if "exw_cost" in tmp else 0,
            "models_with_landed_cost": int(tmp["landed_cost"].notna().sum()) if "landed_cost" in tmp else 0,
            "average_exw_cost": float(pd.to_numeric(tmp.get("exw_cost"), errors="coerce").mean()) if "exw_cost" in tmp else None,
            "average_landed_cost": float(pd.to_numeric(tmp.get("landed_cost"), errors="coerce").mean()) if "landed_cost" in tmp else None,
            "sample_models": tmp.head(limit).to_dict("records"),
        }
        return summary
    except Exception as exc:
        return {"available": False, "reason": str(exc)}
