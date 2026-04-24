import json
from pathlib import Path

import pandas as pd
import streamlit as st

from db import init_db, get_latest_landed, get_model_master
from vc_shared import (
    PRODUCT_LINE_RULES,
    load_global_params,
    month_to_date,
    pct_to_rate,
    rate_to_pct,
    safe_float,
    safe_int,
    safe_text,
    save_global_params,
    load_page_memory,
    save_page_memory,
    get_saved_select_index,
)

init_db()

st.markdown(
    """
<style>
.block-container {
    padding-top: 1.5rem;
    padding-bottom: 1rem;
    padding-left: 2rem;
    padding-right: 2rem;
    max-width: 1800px;
    background: transparent;
}
h1 { font-size: 24px !important; }
h2 { font-size: 18px !important; }
h3 { font-size: 16px !important; }
</style>

""",
    unsafe_allow_html=True,
)

st.title('Value Chain | Landed Cost Calculator')

STATE_FILE = Path('data') / 'landed_state.json'
STATE_FILE.parent.mkdir(exist_ok=True)
DEFAULT_STATE = {'model_overrides': {}}


def load_state():
    if not STATE_FILE.exists():
        return DEFAULT_STATE.copy()
    try:
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict):
            return {'model_overrides': data.get('model_overrides', {})}
    except Exception:
        pass
    return DEFAULT_STATE.copy()


def save_state(state):
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def get_source_df():
    df = get_latest_landed()
    if df is None or df.empty:
        return pd.DataFrame()

    valid_models = set(get_model_master()['model_id'].astype(str).tolist())

    df = df.copy()
    df['model_id'] = df['model_id'].astype(str)
    df = df[df['model_id'].isin(valid_models)].copy()
    if df.empty:
        return pd.DataFrame()

    df['product_line'] = df['product_line'].fillna('')
    df['category'] = df['category'].fillna('')
    df['landed_cost'] = pd.to_numeric(df['landed_cost'], errors='coerce')
    df['cost_month_dt'] = df['cost_month'].apply(month_to_date) if 'cost_month' in df.columns else pd.NaT
    df['成本月份'] = df['cost_month_dt'].dt.strftime('%Y/%m').fillna(df.get('cost_month', '').astype(str))
    df = df.dropna(subset=['landed_cost']).reset_index(drop=True)
    df['upcost_rate'] = df['product_line'].map(lambda x: PRODUCT_LINE_RULES.get(x, {}).get('hq_upcost_rate', 0.0329))
    df['expense_rate'] = df['product_line'].map(lambda x: PRODUCT_LINE_RULES.get(x, {}).get('expense_rate', 0.29))
    df['loading_qty_default'] = df['product_line'].map(lambda x: PRODUCT_LINE_RULES.get(x, {}).get('loading_qty', 100))
    return df


def calc_metrics(landed_cost, selling_price, expense_rate, contractual_rebate_pct, other_rebate_pct, other_disc_value):
    invoice_price = selling_price / 1.1 if selling_price else 0.0
    contractual_rebate = invoice_price * (contractual_rebate_pct / 100.0)
    other_rebate = invoice_price * (other_rebate_pct / 100.0)
    net_net = invoice_price * (1 - contractual_rebate_pct / 100.0 + other_rebate_pct / 100.0) + other_disc_value
    variable_cost = net_net * expense_rate
    total_cost = landed_cost + variable_cost
    net_margin = 1 - total_cost / net_net if net_net else 0.0
    gross_margin = expense_rate + net_margin
    return {
        'Invoice Price': invoice_price,
        'Contractual Rebate': contractual_rebate,
        'Other Rebate': other_rebate,
        'HA Net Net Price': net_net,
        '到库成本': landed_cost,
        'Variable Cost': variable_cost,
        'Total Cost': total_cost,
        'Gross Margin': gross_margin * 100,
        'Net Margin': net_margin * 100,
    }


g = load_global_params()
state = load_state()
page_memory = load_page_memory('landed')
df = get_source_df()
if df.empty:
    st.warning('当前没有可用于 Landed Cost 计算的型号。请先在 VC Cost 页面维护到库成本和 VC Model 页面维护型号。')
    st.stop()

st.sidebar.header('Global Parameters')
fx_aud_to_cny = st.sidebar.number_input('1 AUD = CNY', min_value=0.0001, value=safe_float(g['fx_aud_to_cny']), format='%.4f')
singa_upcost_pct = st.sidebar.number_input('Singa Upcost (%)', min_value=0.0, value=rate_to_pct(g['singa_upcost_rate']), format='%.1f', step=0.1)
insurance_pct = st.sidebar.number_input('Insurance (%)', min_value=0.0, value=rate_to_pct(g['insurance_rate']), format='%.1f', step=0.1)
save_global_params({
    **g,
    'fx_aud_to_cny': float(fx_aud_to_cny),
    'singa_upcost_rate': pct_to_rate(singa_upcost_pct),
    'insurance_rate': pct_to_rate(insurance_pct),
})
g = load_global_params()

st.subheader('Filters')
c1, c2, c3 = st.columns([1, 1, 1.4])
product_line_options = ['All'] + sorted([x for x in df['product_line'].dropna().unique().tolist() if x])
category_options = ['All'] + sorted([x for x in df['category'].dropna().unique().tolist() if x])
with c1:
    selected_product_line = st.selectbox(
        'Product Line',
        product_line_options,
        index=get_saved_select_index(product_line_options, page_memory.get('selected_product_line', 'All')),
        key='landed_selected_product_line',
    )
with c2:
    selected_category = st.selectbox(
        'Category',
        category_options,
        index=get_saved_select_index(category_options, page_memory.get('selected_category', 'All')),
        key='landed_selected_category',
    )
filtered = df.copy()
if selected_product_line != 'All':
    filtered = filtered[filtered['product_line'] == selected_product_line]
if selected_category != 'All':
    filtered = filtered[filtered['category'] == selected_category]
model_options = ['All'] + sorted(filtered['model_id'].dropna().astype(str).unique().tolist())
with c3:
    selected_model = st.selectbox(
        'Model',
        model_options,
        index=get_saved_select_index(model_options, page_memory.get('selected_model', 'All')),
        key='landed_selected_model',
    )
if selected_model != 'All':
    filtered = filtered[filtered['model_id'].astype(str) == selected_model]
save_page_memory('landed', {
    'selected_product_line': selected_product_line,
    'selected_category': selected_category,
    'selected_model': selected_model,
})
if filtered.empty:
    st.warning('当前筛选条件下没有数据。')
    st.stop()

selected_row = filtered.iloc[0]
model_key = str(selected_row['model_id'])
overrides = state.get('model_overrides', {}).get(model_key, {})

r1, r2, r3 = st.columns(3)
with r1:
    selling_price = st.number_input('Selling Price', min_value=0.0, value=safe_float(overrides.get('selling_price', g['selling_price'])), format='%.2f')
with r2:
    expense_rate = st.number_input('Expense Rate', min_value=0.0, value=safe_float(selected_row.get('expense_rate')), format='%.4f', disabled=True)
with r3:
    upcost_rate = st.number_input('HQ Up Cost (%)', min_value=0.0, value=rate_to_pct(selected_row.get('upcost_rate')), format='%.2f', disabled=True)

b1, b2, b3 = st.columns(3)
with b1:
    contractual_rebate_pct = st.number_input('Contractual Rebate (%)', min_value=0.0, max_value=100.0, value=safe_float(overrides.get('contractual_rebate_pct', g['contractual_rebate_pct'])), format='%.1f')
with b2:
    other_rebate_pct = st.number_input('Other Rebate (%)', min_value=0.0, max_value=100.0, value=safe_float(overrides.get('other_rebate_pct', g['other_rebate_pct'])), format='%.1f')
with b3:
    other_disc_value = st.number_input('Other Disc / Incentive ($)', value=safe_float(overrides.get('other_disc_value', g['other_disc_value'])), format='%.2f', step=10.0)

metrics = calc_metrics(
    landed_cost=safe_float(selected_row['landed_cost']),
    selling_price=selling_price,
    expense_rate=expense_rate,
    contractual_rebate_pct=contractual_rebate_pct,
    other_rebate_pct=other_rebate_pct,
    other_disc_value=other_disc_value,
)

save_global_params({
    **g,
    'selling_price': float(selling_price),
    'contractual_rebate_pct': float(contractual_rebate_pct),
    'other_rebate_pct': float(other_rebate_pct),
    'other_disc_value': float(other_disc_value),
})
state.setdefault('model_overrides', {})[model_key] = {
    'selling_price': float(selling_price),
    'contractual_rebate_pct': float(contractual_rebate_pct),
    'other_rebate_pct': float(other_rebate_pct),
    'other_disc_value': float(other_disc_value),
}
save_state(state)

st.subheader('Result')
result_df = pd.DataFrame([{
    '客户型号': model_key,
    '产品线': safe_text(selected_row.get('product_line'), '-'),
    '品类': safe_text(selected_row.get('category'), '-'),
    '成本月份': safe_text(selected_row.get('成本月份'), '-'),
    '售价(AUD)': selling_price,
    '开票价': metrics['Invoice Price'],
    'NETNET': metrics['HA Net Net Price'],
    '到库成本': metrics['到库成本'],
    '变动费用': metrics['Variable Cost'],
    '合计成本': metrics['Total Cost'],
    '毛利率%': metrics['Gross Margin'] ,
    '净利率%': metrics['Net Margin'] ,
}])

st.dataframe(result_df, use_container_width=True, hide_index=True, column_config={
    '柜量': st.column_config.NumberColumn('柜量', format='%d'),
    '售价(AUD)': st.column_config.NumberColumn('售价(AUD)', format='%.0f'),
    '开票价': st.column_config.NumberColumn('开票价', format='%.0f'),
    'NETNET': st.column_config.NumberColumn('NETNET', format='%.0f'),
    '到库成本': st.column_config.NumberColumn('到库成本', format='%.0f'),
    '变动费用': st.column_config.NumberColumn('变动费用', format='%.0f'),
    '合计成本': st.column_config.NumberColumn('合计成本', format='%.0f'),
    '毛利率%': st.column_config.NumberColumn('毛利率%', format='%.1f'),
    '净利率%': st.column_config.NumberColumn('净利率%', format='%.1f'),
})

m1, m2, m3 = st.columns(3)
m1.metric('NET NET', f"{metrics['HA Net Net Price'] :.0f} AUD")
m2.metric('Gross Margin', f"{metrics['Gross Margin']:.1f}%")
m3.metric('Net Margin', f"{metrics['Net Margin']:.1f}%")
