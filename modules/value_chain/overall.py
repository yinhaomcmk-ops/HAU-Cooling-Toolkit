import io
import json
from pathlib import Path

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from db import init_db, get_latest_exw, get_model_master
from vc_shared import (
    PRODUCT_LINE_RULES,
    load_global_params,
    month_to_date,
    pct_to_rate,
    rate_to_pct,
    safe_float,
    safe_int,
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
/* Native st.data_editor: selected cell / row visual support where Streamlit exposes DOM roles.
   Note: st.data_editor is canvas-based in some versions, so row-hover CSS may be limited,
   but the Styler below handles zebra rows and margin highlights. */
div[data-testid="stDataFrame"] div[role="gridcell"][aria-selected="true"] {
    outline: 2px solid #38bdf8 !important;
    outline-offset: -2px !important;
}
div[data-testid="stDataFrame"] div[role="row"]:hover div[role="gridcell"] {
    background-color: #17324d !important;
    color: #ffffff !important;
    font-weight: 700 !important;
}
div[data-testid="stDataFrame"] div[role="row"][aria-selected="true"] div[role="gridcell"] {
    background-color: #1e3a8a !important;
    color: #ffffff !important;
    font-weight: 800 !important;
}


/* Compact aligned title row for the two tables. Keeps the sort popover from pushing only the left table down. */
.vc-table-title {
    height: 38px;
    display: flex;
    align-items: center;
    font-size: 14px;
    font-weight: 800;
    color: #ffffff;
    margin: 0;
    padding: 0;
}
.vc-table-toolbar-spacer {
    height: 38px;
}
</style>
""",
    unsafe_allow_html=True,
)

# Preserve page and table scroll positions across st.rerun() after saving.
components.html(
    """
<script>
(function () {
    const KEY = 'vc_overall_scroll_state_v1';
    const root = window.parent;

    function getScrollables() {
        const nodes = Array.from(root.document.querySelectorAll('div[data-testid=\"stDataFrame\"] *'));
        return nodes.filter(function (el) {
            return (el.scrollHeight > el.clientHeight + 8) || (el.scrollWidth > el.clientWidth + 8);
        });
    }

    function saveScroll() {
        try {
            const scrollables = getScrollables();
            const state = {
                windowY: root.scrollY || root.document.documentElement.scrollTop || 0,
                tables: scrollables.map(function (el) {
                    return {top: el.scrollTop || 0, left: el.scrollLeft || 0};
                })
            };
            root.sessionStorage.setItem(KEY, JSON.stringify(state));
        } catch (e) {}
    }

    function restoreScroll() {
        try {
            const raw = root.sessionStorage.getItem(KEY);
            if (!raw) return;
            const state = JSON.parse(raw);

            if (state.windowY) {
                root.scrollTo(0, state.windowY);
            }

            const scrollables = getScrollables();
            (state.tables || []).forEach(function (pos, i) {
                if (scrollables[i]) {
                    scrollables[i].scrollTop = pos.top || 0;
                    scrollables[i].scrollLeft = pos.left || 0;
                }
            });
        } catch (e) {}
    }

    root.addEventListener('scroll', saveScroll, {passive: true});
    root.addEventListener('beforeunload', saveScroll);
    root.setInterval(saveScroll, 700);

    setTimeout(restoreScroll, 80);
    setTimeout(restoreScroll, 300);
    setTimeout(restoreScroll, 800);
})();
</script>
    """,
    height=0,
)

st.title('Value Chain | Overall Calculator')

STATE_FILE = Path('data') / 'overall_state.json'
STATE_FILE.parent.mkdir(exist_ok=True)
DEFAULT_STATE = {
    'model_overrides': {},
    'competitor_overrides': {},
}

EDITABLE_COLS = ['柜量', '常规价', '促销价', '大促价', '常规%', '促销%', '大促%']
RESULT_COLS = [
    '客户型号', '产品线', '品类', '柜量', '毛利率', '净利率', '常规价', '促销价', '大促价','NETNET','到库成本', '变动费用', '总成本', 'FOB(AUD)', '综合返利额', '开票价','成本月份', 
    'Expense Rate', 
    '海运/台', '清关/台', '保险',  '常规%', '促销%', '大促%'
]


def load_state():
    if not STATE_FILE.exists():
        return DEFAULT_STATE.copy()
    try:
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict):
            return {
                'model_overrides': data.get('model_overrides', {}),
                'competitor_overrides': data.get('competitor_overrides', {}),
            }
    except Exception:
        pass
    return DEFAULT_STATE.copy()


def save_state(state):
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def calc_metrics_exw(
    exw_cost_cny,
    selling_price,
    loading_qty,
    fx_aud_to_cny,
    aud_to_usd,
    sea_freight_usd,
    clearance_aud,
    insurance_rate,
    hq_upcost_rate,
    singa_upcost_rate,
    expense_rate,
    rebate_rate,
):
    invoice_price = selling_price / 1.1 if selling_price else 0.0
    rebate_amount = invoice_price * rebate_rate
    net_net = invoice_price - rebate_amount
    fob_cost_aud = (
        exw_cost_cny / (1 - hq_upcost_rate) / (1 - singa_upcost_rate) / fx_aud_to_cny
        if fx_aud_to_cny and hq_upcost_rate < 1 and singa_upcost_rate < 1
        else 0.0
    )
    sea_freight_per_unit = (sea_freight_usd / aud_to_usd) / loading_qty if aud_to_usd and loading_qty else 0.0
    clearance_per_unit = clearance_aud / loading_qty if loading_qty else 0.0
    insurance = fob_cost_aud * insurance_rate
    stock_cost = fob_cost_aud + sea_freight_per_unit + clearance_per_unit + insurance
    variable_cost = invoice_price * expense_rate
    total_cost = stock_cost + variable_cost
    gross_margin = (invoice_price - total_cost) / invoice_price if invoice_price else 0.0
    net_margin = (net_net - total_cost) / net_net if net_net else 0.0
    return {
        'invoice_price': invoice_price,
        'rebate_amount': rebate_amount,
        'net_net': net_net,
        'fob_cost_aud': fob_cost_aud,
        'sea_freight_per_unit': sea_freight_per_unit,
        'clearance_per_unit': clearance_per_unit,
        'insurance': insurance,
        'stock_cost': stock_cost,
        'variable_cost': variable_cost,
        'total_cost': total_cost,
        'gross_margin': gross_margin,
        'net_margin': net_margin,
    }


def build_result_table(base_df, g):
    rows = []
    for _, row in base_df.iterrows():
        regular_price = safe_float(row['常规价'])
        promo_price = safe_float(row['促销价'])
        big_price = safe_float(row['大促价'])
        regular_mix_pct = safe_float(row['常规%'])
        promo_mix_pct = safe_float(row['促销%'])
        big_mix_pct = safe_float(row['大促%'])
        total_mix_pct = regular_mix_pct + promo_mix_pct + big_mix_pct
        if total_mix_pct > 0:
            rw, pw, bw = regular_mix_pct / total_mix_pct, promo_mix_pct / total_mix_pct, big_mix_pct / total_mix_pct
        else:
            rw = pw = bw = 0.0

        loading_qty = max(1, safe_int(row['柜量'], 100))
        exw_cost_cny = safe_float(row['结算成本'])
        hq_upcost_rate = safe_float(row['_upcost_rate'])
        expense_rate = safe_float(row['_expense_rate'])

        m_regular = calc_metrics_exw(
            exw_cost_cny, regular_price, loading_qty,
            g['fx_aud_to_cny'], g['aud_to_usd'], g['sea_freight_usd'], g['clearance_aud'],
            g['insurance_rate'], hq_upcost_rate, g['singa_upcost_rate'], expense_rate, g['regular_rebate'] / 100.0,
        )
        m_promo = calc_metrics_exw(
            exw_cost_cny, promo_price, loading_qty,
            g['fx_aud_to_cny'], g['aud_to_usd'], g['sea_freight_usd'], g['clearance_aud'],
            g['insurance_rate'], hq_upcost_rate, g['singa_upcost_rate'], expense_rate, g['promo_rebate'] / 100.0,
        )
        m_big = calc_metrics_exw(
            exw_cost_cny, big_price, loading_qty,
            g['fx_aud_to_cny'], g['aud_to_usd'], g['sea_freight_usd'], g['clearance_aud'],
            g['insurance_rate'], hq_upcost_rate, g['singa_upcost_rate'], expense_rate, g['big_rebate'] / 100.0,
        )

        invoice_price = m_regular['invoice_price'] * rw + m_promo['invoice_price'] * pw + m_big['invoice_price'] * bw
        rebate_amount = m_regular['rebate_amount'] * rw + m_promo['rebate_amount'] * pw + m_big['rebate_amount'] * bw
        net_net = invoice_price - rebate_amount
        fob_cost_aud = m_regular['fob_cost_aud'] * rw + m_promo['fob_cost_aud'] * pw + m_big['fob_cost_aud'] * bw
        sea_freight = m_regular['sea_freight_per_unit'] * rw + m_promo['sea_freight_per_unit'] * pw + m_big['sea_freight_per_unit'] * bw
        clearance = m_regular['clearance_per_unit'] * rw + m_promo['clearance_per_unit'] * pw + m_big['clearance_per_unit'] * bw
        insurance = m_regular['insurance'] * rw + m_promo['insurance'] * pw + m_big['insurance'] * bw
        stock_cost = m_regular['stock_cost'] * rw + m_promo['stock_cost'] * pw + m_big['stock_cost'] * bw
        variable_cost = m_regular['variable_cost'] * rw + m_promo['variable_cost'] * pw + m_big['variable_cost'] * bw
        total_cost = m_regular['total_cost'] * rw + m_promo['total_cost'] * pw + m_big['total_cost'] * bw

        gross_margin = 1 - (stock_cost / net_net) if net_net else 0.0
        net_margin = gross_margin - expense_rate

        rows.append({
            '客户型号': row['客户型号'],
            '产品线': row['产品线'],
            '品类': row['品类'],
            '成本月份': row['成本月份'],
            '柜量': loading_qty,
            '结算成本': exw_cost_cny,
            '常规价': regular_price,
            '促销价': promo_price,
            '大促价': big_price,
            '常规%': regular_mix_pct,
            '促销%': promo_mix_pct,
            '大促%': big_mix_pct,
            '开票价': invoice_price,
            '综合返利额': rebate_amount,
            'NETNET': net_net,
            'FOB(AUD)': fob_cost_aud,
            '海运/台': sea_freight,
            '清关/台': clearance,
            '保险': insurance,
            '到库成本': stock_cost,
            '变动费用': variable_cost,
            '总成本': total_cost,
            'Expense Rate': expense_rate * 100,
            '毛利率': gross_margin * 100,
            '净利率': net_margin * 100,
        })
    return pd.DataFrame(rows)


def build_base_df(raw_df, overrides):
    base_df = raw_df.copy().rename(columns={
        'model_id': '客户型号',
        'product_line': '产品线',
        'category': '品类',
        'exw_cost': '结算成本',
    })
    base_df['常规价'] = 1399.0
    base_df['促销价'] = 1299.0
    base_df['大促价'] = 1199.0
    base_df['常规%'] = 35.0
    base_df['促销%'] = 45.0
    base_df['大促%'] = 20.0

    for i, row in base_df.iterrows():
        ov = overrides.get(str(row['客户型号']), {})
        for col in EDITABLE_COLS:
            if col in ov:
                base_df.at[i, col] = ov[col]

    return base_df[[
        '客户型号', '产品线', '品类', '成本月份', '柜量', '结算成本',
        '常规价', '促销价', '大促价', '常规%', '促销%', '大促%',
        '_upcost_rate', '_expense_rate'
    ]].copy()


def make_overrides_from_df(df_to_store):
    result = {}
    for _, row in df_to_store.iterrows():
        model = str(row['客户型号'])
        result[model] = {}
        for col in EDITABLE_COLS:
            result[model][col] = safe_int(row[col], 0) if col == '柜量' else safe_float(row[col], 0.0)
    return result


def build_batch_template_df(source_df):
    """
    Download/upload template.
    Only EDITABLE_COLS will be written back into cache/state.
    Other columns are for reference and matching only.
    """
    reference_cols = ['NETNET', '到库成本', '变动费用']
    base_cols = ['客户型号', '产品线', '品类', '成本月份']
    cols = base_cols + reference_cols + EDITABLE_COLS
    available_cols = [c for c in cols if c in source_df.columns]
    template = source_df[available_cols].copy()

    for col in EDITABLE_COLS:
        if col not in template.columns:
            template[col] = 0

    for col in reference_cols:
        if col not in template.columns:
            template[col] = 0

    return template[base_cols + reference_cols + EDITABLE_COLS]


def dataframe_to_excel_bytes(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Overall Input')
    return output.getvalue()


def apply_batch_upload_to_state(uploaded_df, full_source_df, state):
    """
    Batch upload only updates:
    柜量, 常规价, 促销价, 大促价, 常规%, 促销%, 大促%
    Match key: 客户型号
    """
    if uploaded_df is None or uploaded_df.empty:
        return False, 'Uploaded file is empty.', 0, []

    if '客户型号' not in uploaded_df.columns:
        return False, 'Upload file must contain 客户型号 column.', 0, []

    missing_edit_cols = [c for c in EDITABLE_COLS if c not in uploaded_df.columns]
    if missing_edit_cols:
        return False, f"Upload file is missing editable columns: {', '.join(missing_edit_cols)}", 0, []

    updated_df = full_source_df.copy()
    uploaded_df = uploaded_df.copy()
    uploaded_df['客户型号'] = uploaded_df['客户型号'].astype(str).str.strip()
    updated_df['客户型号'] = updated_df['客户型号'].astype(str).str.strip()

    valid_models = set(updated_df['客户型号'].astype(str).tolist())
    changed_models = []
    skipped_models = []

    for _, row in uploaded_df.iterrows():
        model = str(row['客户型号']).strip()
        if not model or model not in valid_models:
            skipped_models.append(model)
            continue

        mask = updated_df['客户型号'] == model
        changed = False

        for col in EDITABLE_COLS:
            if col == '柜量':
                new_value = safe_int(row[col], 0)
                old_value = safe_int(updated_df.loc[mask, col].iloc[0], 0)
            else:
                new_value = safe_float(row[col], 0.0)
                old_value = safe_float(updated_df.loc[mask, col].iloc[0], 0.0)

            if new_value != old_value:
                updated_df.loc[mask, col] = new_value
                changed = True

        if changed:
            changed_models.append(model)

    state['model_overrides'] = make_overrides_from_df(updated_df)
    save_state(state)

    return True, 'Batch upload saved to overall_state.json.', len(changed_models), skipped_models


def get_default_competitor_entry():
    return {
        '竞品1品牌': '', '竞品1型号': '', '竞品1常规价': 0.0, '竞品1促销价': 0.0,
        '竞品2品牌': '', '竞品2型号': '', '竞品2常规价': 0.0, '竞品2促销价': 0.0,
    }


def normalize_competitor_state(raw_state):
    normalized = {}
    for model, value in (raw_state or {}).items():
        item = get_default_competitor_entry()
        if isinstance(value, dict):
            for k in item:
                if '价' in k:
                    item[k] = safe_float(value.get(k), 0.0)
                else:
                    item[k] = str(value.get(k, '') or '')
        normalized[str(model)] = item
    return normalized


def save_competitor_value(state, model, key, value):
    model = str(model)
    competitor_overrides = normalize_competitor_state(state.get('competitor_overrides', {}))
    model_entry = competitor_overrides.get(model, get_default_competitor_entry())
    if '价' in key:
        model_entry[key] = safe_float(value, 0.0)
    else:
        model_entry[key] = str(value or '')
    competitor_overrides[model] = model_entry
    state['competitor_overrides'] = competitor_overrides
    save_state(state)


def persist_competitor_editor_changes(visible_models):
    widget_state = st.session_state.get('overall_competitor_table_editor', {})
    edited_rows = widget_state.get('edited_rows', {}) if isinstance(widget_state, dict) else {}
    if not edited_rows:
        return

    latest_state = load_state()
    competitor_overrides = normalize_competitor_state(latest_state.get('competitor_overrides', {}))

    col_map = {
        'A品牌': '竞品1品牌',
        'A型号': '竞品1型号',
        'A常规': '竞品1常规价',
        'A促销': '竞品1促销价',
        'B品牌': '竞品2品牌',
        'B型号': '竞品2型号',
        'B常规': '竞品2常规价',
        'B促销': '竞品2促销价',
    }

    for row_idx, changes in edited_rows.items():
        try:
            row_idx = int(row_idx)
        except:
            continue

        if row_idx < 0 or row_idx >= len(visible_models):
            continue

        model = visible_models[row_idx]   # ✅ 正确映射

        entry = competitor_overrides.get(model, get_default_competitor_entry()).copy()

        for editor_col, value in (changes or {}).items():
            target_col = col_map.get(editor_col)
            if not target_col:
                continue

            if '价' in target_col:
                entry[target_col] = safe_float(value, 0.0)
            else:
                entry[target_col] = str(value or '')

        competitor_overrides[model] = entry

    latest_state['competitor_overrides'] = competitor_overrides
    save_state(latest_state)

def build_summary_row(df):
    summary = {col: '' for col in df.columns}
    summary['客户型号'] = len(df)
    summary['产品线'] = 'AVG'
    if not df.empty:
        summary['毛利率'] = df['毛利率'].mean()
        summary['净利率'] = df['净利率'].mean()
        summary['Expense Rate'] = df['Expense Rate'].mean()
    return summary


def style_result_table(df):
    def row_style(row):
        styles = ['' for _ in row.index]
        is_summary = row.name == len(df) - 1
        for i, col in enumerate(row.index):
            if col in ['毛利率', '净利率']:
                styles[i] += 'font-weight:700;'

            if not is_summary and col == '毛利率':
                gm = safe_float(row['毛利率'])
                er = safe_float(row['Expense Rate'])
                if gm < er:
                    styles[i] += 'color:#2563eb; font-weight:700;'
            if not is_summary and col == '净利率':
                nm = safe_float(row['净利率'])
                if nm < 0:
                    styles[i] += 'color:#2563eb; font-weight:700;'
            if is_summary:
                styles[i] += 'background-color:#f8fafc; font-weight:600;'
        return styles

    return df.style.apply(row_style, axis=1)



def style_native_editor_table(df, competitor=False):
    """
    Native st.data_editor styling.
    - Zebra row background for easier row tracking.
    - Highlight 毛利率 / 净利率 columns.
    - 毛利率低于 Expense Rate 时，毛利率字体标蓝。
    - 净利率低于 0 时，净利率字体标蓝。
    - Hover/selected row is handled by CSS above where Streamlit exposes DOM roles.
    """
    def cell_style(row):
        styles = []
        is_even = row.name % 2 == 0
        base_bg = '#06090f' if is_even else '#242933'
        base_border = '1px solid #303846'

        gm = safe_float(row.get('毛利率', 0))
        er = safe_float(row.get('Expense Rate', 0))
        nm = safe_float(row.get('净利率', 0))
        low_gm = gm < (er + 0.05)
        low_nm = nm < 0

        for col in row.index:
            style = (
                f'background-color:{base_bg}; '
                f'color:#e5edf7; '
                f'border-bottom:{base_border}; '
                'font-weight:600;'
            )

            # Margin columns: always visually emphasized.
            if col == '毛利率':
                if low_gm:
                    style += (
                        'background-color:#0b1f3a; '
                        'color:#60a5fa; '
                        'font-weight:900; '
                        'border-left:1px solid #60a5fa; '
                        'border-right:1px solid #60a5fa;'
                    )
                else:
                    style += (
                        'background-color:#0f3f46; '
                        'color:#ffffff; '
                        'font-weight:900; '
                        'border-left:1px solid #2dd4bf; '
                        'border-right:1px solid #2dd4bf;'
                    )

            if col == '净利率':
                if low_nm:
                    style += (
                        'background-color:#0b1f3a; '
                        'color:#60a5fa; '
                        'font-weight:900; '
                        'border-left:1px solid #60a5fa; '
                        'border-right:1px solid #60a5fa;'
                    )
                else:
                    style += (
                        'background-color:#115e59; '
                        'color:#ffffff; '
                        'font-weight:900; '
                        'border-left:1px solid #2dd4bf; '
                        'border-right:1px solid #2dd4bf;'
                    )

            # Competitor price gap columns: positive = red, negative = blue.
            # Formula is Hisense price / competitor price - 1.
            if col in ['A常规价差', 'A促销价差', 'B常规价差', 'B促销价差']:
                gap = safe_float(row.get(col, 0))
                if gap > 0:
                    style += (
                        'color:#ff6b6b; '
                        'font-weight:900; '
                        'background-color:#2a0f14;'
                    )
                elif gap < 0:
                    style += (
                        'color:#60a5fa; '
                        'font-weight:900; '
                        'background-color:#0b1f3a;'
                    )
                else:
                    style += 'color:#94a3b8; font-weight:700;'

            styles.append(style)
        return styles

    return df.style.apply(cell_style, axis=1)



def blank_zero_for_display(value):
    """Return blank for competitor price/gap fields that are truly empty/no competitor data."""
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        v = float(value)
    except Exception:
        return value
    return None if v == 0 else v


def normalize_blank_numeric(value):
    """Convert blank values coming back from st.data_editor into 0 for state comparison/save."""
    if value is None:
        return 0.0
    try:
        if pd.isna(value):
            return 0.0
    except Exception:
        pass
    if isinstance(value, str) and value.strip() == '':
        return 0.0
    return safe_float(value, 0.0)

def format_display_table(df):
    fmt_df = df.copy()
    int_cols = ['客户型号', '柜量']
    money_0_cols = ['常规价', '促销价', '大促价']
    money_2_cols = ['FOB(AUD)', '到库成本', 'NETNET', '变动费用', '总成本', '海运/台', '清关/台', '保险', '综合返利额', '开票价']
    pct_cols = ['Expense Rate', '毛利率', '净利率', '常规%', '促销%', '大促%']

    for col in money_0_cols:
        if col in fmt_df.columns:
            fmt_df[col] = fmt_df[col].apply(lambda x: '' if x == '' else f'{safe_float(x):.0f}')
    for col in money_2_cols:
        if col in fmt_df.columns:
            fmt_df[col] = fmt_df[col].apply(lambda x: '' if x == '' else f'{safe_float(x):.2f}')
    for col in pct_cols:
        if col in fmt_df.columns:
            fmt_df[col] = fmt_df[col].apply(lambda x: '' if x == '' else f'{safe_float(x):.1f}%')
    for col in int_cols:
        if col in fmt_df.columns:
            fmt_df[col] = fmt_df[col].apply(lambda x: '' if x == '' else f'{safe_int(x)}')
    return fmt_df


# ===== load data =====
g = load_global_params()
page_memory = load_page_memory('overall')
state = load_state()
state['competitor_overrides'] = normalize_competitor_state(state.get('competitor_overrides', {}))
raw_df = get_latest_exw()
if raw_df is None or raw_df.empty:
    st.warning('请先在 VC Cost 页面导入结算成本，并在 VC Model 页面维护型号主数据。')
    st.stop()

valid_models = set(get_model_master()['model_id'].astype(str).tolist())
df = raw_df.copy()
df['model_id'] = df['model_id'].astype(str)
df = df[df['model_id'].isin(valid_models)].copy()
df['exw_cost'] = pd.to_numeric(df['exw_cost'], errors='coerce')
df['cost_month_dt'] = df['cost_month'].apply(month_to_date) if 'cost_month' in df.columns else pd.NaT
df['成本月份'] = df['cost_month_dt'].dt.strftime('%Y/%m').fillna(df.get('cost_month', '').astype(str))
df['柜量'] = df['product_line'].map(lambda x: PRODUCT_LINE_RULES.get(x, {}).get('loading_qty', 100))
df['_upcost_rate'] = df['product_line'].map(lambda x: PRODUCT_LINE_RULES.get(x, {}).get('hq_upcost_rate', 0.0329))
df['_expense_rate'] = df['product_line'].map(lambda x: PRODUCT_LINE_RULES.get(x, {}).get('expense_rate', 0.29))
df = df.dropna(subset=['model_id', 'exw_cost']).reset_index(drop=True)
if df.empty:
    st.warning('当前没有可用的结算成本数据。')
    st.stop()

full_source_df = build_base_df(df, state.get('model_overrides', {}))

# ===== sidebar =====
st.sidebar.header('Global Parameters')
fx_aud_to_cny = st.sidebar.number_input('1 AUD = CNY', min_value=0.0001, value=safe_float(g['fx_aud_to_cny']), format='%.2f')
aud_to_usd = st.sidebar.number_input('1 AUD = USD', min_value=0.0001, value=safe_float(g['aud_to_usd']), format='%.2f')
sea_freight_usd = st.sidebar.number_input('Sea Freight / 40HQ (USD)', min_value=0.0, value=safe_float(g['sea_freight_usd']), format='%.0f')
clearance_aud = st.sidebar.number_input('Custom Clearance & Cartage (AUD)', min_value=0.0, value=safe_float(g['clearance_aud']), format='%.0f')
insurance_pct = st.sidebar.number_input('Insurance (%)', min_value=0.0, value=rate_to_pct(g['insurance_rate']), format='%.1f', step=0.1)
singa_upcost_pct = st.sidebar.number_input('Singa Upcost (%)', min_value=0.0, value=rate_to_pct(g['singa_upcost_rate']), format='%.1f', step=0.1)
regular_rebate = st.sidebar.number_input('Regular Rebate (%)', min_value=0.0, value=safe_float(g['regular_rebate']), format='%.1f')
promo_rebate = st.sidebar.number_input('Promo Rebate (%)', min_value=0.0, value=safe_float(g['promo_rebate']), format='%.1f')
big_rebate = st.sidebar.number_input('Big Promo Rebate (%)', min_value=0.0, value=safe_float(g['big_rebate']), format='%.1f')

save_global_params({
    **g,
    'fx_aud_to_cny': float(fx_aud_to_cny),
    'aud_to_usd': float(aud_to_usd),
    'sea_freight_usd': float(sea_freight_usd),
    'clearance_aud': float(clearance_aud),
    'insurance_rate': pct_to_rate(insurance_pct),
    'singa_upcost_rate': pct_to_rate(singa_upcost_pct),
    'regular_rebate': float(regular_rebate),
    'promo_rebate': float(promo_rebate),
    'big_rebate': float(big_rebate),
})
g = load_global_params()

# ===== filters =====
st.subheader('Filters')
f1, f2, f3 = st.columns([1, 1, 1.4])
product_line_options = ['All'] + sorted(full_source_df['产品线'].dropna().unique().tolist())
category_options = ['All'] + sorted(full_source_df['品类'].dropna().unique().tolist())
with f1:
    selected_product_line = st.selectbox(
        'Product Line',
        product_line_options,
        index=get_saved_select_index(product_line_options, page_memory.get('selected_product_line', 'All')),
        key='overall_selected_product_line',
    )
with f2:
    selected_category = st.selectbox(
        'Category',
        category_options,
        index=get_saved_select_index(category_options, page_memory.get('selected_category', 'All')),
        key='overall_selected_category',
    )

filtered_for_model = full_source_df.copy()
if selected_product_line != 'All':
    filtered_for_model = filtered_for_model[filtered_for_model['产品线'] == selected_product_line]
if selected_category != 'All':
    filtered_for_model = filtered_for_model[filtered_for_model['品类'] == selected_category]

model_options = ['All'] + sorted(filtered_for_model['客户型号'].dropna().unique().tolist())
with f3:
    selected_model = st.selectbox(
        'Model',
        model_options,
        index=get_saved_select_index(model_options, page_memory.get('selected_model', 'All')),
        key='overall_selected_model',
    )

filtered_source_df = filtered_for_model.copy()
if selected_model != 'All':
    filtered_source_df = filtered_source_df[filtered_source_df['客户型号'] == selected_model]

save_page_memory('overall', {
    'selected_product_line': selected_product_line,
    'selected_category': selected_category,
    'selected_model': selected_model,
})

if filtered_source_df.empty:
    st.warning('当前筛选条件下没有数据。')
    st.stop()

# ===== synchronized sort state =====
# Native st.data_editor header sorting is frontend-only and cannot sync a separate
# competitor table. These hidden state values drive both tables together. The UI
# controls are rendered as a small popover icon next to the Overall Result title.
sort_options = [
    'Original', '客户型号', '产品线', '品类', '柜量', '毛利率', '净利率',
    '常规价', '促销价', '大促价', 'NETNET', '到库成本', '变动费用',
    '总成本', 'FOB(AUD)', 'Expense Rate'
]
selected_sort_col = st.session_state.get('overall_selected_sort_col', page_memory.get('selected_sort_col', 'Original'))
if selected_sort_col not in sort_options:
    selected_sort_col = 'Original'
selected_sort_order = st.session_state.get('overall_selected_sort_order', page_memory.get('selected_sort_order', 'Ascending'))
if selected_sort_order not in ['Ascending', 'Descending']:
    selected_sort_order = 'Ascending'

if selected_sort_col != 'Original':
    sort_preview_df = build_result_table(filtered_source_df, g)[RESULT_COLS].copy()
    ascending = selected_sort_order == 'Ascending'
    sorted_idx = sort_preview_df.sort_values(
        by=selected_sort_col,
        ascending=ascending,
        kind='mergesort',
        na_position='last',
    ).index
    filtered_source_df = filtered_source_df.iloc[sorted_idx].reset_index(drop=True)

# ===== batch download / upload =====
with st.expander('Batch Download / Upload', expanded=False):
    st.caption('Download the current table including NETNET / 到库成本 / 变动费用 for reference. Edit only 柜量 / 常规价 / 促销价 / 大促价 / 常规% / 促销% / 大促%, then upload it back. Changes are saved into data/overall_state.json.')

    template_df = build_batch_template_df(build_result_table(filtered_source_df, g)[RESULT_COLS].copy())
    c1, c2 = st.columns([1, 2])

    with c1:
        st.download_button(
            label='Download Current Editable Table',
            data=dataframe_to_excel_bytes(template_df),
            file_name='overall_editable_table.xlsx',
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            use_container_width=True,
        )

    with c2:
        uploaded_file = st.file_uploader(
            'Upload Edited Table',
            type=['xlsx', 'xls', 'csv'],
            key='overall_batch_upload_file',
        )

    if uploaded_file is not None:
        try:
            if uploaded_file.name.lower().endswith('.csv'):
                upload_df = pd.read_csv(uploaded_file)
            else:
                upload_df = pd.read_excel(uploaded_file)

            st.info('Only editable columns will be saved: ' + ', '.join(EDITABLE_COLS))

            preview_cols = [c for c in ['客户型号', '产品线', '品类', '成本月份', 'NETNET', '到库成本', '毛利率','净利率','变动费用'] + EDITABLE_COLS if c in upload_df.columns]
            if preview_cols:
                st.dataframe(upload_df[preview_cols].head(20), use_container_width=True, hide_index=True)

            if st.button('Save Uploaded Changes', key='overall_save_batch_upload', use_container_width=True):
                ok, msg, changed_count, skipped_models = apply_batch_upload_to_state(upload_df, full_source_df, state)
                if ok:
                    st.success(f'{msg} Updated models: {changed_count}.')
                    if skipped_models:
                        st.warning('Skipped unmatched models: ' + ', '.join([m for m in skipped_models if m][:20]))
                    st.rerun()
                else:
                    st.error(msg)

        except Exception as e:
            st.error(f'Upload failed: {e}')

# ===== layout title row =====
# Render the table titles in a separate shared row first. This keeps the two data_editors
# starting at exactly the same vertical position even when the left title has a popover button.
header_left, header_right = st.columns([3.7, 3.3], gap='small')

with header_left:
    title_col, sort_tool_col = st.columns([18, 1])
    with title_col:
        st.markdown('<div class="vc-table-title">Overall Result</div>', unsafe_allow_html=True)
    with sort_tool_col:
        with st.popover('⇅', help='Sort both Overall and Competitor tables together'):
            st.caption('Sort both tables')
            st.selectbox(
                'Sort By',
                sort_options,
                index=get_saved_select_index(sort_options, selected_sort_col),
                key='overall_selected_sort_col',
            )
            st.selectbox(
                'Order',
                ['Ascending', 'Descending'],
                index=get_saved_select_index(['Ascending', 'Descending'], selected_sort_order),
                key='overall_selected_sort_order',
            )
            st.caption('Use this instead of table-header sorting so the right table stays aligned.')

with header_right:
    comp_title_col, comp_spacer_col = st.columns([18, 1])
    with comp_title_col:
        st.markdown('<div class="vc-table-title">Competitor Table</div>', unsafe_allow_html=True)
    with comp_spacer_col:
        st.markdown('<div class="vc-table-toolbar-spacer"></div>', unsafe_allow_html=True)

save_page_memory('overall', {
    'selected_product_line': selected_product_line,
    'selected_category': selected_category,
    'selected_model': selected_model,
    'selected_sort_col': st.session_state.get('overall_selected_sort_col', selected_sort_col),
    'selected_sort_order': st.session_state.get('overall_selected_sort_order', selected_sort_order),
})

# ===== table row =====
left_col, right_col = st.columns([3.7, 3.3], gap='small')

# ===== left: main editor =====
with left_col:
    edit_input_df = build_result_table(filtered_source_df, g)[RESULT_COLS].copy()
    edited_display = st.data_editor(
        style_native_editor_table(edit_input_df),
        column_config={
            '客户型号': st.column_config.TextColumn('客户型号', pinned=True, width='medium'),
            '柜量': st.column_config.NumberColumn('柜量', format='%d'),
            'FOB(AUD)': st.column_config.NumberColumn('FOB(AUD)', format='%.0f'),
            '海运/台': st.column_config.NumberColumn('海运/台', format='%.0f'),
            '清关/台': st.column_config.NumberColumn('清关/台', format='%.0f'),
            '保险': st.column_config.NumberColumn('保险', format='%.0f'),
            '到库成本': st.column_config.NumberColumn('到库成本', format='%.0f'),
            '变动费用': st.column_config.NumberColumn('变动费用', format='%.0f'),
            '综合返利额': st.column_config.NumberColumn('综合返利额', format='%.0f'),
            'NETNET': st.column_config.NumberColumn('NETNET', format='%.0f'),
            '总成本': st.column_config.NumberColumn('总成本', format='%.0f'),
            'Expense Rate': st.column_config.NumberColumn('Expense Rate', format='%.0f%%'),
            '毛利率': st.column_config.NumberColumn('毛利率', format='%.1f%%'),
            '净利率': st.column_config.NumberColumn('净利率', format='%.1f%%'),
            '常规价': st.column_config.NumberColumn('常规价', format='$ %,.0f'),
            '促销价': st.column_config.NumberColumn('促销价', format='$ %,.0f'),
            '大促价': st.column_config.NumberColumn('大促价', format='$ %,.0f'),
            '开票价': st.column_config.NumberColumn('开票价', format='%.0f'),
            '常规%': st.column_config.NumberColumn('常规%', format='%.0f'),
            '促销%': st.column_config.NumberColumn('促销%', format='%.0f'),
            '大促%': st.column_config.NumberColumn('大促%', format='%.0f'),
        },
        disabled=[
            '客户型号', '产品线', '品类', '成本月份', 'FOB(AUD)', '到库成本', 'NETNET',
            'Expense Rate', '毛利率', '净利率', '变动费用', '总成本', '海运/台',
            '清关/台', '保险', '综合返利额', '开票价'
        ],
        hide_index=True,
        height=600,
        use_container_width=True,
        key='overall_single_table_editor_sync',
    )

    has_change = False
    updated_full_source_df = full_source_df.copy()
    for _, edited_row in edited_display.iterrows():
        model = edited_row['客户型号']
        mask = updated_full_source_df['客户型号'] == model
        if not mask.any():
            continue
        for col in EDITABLE_COLS:
            old_norm = safe_int(updated_full_source_df.loc[mask, col].iloc[0], 0) if col == '柜量' else round(safe_float(updated_full_source_df.loc[mask, col].iloc[0], 0.0), 6)
            new_norm = safe_int(edited_row[col], 0) if col == '柜量' else round(safe_float(edited_row[col], 0.0), 6)
            if old_norm != new_norm:
                updated_full_source_df.loc[mask, col] = safe_int(edited_row[col], 0) if col == '柜量' else safe_float(edited_row[col], 0.0)
                has_change = True

    save_clicked = st.button(
        'Save Changes',
        key='overall_save_table_changes',
        type='primary',
        use_container_width=True,
        disabled=not has_change,
    )
    if has_change:
        st.caption('Unsaved changes detected. Click Save Changes to recalculate and store them.')
    else:
        st.caption('No unsaved changes.')

    if save_clicked and has_change:
        state['model_overrides'] = make_overrides_from_df(updated_full_source_df)
        save_state(state)
        st.success('Changes saved. Recalculating...')
        st.rerun()

# ===== right: competitor inputs =====
with right_col:
    visible_models = filtered_source_df['客户型号'].astype(str).tolist()
    competitor_state = normalize_competitor_state(state.get('competitor_overrides', {}))

    competitor_rows = []
    for model in visible_models:
        comp = competitor_state.get(model, get_default_competitor_entry())
        competitor_rows.append({
            'A品牌': comp['竞品1品牌'],
            'A型号': comp['竞品1型号'],
            'A常规': safe_float(comp['竞品1常规价']),
            'A促销': safe_float(comp['竞品1促销价']),
            'B品牌': comp['竞品2品牌'],
            'B型号': comp['竞品2型号'],
            'B常规': safe_float(comp['竞品2常规价']),
            'B促销': safe_float(comp['竞品2促销价']),
        })

    competitor_df = pd.DataFrame(
        competitor_rows,
        columns=['A品牌', 'A型号', 'A常规', 'A促销', 'B品牌', 'B型号', 'B常规', 'B促销']
    )

    # ===== competitor price gap columns =====
    # Formula: Hisense price / competitor price - 1
    # Positive gap is shown in red; negative gap is shown in blue.
    competitor_display_df = competitor_df.copy()
    left_regular_prices = edit_input_df['常规价'].reset_index(drop=True)
    left_promo_prices = edit_input_df['促销价'].reset_index(drop=True)

    def calc_gap(base_price, comp_price):
        comp = safe_float(comp_price, 0.0)
        if comp == 0:
            # No competitor price input: show blank instead of 0.0%.
            return None
        # Store as percentage number for display:
        # 0.1113 -> 11.13, then NumberColumn format='%.1f%%' shows 11.1%.
        # If Hisense price equals competitor price, this returns exactly 0.0 and still displays 0.0%.
        return (safe_float(base_price, 0.0) / comp - 1.0) * 100.0

    competitor_display_df['A常规价差'] = [calc_gap(left_regular_prices.iloc[i], competitor_display_df.loc[i, 'A常规']) for i in range(len(competitor_display_df))]
    competitor_display_df['A促销价差'] = [calc_gap(left_promo_prices.iloc[i], competitor_display_df.loc[i, 'A促销']) for i in range(len(competitor_display_df))]
    competitor_display_df['B常规价差'] = [calc_gap(left_regular_prices.iloc[i], competitor_display_df.loc[i, 'B常规']) for i in range(len(competitor_display_df))]
    competitor_display_df['B促销价差'] = [calc_gap(left_promo_prices.iloc[i], competitor_display_df.loc[i, 'B促销']) for i in range(len(competitor_display_df))]

    # For display only: blank out competitor prices/gaps where there is no competitor price input.
    # If a valid competitor price exists and the calculated gap is 0.0%, it remains displayed as 0.0%.
    for _col in ['A常规', 'A促销', 'B常规', 'B促销']:
        competitor_display_df[_col] = competitor_display_df[_col].apply(blank_zero_for_display)

    competitor_display_df = competitor_display_df[
        ['A品牌', 'A型号', 'A常规', 'A促销', 'A常规价差', 'A促销价差',
         'B品牌', 'B型号', 'B常规', 'B促销', 'B常规价差', 'B促销价差']
    ]

    edited_competitor_df = st.data_editor(
        style_native_editor_table(competitor_display_df, competitor=True),
        column_config={
            # Keep columns compact so A/B brand, model, price and gap columns can fit in the right panel.
            'A品牌': st.column_config.TextColumn('A品牌', width=60),
            'A型号': st.column_config.TextColumn('A型号', width=85),
            'A常规': st.column_config.NumberColumn('A常规', format='%.0f', width=58),
            'A促销': st.column_config.NumberColumn('A促销', format='%.0f', width=58),
            'A常规价差': st.column_config.NumberColumn('A常规价差', format='%.1f%%', width=72),
            'A促销价差': st.column_config.NumberColumn('A促销价差', format='%.1f%%', width=72),
            'B品牌': st.column_config.TextColumn('B品牌', width=60),
            'B型号': st.column_config.TextColumn('B型号', width=85),
            'B常规': st.column_config.NumberColumn('B常规', format='%.0f', width=58),
            'B促销': st.column_config.NumberColumn('B促销', format='%.0f', width=58),
            'B常规价差': st.column_config.NumberColumn('B常规价差', format='%.1f%%', width=72),
            'B促销价差': st.column_config.NumberColumn('B促销价差', format='%.1f%%', width=72),
        },
        disabled=['A常规价差', 'A促销价差', 'B常规价差', 'B促销价差'],
        hide_index=True,
        use_container_width=True,
          height=600,
        key='overall_competitor_table_editor',
    )

    compare_cols = ['A品牌', 'A型号', 'A常规', 'A促销', 'B品牌', 'B型号', 'B常规', 'B促销']
    edited_competitor_base_df = edited_competitor_df[compare_cols].copy()

    # Convert display blanks back to zero before comparison/save.
    for _col in ['A常规', 'A促销', 'B常规', 'B促销']:
        edited_competitor_base_df[_col] = edited_competitor_base_df[_col].apply(normalize_blank_numeric)

    normalized_old_comp = competitor_df[compare_cols].copy()
    for _col in ['A常规', 'A促销', 'B常规', 'B促销']:
        normalized_old_comp[_col] = normalized_old_comp[_col].apply(normalize_blank_numeric)
    for _col in ['A品牌', 'A型号', 'B品牌', 'B型号']:
        edited_competitor_base_df[_col] = edited_competitor_base_df[_col].fillna('').astype(str)
        normalized_old_comp[_col] = normalized_old_comp[_col].fillna('').astype(str)

    comp_has_change = not edited_competitor_base_df.reset_index(drop=True).equals(normalized_old_comp.reset_index(drop=True))
    save_comp_clicked = st.button(
        'Save Competitor Changes',
        key='overall_save_competitor_changes',
        use_container_width=True,
        disabled=not comp_has_change,
    )
    if comp_has_change:
        st.caption('Unsaved competitor changes detected.')
    else:
        st.caption('No unsaved competitor changes.')

    if save_comp_clicked and comp_has_change:
        latest_state = load_state()
        competitor_overrides = normalize_competitor_state(latest_state.get('competitor_overrides', {}))

        col_map = {
            'A品牌': '竞品1品牌',
            'A型号': '竞品1型号',
            'A常规': '竞品1常规价',
            'A促销': '竞品1促销价',
            'B品牌': '竞品2品牌',
            'B型号': '竞品2型号',
            'B常规': '竞品2常规价',
            'B促销': '竞品2促销价',
        }

        for row_idx, row in edited_competitor_base_df.reset_index(drop=True).iterrows():
            if row_idx >= len(visible_models):
                continue
            model = visible_models[row_idx]
            entry = competitor_overrides.get(model, get_default_competitor_entry()).copy()
            for editor_col, target_col in col_map.items():
                value = row.get(editor_col, '')
                if '价' in target_col:
                    entry[target_col] = safe_float(value, 0.0)
                else:
                    entry[target_col] = str(value or '')
            competitor_overrides[model] = entry

        latest_state['competitor_overrides'] = competitor_overrides
        save_state(latest_state)
        st.success('Competitor changes saved.')
        st.rerun()
