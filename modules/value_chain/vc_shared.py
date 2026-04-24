import json
from pathlib import Path
import pandas as pd

GLOBAL_STATE_FILE = Path('data') / 'global_parameters.json'
GLOBAL_STATE_FILE.parent.mkdir(exist_ok=True)

DEFAULT_GLOBALS = {
    'fx_aud_to_cny': 4.50,
    'aud_to_usd': 0.63,
    'sea_freight_usd': 1750.0,
    'clearance_aud': 3000.0,
    'insurance_rate': 0.0030,
    'singa_upcost_rate': 0.0030,
    'regular_rebate': 40.0,
    'promo_rebate': 35.0,
    'big_rebate': 32.0,
    'selling_price': 1399.0,
    'contractual_rebate_pct': 35.0,
    'other_rebate_pct': 5.0,
    'other_disc_value': 0.0,
    'loading_qty': 100,
}

PRODUCT_LINE_RULES = {
    'Refrigerator': {'hq_upcost_rate': 0.0329, 'expense_rate': 0.29, 'loading_qty': 100},
    'Wine Cabinet': {'hq_upcost_rate': 0.0329, 'expense_rate': 0.28, 'loading_qty': 80},
    'Freezer': {'hq_upcost_rate': 0.0263, 'expense_rate': 0.20, 'loading_qty': 120},
}


def month_to_date(val):
    if pd.isna(val):
        return pd.NaT
    if isinstance(val, (int, float)) and not pd.isna(val):
        try:
            return pd.to_datetime('1899-12-30') + pd.to_timedelta(int(val), unit='D')
        except Exception:
            pass
    text = str(val).strip()
    try:
        if '/' in text:
            return pd.to_datetime(text + '/01', format='%Y/%m/%d', errors='coerce')
        return pd.to_datetime(text, errors='coerce')
    except Exception:
        return pd.NaT


def safe_float(v, default=0.0):
    try:
        if pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def safe_int(v, default=0):
    try:
        if pd.isna(v):
            return default
        return int(round(float(v)))
    except Exception:
        return default


def safe_text(v, default=''):
    if pd.isna(v):
        return default
    return str(v).strip()


def load_global_params():
    if not GLOBAL_STATE_FILE.exists():
        return DEFAULT_GLOBALS.copy()
    try:
        with open(GLOBAL_STATE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        merged = DEFAULT_GLOBALS.copy()
        if isinstance(data, dict):
            merged.update(data)
        return merged
    except Exception:
        return DEFAULT_GLOBALS.copy()


def save_global_params(params):
    merged = DEFAULT_GLOBALS.copy()
    if isinstance(params, dict):
        merged.update(params)
    with open(GLOBAL_STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)


def pct_to_rate(v):
    return safe_float(v, 0.0) / 100.0


def rate_to_pct(v):
    return safe_float(v, 0.0) * 100.0

PAGE_MEMORY_FILE = Path('data') / 'page_memory.json'
PAGE_MEMORY_FILE.parent.mkdir(exist_ok=True)


def load_page_memory(page_key):
    if not PAGE_MEMORY_FILE.exists():
        return {}
    try:
        with open(PAGE_MEMORY_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict):
            page_data = data.get(page_key, {})
            return page_data if isinstance(page_data, dict) else {}
    except Exception:
        pass
    return {}


def save_page_memory(page_key, page_state):
    all_data = {}
    if PAGE_MEMORY_FILE.exists():
        try:
            with open(PAGE_MEMORY_FILE, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                all_data = loaded
        except Exception:
            all_data = {}
    all_data[page_key] = page_state if isinstance(page_state, dict) else {}
    with open(PAGE_MEMORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)


def get_saved_select_index(options, saved_value, default=0):
    try:
        if saved_value in options:
            return options.index(saved_value)
    except Exception:
        pass
    return default
