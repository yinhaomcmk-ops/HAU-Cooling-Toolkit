
import sqlite3
import pandas as pd
import streamlit as st

from db import (
    DB_PATH,
    get_exw_history,
    get_landed_history,
    get_model_master,
    init_db,
    insert_exw,
    insert_landed,
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

st.title("Value Chain | Cost")

if "exw_upload_token" not in st.session_state:
    st.session_state["exw_upload_token"] = 0
if "landed_upload_token" not in st.session_state:
    st.session_state["landed_upload_token"] = 0


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def normalize_month_text(v) -> str:
    if pd.isna(v):
        return ""
    text = str(v).strip()
    if not text:
        return ""
    dt = pd.to_datetime(text, errors="coerce")
    if pd.notna(dt):
        return dt.strftime("%Y/%m")
    for fmt in ["%Y/%m", "%Y-%m", "%Y%m", "%m/%Y", "%m-%Y"]:
        try:
            dt = pd.to_datetime(text, format=fmt, errors="raise")
            return dt.strftime("%Y/%m")
        except Exception:
            pass
    return text


def read_upload_file(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    return pd.read_excel(uploaded_file)


def safe_float(v, default=0.0) -> float:
    try:
        if pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def load_valid_models() -> list:
    model_df = get_model_master().copy()
    if model_df.empty:
        return []
    return sorted([str(x).strip() for x in model_df["model_id"].dropna().astype(str).tolist() if str(x).strip()])


def prepare_exw_upload(df_raw: pd.DataFrame, valid_models: set):
    df = df_raw.copy()
    rename_map = {}
    for col in df.columns:
        col_clean = str(col).strip().lower()
        if col_clean in ["客户型号", "model_id", "model", "型号"]:
            rename_map[col] = "客户型号"
        elif col_clean in ["工厂结算价", "exw_cost", "exw", "结算价"]:
            rename_map[col] = "工厂结算价"
        elif col_clean in ["月份-年", "cost_month", "月份", "month"]:
            rename_map[col] = "月份-年"
    df = df.rename(columns=rename_map)
    required = ["客户型号", "工厂结算价", "月份-年"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"EXW 文件缺少字段: {', '.join(missing)}")
    df = df[required].copy()
    df["客户型号"] = df["客户型号"].astype(str).str.strip()
    df["工厂结算价"] = pd.to_numeric(df["工厂结算价"], errors="coerce")
    df["月份-年"] = df["月份-年"].apply(normalize_month_text)
    df = df.dropna(subset=["工厂结算价"])
    df = df[(df["客户型号"] != "") & (df["月份-年"] != "")]
    mask_valid = df["客户型号"].isin(valid_models)
    return df[mask_valid].reset_index(drop=True), df[~mask_valid].reset_index(drop=True)


def prepare_landed_upload(df_raw: pd.DataFrame, valid_models: set):
    df = df_raw.copy()
    rename_map = {}
    for col in df.columns:
        col_clean = str(col).strip().lower()
        if col_clean in ["客户型号", "model_id", "model", "型号"]:
            rename_map[col] = "客户型号"
        elif col_clean in ["到库成本", "landed_cost", "landed", "到港成本"]:
            rename_map[col] = "到库成本"
        elif col_clean in ["月份-年", "cost_month", "月份", "month"]:
            rename_map[col] = "月份-年"
    df = df.rename(columns=rename_map)
    required = ["客户型号", "到库成本", "月份-年"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Landed 文件缺少字段: {', '.join(missing)}")
    df = df[required].copy()
    df["客户型号"] = df["客户型号"].astype(str).str.strip()
    df["到库成本"] = pd.to_numeric(df["到库成本"], errors="coerce")
    df["月份-年"] = df["月份-年"].apply(normalize_month_text)
    df = df.dropna(subset=["到库成本"])
    df = df[(df["客户型号"] != "") & (df["月份-年"] != "")]
    mask_valid = df["客户型号"].isin(valid_models)
    return df[mask_valid].reset_index(drop=True), df[~mask_valid].reset_index(drop=True)


def update_exw_records(df_edit: pd.DataFrame):
    conn = get_conn()
    cur = conn.cursor()
    rows = []
    for _, r in df_edit.iterrows():
        rows.append((str(r["客户型号"]).strip(), safe_float(r["工厂结算价"]), normalize_month_text(r["月份-年"]), int(r["ID"])))
    cur.executemany("UPDATE exw_cost SET model_id = ?, exw_cost = ?, cost_month = ? WHERE id = ?", rows)
    conn.commit()
    conn.close()


def update_landed_records(df_edit: pd.DataFrame):
    conn = get_conn()
    cur = conn.cursor()
    rows = []
    for _, r in df_edit.iterrows():
        rows.append((str(r["客户型号"]).strip(), safe_float(r["到库成本"]), normalize_month_text(r["月份-年"]), int(r["ID"])))
    cur.executemany("UPDATE landed_cost SET model_id = ?, landed_cost = ?, cost_month = ? WHERE id = ?", rows)
    conn.commit()
    conn.close()


def delete_exw_ids(ids):
    if not ids:
        return
    conn = get_conn()
    cur = conn.cursor()
    cur.executemany("DELETE FROM exw_cost WHERE id = ?", [(int(x),) for x in ids])
    conn.commit()
    conn.close()


def delete_landed_ids(ids):
    if not ids:
        return
    conn = get_conn()
    cur = conn.cursor()
    cur.executemany("DELETE FROM landed_cost WHERE id = ?", [(int(x),) for x in ids])
    conn.commit()
    conn.close()


valid_models_list = load_valid_models()
valid_models_set = set(valid_models_list)
if not valid_models_list:
    st.warning("请先在 VC Model 页面维护型号列表，再维护成本。")
    st.stop()

exw_tab, landed_tab = st.tabs(["EXW Cost", "Landed Cost"])

with exw_tab:
    st.subheader("上传 EXW 成本")
    exw_uploaded = st.file_uploader("上传 EXW 文件", type=["xlsx", "csv"], key=f"exw_upload_{st.session_state['exw_upload_token']}")
    if exw_uploaded is not None:
        st.info(f"已选择文件：{exw_uploaded.name}")
        if st.button("确认导入 EXW 文件", key="confirm_import_exw"):
            try:
                df_raw = read_upload_file(exw_uploaded)
                valid_df, invalid_df = prepare_exw_upload(df_raw, valid_models_set)
                if valid_df.empty:
                    st.warning("文件中没有可导入的 EXW 数据。请检查字段或确认型号已在 VC Model 中维护。")
                else:
                    insert_exw(valid_df)
                    msg = f"成功导入 {len(valid_df)} 条 EXW 记录"
                    if not invalid_df.empty:
                        msg += f"；跳过 {len(invalid_df)} 条无效型号记录"
                    st.success(msg)
                    st.session_state["exw_upload_token"] += 1
                    st.rerun()
            except Exception as e:
                st.error(f"EXW 导入失败：{e}")

    st.markdown("---")
    st.subheader("手动添加 / 修改 EXW")
    c1, c2, c3, c4 = st.columns([2.2, 1.4, 1.4, 1])
    with c1:
        exw_model = st.selectbox("客户型号", valid_models_list, key="manual_exw_model")
    with c2:
        exw_cost_value = st.number_input("工厂结算价", min_value=0.0, value=0.0, format="%.2f", key="manual_exw_cost")
    with c3:
        exw_month = st.text_input("月份-年", value=pd.Timestamp.today().strftime("%Y/%m"), key="manual_exw_month")
    with c4:
        st.write("")
        st.write("")
        if st.button("保存 EXW", key="save_manual_exw", use_container_width=True):
            try:
                df_save = pd.DataFrame([{"客户型号": exw_model, "工厂结算价": exw_cost_value, "月份-年": normalize_month_text(exw_month)}])
                insert_exw(df_save)
                st.success("EXW 已保存")
                st.rerun()
            except Exception as e:
                st.error(f"保存失败：{e}")

    st.markdown("---")
    st.subheader("EXW 成本列表")
    exw_hist = get_exw_history().copy()
    exw_hist = exw_hist[exw_hist["model_id"].isin(valid_models_set)].reset_index(drop=True)
    if exw_hist.empty:
        st.info("暂无 EXW 成本记录。")
    else:
        exw_hist = exw_hist.rename(columns={"id": "ID", "model_id": "客户型号", "exw_cost": "工厂结算价", "cost_month": "月份-年"})
        exw_hist.insert(0, "删除", False)
        edited_exw = st.data_editor(
            exw_hist,
            use_container_width=True,
            hide_index=True,
            key="exw_history_editor",
            column_config={
                "删除": st.column_config.CheckboxColumn("删除"),
                "ID": st.column_config.NumberColumn("ID", disabled=True, format="%d"),
                "客户型号": st.column_config.SelectboxColumn("客户型号", options=valid_models_list, required=True),
                "工厂结算价": st.column_config.NumberColumn("工厂结算价", format="%.2f", required=True),
                "月份-年": st.column_config.TextColumn("月份-年", required=True),
            },
            disabled=["ID"],
        )
        a1, a2 = st.columns([1, 1])
        with a1:
            if st.button("保存 EXW 列表修改", key="save_exw_grid", use_container_width=True):
                try:
                    update_exw_records(edited_exw[["ID", "客户型号", "工厂结算价", "月份-年"]].copy())
                    st.success("EXW 列表修改已保存")
                    st.rerun()
                except Exception as e:
                    st.error(f"保存失败：{e}")
        with a2:
            if st.button("删除所选 EXW 记录", key="delete_exw_rows", use_container_width=True):
                ids = edited_exw.loc[edited_exw["删除"] == True, "ID"].tolist()
                if not ids:
                    st.warning("请先勾选要删除的 EXW 记录。")
                else:
                    delete_exw_ids(ids)
                    st.success(f"已删除 {len(ids)} 条 EXW 记录")
                    st.rerun()

with landed_tab:
    st.subheader("上传到库成本")
    landed_uploaded = st.file_uploader("上传到库成本文件", type=["xlsx", "csv"], key=f"landed_upload_{st.session_state['landed_upload_token']}")
    if landed_uploaded is not None:
        st.info(f"已选择文件：{landed_uploaded.name}")
        if st.button("确认导入到库成本文件", key="confirm_import_landed"):
            try:
                df_raw = read_upload_file(landed_uploaded)
                valid_df, invalid_df = prepare_landed_upload(df_raw, valid_models_set)
                if valid_df.empty:
                    st.warning("文件中没有可导入的到库成本数据。请检查字段或确认型号已在 VC Model 中维护。")
                else:
                    insert_landed(valid_df)
                    msg = f"成功导入 {len(valid_df)} 条到库成本记录"
                    if not invalid_df.empty:
                        msg += f"；跳过 {len(invalid_df)} 条无效型号记录"
                    st.success(msg)
                    st.session_state["landed_upload_token"] += 1
                    st.rerun()
            except Exception as e:
                st.error(f"到库成本导入失败：{e}")

    st.markdown("---")
    st.subheader("手动添加 / 修改到库成本")
    l1, l2, l3, l4 = st.columns([2.2, 1.4, 1.4, 1])
    with l1:
        landed_model = st.selectbox("客户型号", valid_models_list, key="manual_landed_model")
    with l2:
        landed_cost_value = st.number_input("到库成本", min_value=0.0, value=0.0, format="%.2f", key="manual_landed_cost")
    with l3:
        landed_month = st.text_input("月份-年", value=pd.Timestamp.today().strftime("%Y/%m"), key="manual_landed_month")
    with l4:
        st.write("")
        st.write("")
        if st.button("保存到库成本", key="save_manual_landed", use_container_width=True):
            try:
                df_save = pd.DataFrame([{"客户型号": landed_model, "到库成本": landed_cost_value, "月份-年": normalize_month_text(landed_month)}])
                insert_landed(df_save)
                st.success("到库成本已保存")
                st.rerun()
            except Exception as e:
                st.error(f"保存失败：{e}")

    st.markdown("---")
    st.subheader("到库成本列表")
    landed_hist = get_landed_history().copy()
    landed_hist = landed_hist[landed_hist["model_id"].isin(valid_models_set)].reset_index(drop=True)
    if landed_hist.empty:
        st.info("暂无到库成本记录。")
    else:
        landed_hist = landed_hist.rename(columns={"id": "ID", "model_id": "客户型号", "landed_cost": "到库成本", "cost_month": "月份-年"})
        landed_hist.insert(0, "删除", False)
        edited_landed = st.data_editor(
            landed_hist,
            use_container_width=True,
            hide_index=True,
            key="landed_history_editor",
            column_config={
                "删除": st.column_config.CheckboxColumn("删除"),
                "ID": st.column_config.NumberColumn("ID", disabled=True, format="%d"),
                "客户型号": st.column_config.SelectboxColumn("客户型号", options=valid_models_list, required=True),
                "到库成本": st.column_config.NumberColumn("到库成本", format="%.2f", required=True),
                "月份-年": st.column_config.TextColumn("月份-年", required=True),
            },
            disabled=["ID"],
        )
        b1, b2 = st.columns([1, 1])
        with b1:
            if st.button("保存到库成本列表修改", key="save_landed_grid", use_container_width=True):
                try:
                    update_landed_records(edited_landed[["ID", "客户型号", "到库成本", "月份-年"]].copy())
                    st.success("到库成本列表修改已保存")
                    st.rerun()
                except Exception as e:
                    st.error(f"保存失败：{e}")
        with b2:
            if st.button("删除所选到库成本记录", key="delete_landed_rows", use_container_width=True):
                ids = edited_landed.loc[edited_landed["删除"] == True, "ID"].tolist()
                if not ids:
                    st.warning("请先勾选要删除的到库成本记录。")
                else:
                    delete_landed_ids(ids)
                    st.success(f"已删除 {len(ids)} 条到库成本记录")
                    st.rerun()
