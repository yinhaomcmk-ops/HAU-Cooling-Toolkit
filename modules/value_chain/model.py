import streamlit as st
import pandas as pd
from db import init_db, get_model_master, upsert_model_master, execute_sql

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
st.title("Value Chain | Model")

# =========================
# 上传 Model
# =========================
st.subheader("上传 Model 文件")

if "model_upload_token" not in st.session_state:
    st.session_state["model_upload_token"] = 0

uploaded_file = st.file_uploader(
    "上传 Model 文件（需包含：客户型号 / 产品线 / 品类）",
    type=["xlsx", "csv"],
    key=f"model_upload_{st.session_state['model_upload_token']}"
)

def read_file(file):
    if file.name.endswith(".csv"):
        return pd.read_csv(file)
    return pd.read_excel(file)

if uploaded_file is not None:
    st.success(f"已选择文件：{uploaded_file.name}")

    if st.button("导入 Model 文件"):
        df = read_file(uploaded_file)

        required_cols = ["客户型号", "产品线", "品类"]
        if not all(col in df.columns for col in required_cols):
            st.error("文件必须包含：客户型号 / 产品线 / 品类")
        else:
            df = df[required_cols].copy()
            df = df.dropna(subset=["客户型号"])
            df["客户型号"] = df["客户型号"].astype(str).str.strip()
            df = df[df["客户型号"] != ""]

            upsert_model_master(df)

            st.success(f"成功导入 {len(df)} 条 Model")
            st.session_state["model_upload_token"] += 1
            st.rerun()

# =========================
# 手动新增
# =========================
st.subheader("手动添加 / 修改 Model")

c1, c2, c3, c4 = st.columns(4)

with c1:
    model_id = st.text_input("客户型号")

with c2:
    product_line = st.text_input("产品线")

with c3:
    category = st.text_input("品类")

with c4:
    if st.button("保存 Model"):
        if model_id.strip():
            df = pd.DataFrame([{
                "客户型号": model_id,
                "产品线": product_line,
                "品类": category
            }])
            upsert_model_master(df)
            st.success("保存成功")
            st.rerun()
        else:
            st.warning("客户型号不能为空")

# =========================
# Model 列表（可编辑）
# =========================
st.subheader("Model 列表")

df = get_model_master()

if df.empty:
    st.info("当前没有 Model 数据")
    st.stop()

df_display = df.rename(columns={
    "model_id": "客户型号",
    "product_line": "产品线",
    "category": "品类"
})

edited_df = st.data_editor(
    df_display,
    num_rows="dynamic",
    use_container_width=True,
    key="model_editor"
)

# =========================
# 保存修改
# =========================
c1, c2 = st.columns(2)

with c1:
    if st.button("保存 Model 列表修改"):
        df_to_save = edited_df.copy()
        df_to_save = df_to_save.dropna(subset=["客户型号"])
        df_to_save["客户型号"] = df_to_save["客户型号"].astype(str)

        upsert_model_master(df_to_save)
        st.success("更新成功")
        st.rerun()

# =========================
# 删除
# =========================
with c2:
    delete_ids = st.multiselect(
        "选择要删除的型号",
        df_display["客户型号"].tolist()
    )

    if st.button("删除所选 Model"):
        if delete_ids:
            execute_sql(
                f"DELETE FROM model_master WHERE model_id IN ({','.join(['?']*len(delete_ids))})",
                delete_ids
            )
            st.success("删除成功")
            st.rerun()
        else:
            st.warning("请选择要删除的型号")