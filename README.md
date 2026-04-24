# Hisense Toolkit

## Structure
- `app.py`: unified entry
- `modules/value_chain/`: Value Chain modules
- `modules/sales_heatmap/`: Sales Heatmap module
- `assets/`: logo and static assets
- `data/app_data.db`: shared SQLite database file

## Run
```bash
pip install -r requirements.txt
streamlit run app.py
```

## Current integration approach
- Single entry app
- Shared database file (`data/app_data.db`)
- Separate tables by business module
- Legacy Value Chain logic preserved and mounted as submodules


## 已恢复的数据

- Value Chain 旧数据库已并入 `data/app_data.db`
- Sales Heatmap 旧数据库已并入 `data/app_data.db`
- Value Chain 的全局参数、筛选记忆、Overall/Special/Landed 状态已恢复
