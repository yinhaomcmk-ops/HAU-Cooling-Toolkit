from pathlib import Path
import runpy
import sys
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent
ASSET_LOGO = BASE_DIR / "assets" / "hisense_logo.png"
HISENSE_LOGO = BASE_DIR / "assets" / "hisense.png"

st.set_page_config(
    page_title="Hisense Cooling Analysis Toolkit",
    page_icon=str(ASSET_LOGO) if ASSET_LOGO.exists() else "❄️",
    layout="wide",
    initial_sidebar_state="expanded",
)

VALUE_CHAIN_PAGES = {
    "Overall": BASE_DIR / "modules" / "value_chain" / "overall.py",
    "Special": BASE_DIR / "modules" / "value_chain" / "special.py",
    "Landed": BASE_DIR / "modules" / "value_chain" / "landed.py",
}
SALES_HEATMAP_PAGES = {"Analysis": BASE_DIR / "modules" / "sales_heatmap" / "analysis.py"}
SALES_AGENT_PAGES = {"Analysis": BASE_DIR / "modules" / "sales_ai" / "page.py"}
DATABASE_PAGE = BASE_DIR / "modules" / "database" / "page.py"

MODULES = ["Value Chain", "Sales Heatmap", "Sales Agent", "Database"]
ROLE_PERMISSIONS = {
    "admin": set(MODULES),
    "KAM": {"Sales Heatmap", "Sales Agent"},
    "AM": {"Sales Heatmap", "Sales Agent"},
}
DEFAULT_USER_ROLES = {
    "admin": "admin",
    "KAM": "KAM",
    "AM": "AM",
}

# -----------------------------
# Session State Init
# -----------------------------
def _init_state() -> None:
    defaults = {
        "main_module": "Home",
        "vc_page": "Overall",
        "sh_page": "Analysis",
        "sa_page": "Analysis",
        "authenticated": False,
        "auth_user": None,
        "auth_role": None,
        "pending_module": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

_init_state()

# -----------------------------
# Auth / Permissions
# -----------------------------
def load_credentials() -> dict:
    try:
        return dict(st.secrets["app_credentials"])
    except Exception:
        return {"admin": "admin", "KAM": "kam", "AM": "am"}


def load_user_roles() -> dict:
    roles = DEFAULT_USER_ROLES.copy()
    try:
        roles.update(dict(st.secrets.get("app_roles", {})))
    except Exception:
        pass
    return roles


def check_login(username: str, password: str) -> bool:
    credentials = load_credentials()
    return username in credentials and str(credentials[username]) == str(password)


def role_for_user(username: str | None) -> str | None:
    if not username:
        return None
    return load_user_roles().get(username, username if username in ROLE_PERMISSIONS else None)


def allowed_modules() -> set[str]:
    role = st.session_state.get("auth_role")
    return ROLE_PERMISSIONS.get(role, set())


def has_access(module_name: str) -> bool:
    return module_name in allowed_modules()


def logout() -> None:
    st.session_state["authenticated"] = False
    st.session_state["auth_user"] = None
    st.session_state["auth_role"] = None
    st.session_state["pending_module"] = None


def _query_payload(module: str, page: str | None = None) -> dict:
    payload = {"module": module}
    if st.session_state.get("authenticated") and st.session_state.get("auth_user"):
        payload["auth"] = "1"
        payload["user"] = st.session_state["auth_user"]
    if page:
        payload["page"] = page
    return payload


def do_login(username: str, password: str, target_module: str | None = None) -> bool:
    if not check_login(username, password):
        return False
    st.session_state["authenticated"] = True
    st.session_state["auth_user"] = username
    st.session_state["auth_role"] = role_for_user(username)
    st.session_state["pending_module"] = None

    target = target_module or st.session_state.get("main_module", "Home")
    if target == "Login":
        target = "Home"
    if target in MODULES and not has_access(target):
        target = "Home"
        st.warning("Login successful, but this account does not have access to the requested module.")
    st.session_state["main_module"] = target
    st.query_params.update(_query_payload(target))
    return True

# -----------------------------
# Navigation
# -----------------------------
def sync_from_query_params() -> None:
    q = st.query_params
    module = q.get("module", None)
    page = q.get("page", None)
    auth = q.get("auth", None)
    auth_user = q.get("user", None)

    if auth == "1" and auth_user:
        st.session_state["authenticated"] = True
        st.session_state["auth_user"] = auth_user
        st.session_state["auth_role"] = role_for_user(auth_user)

    if module:
        if module in MODULES:
            if not st.session_state.get("authenticated"):
                st.session_state["pending_module"] = module
                st.session_state["main_module"] = "Login"
                return
            if not has_access(module):
                st.session_state["pending_module"] = module
                st.session_state["main_module"] = "No Access"
                return
        st.session_state["main_module"] = module

    current_module = st.session_state.get("main_module")
    if page:
        if current_module == "Value Chain" and page in VALUE_CHAIN_PAGES:
            st.session_state["vc_page"] = page
        elif current_module == "Sales Heatmap" and page in SALES_HEATMAP_PAGES:
            st.session_state["sh_page"] = page
        elif current_module == "Sales Agent" and page in SALES_AGENT_PAGES:
            st.session_state["sa_page"] = page


def set_location(module_name: str, page_name: str | None = None) -> None:
    if module_name in MODULES:
        if not st.session_state.get("authenticated"):
            st.session_state["pending_module"] = module_name
            st.session_state["main_module"] = "Login"
            st.query_params.clear()
            return
        if not has_access(module_name):
            st.session_state["pending_module"] = module_name
            st.session_state["main_module"] = "No Access"
            st.query_params.update(_query_payload("No Access"))
            return

    st.session_state["main_module"] = module_name
    if page_name:
        if module_name == "Value Chain":
            st.session_state["vc_page"] = page_name
        elif module_name == "Sales Heatmap":
            st.session_state["sh_page"] = page_name
        elif module_name == "Sales Agent":
            st.session_state["sa_page"] = page_name
    st.query_params.update(_query_payload(module_name, page_name))


def go_home() -> None:
    st.session_state["main_module"] = "Home"
    if st.session_state.get("authenticated") and st.session_state.get("auth_user"):
        st.query_params.update(_query_payload("Home"))
    else:
        st.query_params.clear()


def run_module(script_path: Path) -> None:
    if not script_path.exists():
        st.error(f"Module file not found: {script_path}")
        st.stop()
    for p in [str(script_path.parent), str(BASE_DIR)]:
        if p not in sys.path:
            sys.path.insert(0, p)
    runpy.run_path(str(script_path), run_name="__main__")

# -----------------------------
# Shared UI
# -----------------------------
def render_sidebar_auth(force_login_form: bool = False) -> None:
    with st.sidebar:
        if HISENSE_LOGO.exists():
            st.image(str(HISENSE_LOGO), width=180)
        st.markdown("### Account")
        if st.session_state.get("authenticated"):
            role = st.session_state.get("auth_role") or "-"
            st.success(f"Logged in as: {st.session_state['auth_user']} / Role: {role}")
            if st.button("Logout", key="sidebar_logout", use_container_width=True):
                logout()
                go_home()
                st.rerun()
        else:
            show_expanded = force_login_form or bool(st.session_state.get("pending_module"))
            with st.expander("Login", expanded=show_expanded):
                username = st.text_input("Username", key="sidebar_login_username")
                password = st.text_input("Password", type="password", key="sidebar_login_password")
                if st.button("Login", key="sidebar_login_btn", use_container_width=True):
                    target_module = st.session_state.get("pending_module")
                    if do_login(username, password, target_module=target_module):
                        st.rerun()
                    else:
                        st.error("Invalid username or password")
        st.markdown("---")


def render_module_sidebar(module_name: str, page_map: dict | None = None, state_key: str | None = None) -> None:
    with st.sidebar:
        if HISENSE_LOGO.exists():
            st.image(str(HISENSE_LOGO), width=180)
        st.markdown(f"### {module_name}")
        role = st.session_state.get("auth_role") or "-"
        st.success(f"Logged in as: {st.session_state['auth_user']} / Role: {role}")
        if st.button("← Homepage", key=f"back_home_{module_name}", use_container_width=True):
            go_home(); st.rerun()
        if st.button("Logout", key=f"logout_{module_name}", use_container_width=True):
            logout(); go_home(); st.rerun()
        if page_map and state_key:
            st.markdown("---")
            keys = list(page_map.keys())
            current = st.session_state.get(state_key, keys[0])
            selected = st.radio("Feature Menu", keys, index=keys.index(current) if current in keys else 0)
            if selected != current:
                set_location(module_name, selected)
                st.rerun()


def render_login_page() -> None:
    render_sidebar_auth(force_login_form=True)
    st.markdown('<div class="hero-wrap">', unsafe_allow_html=True)
    st.markdown('<div class="hero-title">Please Login</div>', unsafe_allow_html=True)
    st.markdown('<div class="hero-subtitle">Please login in the sidebar to access this module.</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


def render_no_access_page() -> None:
    render_sidebar_auth(force_login_form=False)
    target = st.session_state.get("pending_module") or "this module"
    st.markdown('<div class="hero-wrap">', unsafe_allow_html=True)
    st.markdown('<div class="hero-title">No Access</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="hero-subtitle">Your current role cannot access {target}.</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


def render_module_card(col, pill, emoji, title, text, note, module_name, accent="teal") -> None:
    user_allowed = has_access(module_name) if st.session_state.get("authenticated") else False
    logged_in = st.session_state.get("authenticated")
    is_disabled = logged_in and not user_allowed
    href = f"?module={module_name.replace(' ', '%20')}"
    if logged_in and st.session_state.get("auth_user"):
        href += f"&auth=1&user={st.session_state['auth_user']}"
    status = "ACCESS LOCKED" if is_disabled else ("NOT AVALIABLE" if not logged_in else "AVALIABLE")
    cls = f"module-card accent-{accent}" + (" disabled-card" if is_disabled else "")
    anchor_open = f'<a class="card-anchor" href="{href}" target="_self">' if not is_disabled else '<div class="card-anchor disabled-anchor">'
    anchor_close = '</a>' if not is_disabled else '</div>'
    with col:
        st.markdown(
            f"""
            {anchor_open}
                <div class="{cls}">
                    <div class="module-pill">{pill}</div>
                    <div class="module-status">{status}</div>
                    <div class="module-icon">{emoji}</div>
                    <div class="module-title">{title}</div>
                    <div class="module-text">{text}</div>
                    <div class="module-note">{note}</div>
                </div>
            {anchor_close}
            """,
            unsafe_allow_html=True,
        )

sync_from_query_params()

st.markdown(
    """
    <style>
    .stApp {background: radial-gradient(circle at top left, rgba(10,157,149,0.16), transparent 24%), radial-gradient(circle at top right, rgba(79,70,229,0.13), transparent 20%), linear-gradient(180deg, #0b1116 0%, #101820 100%); color: #E5EEF5;}
    [data-testid="stSidebar"] {background: linear-gradient(180deg, rgba(0,118,112,0.18), rgba(255,255,255,0.015)); border-right: 1px solid rgba(255,255,255,0.08);}
    .block-container {padding-top: 1.35rem; padding-bottom: 1rem; max-width: 1420px;}
    h1, h2, h3, .stMarkdown, label, p, span, div {color: #E5EEF5;}
    .hero-wrap {max-width: 1180px; margin: 0 auto 1rem auto; text-align: center;}
    .hero-title {font-size: 44px; font-weight: 900; margin-top: 8px; margin-bottom: 6px; letter-spacing: .2px;}
    .hero-subtitle {color: #9FB0BD; font-size: 15px; margin-bottom: 28px;}
    a.card-anchor, .disabled-anchor {display:block; text-decoration:none!important; color:inherit!important;}
    .module-card {position:relative; z-index:1; border:1px solid rgba(255,255,255,.12); border-radius:24px; padding:26px 24px; min-height:320px; box-shadow:0 18px 42px rgba(0,0,0,.28); transition:transform .18s ease,border-color .18s ease,box-shadow .18s ease,filter .18s ease; cursor:pointer; overflow:hidden; background:linear-gradient(180deg,rgba(255,255,255,.06),rgba(255,255,255,.028));}
    .module-card::before {content:""; position:absolute; inset:0; opacity:.85; z-index:-1;}
    .module-card::after {content:""; position:absolute; right:-42px; bottom:-46px; width:170px; height:170px; border-radius:999px; opacity:.55; pointer-events:none;}
    .module-card:hover {transform:translateY(-5px); box-shadow:0 24px 50px rgba(0,0,0,.34);}
    .accent-teal::before{background:linear-gradient(135deg,rgba(13,148,136,.22),rgba(6,78,59,.04));}.accent-teal::after{background:radial-gradient(circle,rgba(45,212,191,.42),transparent 70%)}
    .accent-blue::before{background:linear-gradient(135deg,rgba(37,99,235,.22),rgba(30,64,175,.04));}.accent-blue::after{background:radial-gradient(circle,rgba(96,165,250,.42),transparent 70%)}
    .accent-purple::before{background:linear-gradient(135deg,rgba(124,58,237,.22),rgba(88,28,135,.04));}.accent-purple::after{background:radial-gradient(circle,rgba(167,139,250,.42),transparent 70%)}
    .accent-amber::before{background:linear-gradient(135deg,rgba(245,158,11,.20),rgba(120,53,15,.04));}.accent-amber::after{background:radial-gradient(circle,rgba(251,191,36,.40),transparent 70%)}
    .disabled-card {filter:grayscale(1); opacity:.42; cursor:not-allowed; box-shadow:none;}
    .disabled-card:hover {transform:none; border-color:rgba(255,255,255,.12); box-shadow:none;}
    .module-pill {display:inline-block; background:rgba(255,255,255,.08); border:1px solid rgba(255,255,255,.16); padding:6px 10px; border-radius:999px; font-size:12px; font-weight:800; margin-bottom:14px;}
    .module-status {position:absolute; top:22px; right:22px; font-size:11px; font-weight:900; color:#A7F3D0; letter-spacing:.08em;}
    .disabled-card .module-status {color:#CBD5E1;}
    .module-icon {font-size:44px; line-height:1; margin-bottom:16px; filter:drop-shadow(0 4px 12px rgba(255,255,255,.10));}
    .module-title {font-size:28px; font-weight:900; margin-bottom:16px; color:#F1F7FB!important;}
    .module-text {color:#A6B6C3!important; font-size:14px; line-height:1.6; min-height:106px;}
    .module-note {color:#7F95A6!important; font-size:12px; margin-top:34px;}
    div[data-testid="stMetric"] {background:rgba(255,255,255,.04); border:1px solid rgba(255,255,255,.08); padding:10px 14px; border-radius:14px;}
    div[data-testid="stDataFrame"], div[data-testid="stTable"] {border:1px solid rgba(255,255,255,.08); border-radius:14px; overflow:hidden; background:rgba(255,255,255,.02);}
    .stButton>button, .stDownloadButton>button {width:100%; border-radius:12px; border:1px solid rgba(103,232,218,.22); background:linear-gradient(180deg,rgba(10,157,149,.30),rgba(10,157,149,.18)); color:white; font-weight:800; min-height:44px;}
    .stButton>button:hover, .stDownloadButton>button:hover {border-color:rgba(103,232,218,.34); background:linear-gradient(180deg,rgba(10,157,149,.42),rgba(10,157,149,.24)); color:white;}
    div[data-baseweb="select"]>div, div[data-baseweb="input"]>div, textarea, input {background:rgba(255,255,255,.03)!important;}
    </style>
    """,
    unsafe_allow_html=True,
)

main_module = st.session_state["main_module"]

if main_module == "Home":
    render_sidebar_auth()
    st.markdown('<div class="hero-wrap">', unsafe_allow_html=True)
    st.markdown('<div class="hero-title">Hisense Cooling Analysis Toolkit</div>', unsafe_allow_html=True)
    st.markdown('<div class="hero-subtitle">Role-based modules with a shared data layer</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)
    modules = [
        ("VALUE CHAIN", "📊", "Value Chain", "Pricing, margin, rebate, landed cost, and scenario simulation for cooling products.", "Overall/EXW/Landed Cost", "Value Chain", "teal"),
        ("SALES HEATMAP", "🌍", "Sales Heatmap", "Store-level sales distribution, heatmap and opportunity gap analysis.", "Cluster Map/Heat Map", "Sales Heatmap", "blue"),
        ("SALES AGENT", "🤖", "Sales Agent", "AI sales diagnosis combining sales, product master, heatmap and value chain context.", "Generative Sales Summary", "Sales Agent", "purple"),
        ("DATABASE", "🗂️", "Database", "Shared maintenance for product master, cost, store sales and Sales Agent sellout data.", "DBMS", "Database", "amber"),
    ]
    for i in range(0, len(modules), 3):
        cols = st.columns(3, gap="large")
        for idx, col in enumerate(cols):
            if i + idx < len(modules):
                render_module_card(col, *modules[i + idx])
            else:
                col.empty()
        st.markdown("<div style='height: 30px'></div>", unsafe_allow_html=True)

elif main_module == "Login":
    render_login_page()
elif main_module == "No Access":
    render_no_access_page()
elif main_module == "Value Chain":
    if not st.session_state.get("authenticated") or not has_access("Value Chain"):
        st.session_state["pending_module"] = "Value Chain"; st.session_state["main_module"] = "Login" if not st.session_state.get("authenticated") else "No Access"; st.rerun()
    render_module_sidebar("Value Chain", VALUE_CHAIN_PAGES, "vc_page")
    run_module(VALUE_CHAIN_PAGES[st.session_state["vc_page"]])
elif main_module == "Sales Heatmap":
    if not st.session_state.get("authenticated") or not has_access("Sales Heatmap"):
        st.session_state["pending_module"] = "Sales Heatmap"; st.session_state["main_module"] = "Login" if not st.session_state.get("authenticated") else "No Access"; st.rerun()
    render_module_sidebar("Sales Heatmap", SALES_HEATMAP_PAGES, "sh_page")
    run_module(SALES_HEATMAP_PAGES[st.session_state["sh_page"]])
elif main_module == "Sales Agent":
    if not st.session_state.get("authenticated") or not has_access("Sales Agent"):
        st.session_state["pending_module"] = "Sales Agent"; st.session_state["main_module"] = "Login" if not st.session_state.get("authenticated") else "No Access"; st.rerun()
    render_module_sidebar("Sales Agent", SALES_AGENT_PAGES, "sa_page")
    run_module(SALES_AGENT_PAGES[st.session_state["sa_page"]])
elif main_module == "Database":
    if not st.session_state.get("authenticated") or not has_access("Database"):
        st.session_state["pending_module"] = "Database"; st.session_state["main_module"] = "Login" if not st.session_state.get("authenticated") else "No Access"; st.rerun()
    render_module_sidebar("Database")
    run_module(DATABASE_PAGE)
else:
    go_home(); st.rerun()
