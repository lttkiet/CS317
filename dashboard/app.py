import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

import streamlit as st

st.set_page_config(
    page_title="Fraud Detection Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

from config import Config
from db import create_datasource

if "config" not in st.session_state:
    st.session_state["config"] = Config()
if "db" not in st.session_state:
    try:
        st.session_state["db"] = create_datasource(st.session_state["config"])
    except Exception as e:
        st.warning(f"Data source not available: {e}")
        st.session_state["db"] = None

config = st.session_state["config"]
db = st.session_state["db"]

from page_modules import live_feed, visualizations, demo_tester

st.sidebar.markdown(
    """
    <style>
    .sidebar-brand { text-align: center; padding: 0.5rem 0; }
    .sidebar-brand h1 { font-size: 1.3rem; margin: 0; }
    .sidebar-brand .subtitle { font-size: 0.8rem; opacity: 0.7; }
    </style>
    <div class="sidebar-brand">
        <h1>🛡️ Fraud Detection</h1>
        <div class="subtitle">Real-time · MLOps Pipeline</div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.sidebar.divider()

page = st.sidebar.radio(
    "Navigation",
    ["Live Feed", "Visualizations", "Tester"],
    label_visibility="collapsed",
)

st.sidebar.divider()

mode_icon = "☁️" if config.mode == "gcp" else "🐳"
st.sidebar.info(
    f"{mode_icon} **Mode:** {config.mode.upper()}\n\n"
    f"**Project:** `{config.project_id or 'local'}`"
)

if db is not None:
    try:
        total = db.get_total_count()
        st.sidebar.metric("Total Predictions", f"{total:,}")
    except Exception:
        st.sidebar.warning("Cannot connect to data source")
else:
    st.sidebar.error("Data source not initialized")

pages_map = {
    "Live Feed": live_feed,
    "Visualizations": visualizations,
    "Tester": demo_tester,
}

page_module = pages_map[page]
if page == "Tester":
    page_module.render(db, config)
else:
    page_module.render(db)
