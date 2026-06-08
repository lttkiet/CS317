import sys
import os
import time

import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from training.constants import FRAUD_THRESHOLD


def render(db):
    st.header("Live Prediction Feed")

    with st.expander("Auto-refresh settings", expanded=False):
        col1, col2 = st.columns(2)
        refresh_interval = col1.slider("Refresh interval (seconds)", 2, 30, 5, key="lr_refresh")
        page_size = col2.slider("Rows to show", 10, 200, 100, key="lr_page")

    try:
        df = db.get_latest_predictions(limit=page_size)
        total = db.get_total_count()
        fraud = db.get_fraud_count()
        last = db.get_last_update_time()
    except Exception as e:
        st.error(f"Query failed: {e}")
        return

    fraud_rate = (fraud / total * 100) if total > 0 else 0.0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Transactions Processed", f"{total:,}")
    m2.metric("Fraud Predicted", f"{fraud:,}")
    m3.metric("Fraud Rate", f"{fraud_rate:.2f}%")
    m4.metric("Last Prediction", last.strftime("%H:%M:%S") if last is not None else "—")

    if not df.empty:
        display_cols = [
            "transaction_id", "customer_id", "terminal_id", "tx_amount",
            "fraud_probability", "is_fraud_predicted", "predicted_at",
        ]
        display_cols = [c for c in display_cols if c in df.columns]
        display = df[display_cols].copy()

        def color_prob(val):
            if val >= FRAUD_THRESHOLD * 1.5:
                return "background-color: #ff4b4b; color: white"
            if val >= FRAUD_THRESHOLD:
                return "background-color: #ffa500; color: black"
            return ""

        styled = display.style.map(color_prob, subset=["fraud_probability"]).format(
            {"tx_amount": "${:.2f}", "fraud_probability": "{:.4f}"}
        )
        st.dataframe(styled, use_container_width=True, height=500)
    else:
        st.info("No predictions yet. Is the pipeline running?")

    st.caption(f"Next refresh in {refresh_interval}s...")
    time.sleep(refresh_interval)
    st.rerun()
