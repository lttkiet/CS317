import sys
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from training.constants import FRAUD_THRESHOLD


def render(db):
    st.header("Fraud Visualizations")

    time_range = st.selectbox("Time range", [1, 6, 12, 24, 48, 168], format_func=lambda h: f"Last {h}h" if h < 24 else f"Last {h//24}d", index=3)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Fraud Probability Distribution")
        try:
            df_prob = db.get_probability_distribution(hours=time_range)
            if not df_prob.empty:
                fig = px.bar(
                    df_prob,
                    x="prob_bucket",
                    y="count",
                    labels={"prob_bucket": "Fraud Probability", "count": "Count"},
                    color="count",
                    color_continuous_scale="Reds",
                )
                fig.add_vline(x=FRAUD_THRESHOLD, line_dash="dash", line_color="red",
                              annotation_text=f"Threshold ({FRAUD_THRESHOLD})")
                fig.update_layout(height=350, margin=dict(l=20, r=20, t=20, b=20))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data available")
        except Exception as e:
            st.error(f"Query failed: {e}")

        st.subheader("Fraud Rate Over Time")
        try:
            df_trend = db.get_fraud_rate_trend(hours=time_range)
            if not df_trend.empty:
                fig = px.line(
                    df_trend,
                    x="bucket",
                    y="fraud_rate",
                    markers=True,
                    labels={"bucket": "Time", "fraud_rate": "Fraud Rate (%)"},
                )
                fig.add_hline(y=1, line_dash="dash", line_color="green",
                              annotation_text="1% Target")
                fig.add_hline(y=3, line_dash="dash", line_color="red",
                              annotation_text="3% Alert")
                fig.update_layout(height=350, margin=dict(l=20, r=20, t=20, b=20))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data available")
        except Exception as e:
            st.error(f"Query failed: {e}")

    with col2:
        st.subheader("Top Fraudulent Terminals")
        try:
            df_term = db.get_top_terminals(hours=time_range)
            if not df_term.empty:
                fig = px.bar(
                    df_term,
                    x="terminal_id",
                    y="fraud_count",
                    labels={"terminal_id": "Terminal ID", "fraud_count": "Fraud Count"},
                    color="avg_probability",
                    color_continuous_scale="Reds",
                    text="fraud_count",
                )
                fig.update_layout(height=350, margin=dict(l=20, r=20, t=20, b=20))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data available")
        except Exception as e:
            st.error(f"Query failed: {e}")

        st.subheader("Transaction Amount Distribution")
        try:
            df_latest = db.get_latest_predictions(limit=5000)
            if not df_latest.empty and "tx_amount" in df_latest.columns:
                df_latest["category"] = df_latest["is_fraud_predicted"].map({0: "Legitimate", 1: "Fraud"})
                fig = px.box(
                    df_latest,
                    x="category",
                    y="tx_amount",
                    color="category",
                    labels={"category": "", "tx_amount": "Amount ($)"},
                    color_discrete_map={"Legitimate": "#636efa", "Fraud": "#ef553b"},
                )
                fig.update_layout(
                    height=350,
                    showlegend=False,
                    margin=dict(l=20, r=20, t=20, b=20),
                    yaxis_type="log",
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data available")
        except Exception as e:
            st.error(f"Query failed: {e}")

    st.subheader("Fraud Probability vs Amount")
    try:
        df_scatter = db.get_latest_predictions(limit=2000)
        if not df_scatter.empty and "tx_amount" in df_scatter.columns:
            df_scatter["category"] = df_scatter["is_fraud_predicted"].map({0: "Legitimate", 1: "Fraud"})
            fig = px.scatter(
                df_scatter,
                x="tx_amount",
                y="fraud_probability",
                color="category",
                labels={"tx_amount": "Amount ($)", "fraud_probability": "Fraud Probability"},
                color_discrete_map={"Legitimate": "#636efa", "Fraud": "#ef553b"},
                opacity=0.5,
            )
            fig.add_hline(y=FRAUD_THRESHOLD, line_dash="dash", line_color="red")
            fig.update_layout(height=400, margin=dict(l=20, r=20, t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available")
    except Exception as e:
        st.error(f"Query failed: {e}")
