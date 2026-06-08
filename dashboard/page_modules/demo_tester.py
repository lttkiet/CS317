import sys
import os
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import time
import random
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from training.constants import FRAUD_THRESHOLD

from kafka_producer import DashboardKafkaProducer
try:
    from direct_inference import predict as direct_predict
    DIRECT_INFERENCE_AVAILABLE = True
except ImportError:
    DIRECT_INFERENCE_AVAILABLE = False


def _generate_tx_id():
    return random.randint(1, 2**31)


def _build_gauge(probability: float) -> go.Figure:
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=probability * 100,
        number={"suffix": "%", "font": {"size": 36}},
        title={"text": "Fraud Probability", "font": {"size": 18}},
        delta={"reference": int(FRAUD_THRESHOLD * 100), "increasing": {"color": "red"}, "decreasing": {"color": "green"}},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1, "tickcolor": "white"},
            "bar": {"color": "white", "thickness": 0.3},
            "steps": [
                {"range": [0, int(FRAUD_THRESHOLD * 100 / 2)], "color": "#00CC96"},
                {"range": [int(FRAUD_THRESHOLD * 100 / 2), int(FRAUD_THRESHOLD * 100)], "color": "#FFA500"},
                {"range": [int(FRAUD_THRESHOLD * 100), 100], "color": "#FF4B4B"},
            ],
            "threshold": {
                "line": {"color": "red", "width": 4},
                "thickness": 0.75,
                "value": int(FRAUD_THRESHOLD * 100),
            },
        },
    ))
    fig.update_layout(
        height=300,
        margin=dict(l=30, r=30, t=30, b=30),
        paper_bgcolor="rgba(0,0,0,0)",
        font={"color": "white"},
    )
    return fig


def _build_pipeline_timeline(submit_time, result_time) -> go.Figure:
    fig = go.Figure()
    nodes = [
        {"label": "Dashboard<br>Submitted", "time": submit_time},
        {"label": "Kafka<br>Produced", "time": submit_time + 0.5},
        {"label": "Spark<br>Processed", "time": result_time - 1.0},
        {"label": "BigQuery<br>Written", "time": result_time - 0.3},
        {"label": "Dashboard<br>Displayed", "time": result_time},
    ]

    y_positions = list(range(len(nodes)))
    times = [n["time"] for n in nodes]
    labels = [n["label"] for n in nodes]

    fig.add_trace(go.Scatter(
        x=times,
        y=y_positions,
        mode="lines+markers+text",
        line=dict(color="#00CC96", width=3),
        marker=dict(size=12, color="#636EFA"),
        text=labels,
        textposition="middle right",
        textfont=dict(size=10, color="white"),
        hoverinfo="x",
    ))

    fig.update_layout(
        title="Pipeline Timeline (seconds)",
        height=250,
        xaxis_title="Elapsed time (s)",
        yaxis=dict(visible=False),
        margin=dict(l=10, r=10, t=30, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": "white"},
    )
    return fig


def _generate_tx_params(tx_type: str):
    now = datetime.now(timezone.utc)
    sim_start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=7)
    elapsed = now - sim_start

    if tx_type == "fraud":
        amount = round(random.uniform(500.0, 5000.0), 2)
        customer_id = random.randint(1, 999999)
        terminal_id = random.randint(1, 999999)
        night_hour = random.choice([0, 1, 2, 3, 22, 23])
        fake_time = now.replace(hour=night_hour, minute=random.randint(0, 59))
    else:
        amount = round(random.uniform(5.0, 200.0), 2)
        customer_id = random.randint(1, 999999)
        terminal_id = random.randint(1, 999999)
        day_hour = random.randint(9, 17)
        fake_time = now.replace(hour=day_hour, minute=random.randint(0, 59))

    fake_elapsed = fake_time - sim_start
    tx_time_seconds = int(fake_elapsed.total_seconds())
    tx_time_days = tx_time_seconds // 86400

    return {
        "CUSTOMER_ID": customer_id,
        "TERMINAL_ID": terminal_id,
        "TX_AMOUNT": amount,
        "TX_TIME_SECONDS": tx_time_seconds,
        "TX_TIME_DAYS": tx_time_days,
        "TX_DATETIME": fake_time.strftime("%Y-%m-%dT%H:%M:%S.") + f"{fake_time.microsecond // 1000:03d}Z",
    }


def render(db, config):
    st.header("Tester")
    st.caption("Generate and submit transactions through the real pipeline to see fraud predictions.")

    if "demo_producer" not in st.session_state:
        try:
            st.session_state["demo_producer"] = DashboardKafkaProducer(config)
        except Exception as e:
            st.error(f"Failed to create Kafka producer: {e}")
            st.info(
                "To use the Ad-hoc Tester, set the `KAFKA_SA_EMAIL` environment variable "
                "(the service account email for Kafka SASL authentication)."
            )
            st.info("The pipeline works independently — predictions can still be viewed on other pages.")
            return

    producer = st.session_state["demo_producer"]

    with st.form("demo_form"):
        cols = st.columns(3)
        tx_count = cols[0].number_input("Number of Transactions", min_value=1, max_value=100, value=1, step=1)
        tx_type = cols[1].selectbox("Transaction Type", options=["normal", "fraud"], format_func={"normal": "Normal ✅", "fraud": "Fraud 🚨"}.get, index=0)
        submitted = st.form_submit_button(f"Generate {tx_count} {'Fraud' if tx_type == 'fraud' else 'Normal'} Transaction{'s' if tx_count > 1 else ''}", type="primary", use_container_width=True)

    if submitted:
        submit_time = time.time()
        txs = []
        for _ in range(tx_count):
            params = _generate_tx_params(tx_type)
            tx_id = _generate_tx_id()
            tx_data = {
                "TRANSACTION_ID": tx_id,
                "TX_DATETIME": params["TX_DATETIME"],
                "CUSTOMER_ID": params["CUSTOMER_ID"],
                "TERMINAL_ID": params["TERMINAL_ID"],
                "TX_AMOUNT": params["TX_AMOUNT"],
                "TX_TIME_SECONDS": params["TX_TIME_SECONDS"],
                "TX_TIME_DAYS": params["TX_TIME_DAYS"],
            }
            txs.append(tx_data)
            producer.produce_transaction(tx_data)

        st.success(f"✅ {tx_count} transaction{'s' if tx_count > 1 else ''} produced to `transactions_topic`")

        if tx_count == 1:
            tx_data = txs[0]

            with st.status("Processing transaction...", expanded=True) as status:
                st.write(f"**Transaction ID:** {tx_data['TRANSACTION_ID']}")
                st.write(f"**Amount:** ${tx_data['TX_AMOUNT']:.2f}")
                st.write(f"**Customer:** {tx_data['CUSTOMER_ID']}  **Terminal:** {tx_data['TERMINAL_ID']}")

                try:
                    if DIRECT_INFERENCE_AVAILABLE:
                        st.write("⚡ Running direct inference...")
                        now = datetime.now(timezone.utc)
                        direct_result = direct_predict(
                            customer_id=tx_data["CUSTOMER_ID"],
                            terminal_id=tx_data["TERMINAL_ID"],
                            tx_amount=tx_data["TX_AMOUNT"],
                            tx_time_seconds=tx_data["TX_TIME_SECONDS"],
                            tx_time_days=tx_data["TX_TIME_DAYS"],
                            tx_datetime=now,
                        )
                    else:
                        direct_result = None

                    if direct_result is not None:
                        prob = direct_result["fraud_probability"]
                        pred = direct_result["is_fraud_predicted"]
                        result_time = time.time()

                        status.update(label="✅ Prediction received (direct inference)", state="complete")
                        st.balloons()

                        col1, col2 = st.columns(2)
                        with col1:
                            st.plotly_chart(_build_gauge(prob), use_container_width=True)
                        with col2:
                            st.metric("Prediction", "⚠️ FRAUD" if pred == 1 else "✅ Legitimate")
                            st.metric("Fraud Probability", f"{prob*100:.2f}%",
                                      delta=f"{'Above' if prob >= FRAUD_THRESHOLD else 'Below'} threshold ({int(FRAUD_THRESHOLD * 100)}%)",
                                      delta_color="inverse")
                            st.metric("Time", f"{result_time - submit_time:.1f}s")
                        with st.expander("Full result"):
                            st.json(direct_result)
                    else:
                        progress_bar = st.progress(0, text="Waiting for Spark to process...")

                        start_wait = time.time()
                        result = None
                        while time.time() - start_wait < 15:
                            elapsed_wait = time.time() - start_wait
                            progress = min(elapsed_wait / 12, 0.95)
                            progress_bar.progress(progress, text=f"Polling database... ({elapsed_wait:.0f}s)")
                            result = db.search_transactions(tx_id=tx_data["TRANSACTION_ID"])
                            if result is not None and not result.empty:
                                progress_bar.progress(1.0, text="✅ Prediction received!")
                                break
                            time.sleep(1)

                        progress_bar.empty()

                        if result is not None and not result.empty:
                            row = result.iloc[0]
                            prob = float(row["fraud_probability"])
                            pred = int(row["is_fraud_predicted"])
                            result_time = time.time()
                            status.update(label="✅ Prediction received!", state="complete")
                            st.balloons()

                            col1, col2 = st.columns(2)
                            with col1:
                                st.plotly_chart(_build_gauge(prob), use_container_width=True)
                            with col2:
                                st.metric("Prediction", "⚠️ FRAUD" if pred == 1 else "✅ Legitimate")
                                st.metric("Fraud Probability", f"{prob*100:.2f}%",
                                          delta=f"{'Above' if prob >= FRAUD_THRESHOLD else 'Below'} threshold ({int(FRAUD_THRESHOLD * 100)}%)",
                                          delta_color="inverse")
                                st.metric("Time in pipeline", f"{result_time - submit_time:.1f}s")
                            st.plotly_chart(_build_pipeline_timeline(submit_time, result_time), use_container_width=True)
                            with st.expander("Full result record"):
                                st.json(row.to_dict())
                        else:
                            status.update(label="⏱️ Timeout — prediction not received within 15s", state="error")
                            st.warning("Transaction was sent to Kafka but no prediction appeared in BigQuery within 15s.")
                except Exception as e:
                    status.update(label=f"❌ Error: {e}", state="error")
                    st.error(str(e))
        else:
            st.info(f"⏳ Waiting for {tx_count} transactions to appear in the database...")
            progress_bar = st.progress(0, text="Polling database...")
            start_wait = time.time()
            tx_ids = {t["TRANSACTION_ID"]: t["TX_AMOUNT"] for t in txs}
            found_rows = []

            while time.time() - start_wait < 30 and len(found_rows) < tx_count:
                elapsed_wait = time.time() - start_wait
                progress = min(elapsed_wait / 25, 0.95)
                progress_bar.progress(progress, text=f"Polling database... ({elapsed_wait:.0f}s)")

                for tid, amt in list(tx_ids.items()):
                    result = db.search_transactions(tx_id=tid)
                    if result is not None and not result.empty:
                        row = result.iloc[0]
                        found_rows.append({"transaction_id": tid, "amount": amt, "fraud_probability": float(row["fraud_probability"]), "is_fraud_predicted": int(row["is_fraud_predicted"])})
                        del tx_ids[tid]

                time.sleep(1)

            progress_bar.empty()

            if found_rows:
                st.success(f"✅ {len(found_rows)}/{tx_count} predictions received")
                df = pd.DataFrame(found_rows)
                df["correct"] = df.apply(lambda r: (r["fraud_probability"] >= FRAUD_THRESHOLD) if tx_type == "fraud" else (r["fraud_probability"] < FRAUD_THRESHOLD), axis=1)

                results_col1, results_col2 = st.columns(2)
                results_col1.metric("Transactions Sent", tx_count)
                results_col2.metric("Predictions Received", len(found_rows))

                st.dataframe(df, use_container_width=True, column_config={
                    "transaction_id": "TX ID",
                    "amount": st.column_config.NumberColumn("Amount ($)", format="$%.2f"),
                    "fraud_probability": st.column_config.ProgressColumn("Fraud Prob", format="%.2f", min_value=0, max_value=1),
                    "is_fraud_predicted": "Predicted",
                })
            else:
                st.warning("No predictions appeared in the database within 30s.")
