from __future__ import annotations

import os

import pandas as pd
import psycopg
import streamlit as st
from streamlit_autorefresh import st_autorefresh


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "pizza_serving")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")
REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "5"))


st.set_page_config(page_title="Pizza Pulse", layout="wide")
st_autorefresh(interval=REFRESH_SECONDS * 1000, key="pizza-pulse-refresh")


def connect():
    return psycopg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def query(sql: str) -> pd.DataFrame:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [column.name for column in cur.description]
    return pd.DataFrame(rows, columns=columns)


@st.cache_data(ttl=REFRESH_SECONDS)
def load_frames() -> dict[str, pd.DataFrame]:
    return {
        "predictions": safe_query(
            """
            SELECT
                target_hour,
                pizza_id,
                pizza_name,
                pizza_size,
                pizza_category,
                predicted_quantity,
                model_version,
                predicted_at
            FROM demand_predictions
            WHERE predicted_at >= now() - interval '2 days'
            ORDER BY predicted_at DESC, target_hour DESC, predicted_quantity DESC
            LIMIT 300
            """
        ),
        "actuals": safe_query(
            """
            SELECT
                order_hour,
                pizza_id,
                pizza_name,
                pizza_size,
                pizza_category,
                quantity,
                revenue,
                order_count,
                updated_at
            FROM online_hourly_demand
            ORDER BY order_hour DESC, quantity DESC
            LIMIT 300
            """
        ),
        "risks": safe_query(
            """
            SELECT
                target_hour,
                ingredient_name,
                predicted_usage,
                current_stock,
                projected_stock,
                severity,
                model_version,
                predicted_at
            FROM ingredient_risk_predictions
            ORDER BY
                target_hour DESC,
                CASE severity WHEN 'critical' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END,
                projected_stock ASC
            LIMIT 200
            """
        ),
        "status": safe_query(
            """
            SELECT
                MAX(predicted_at) AS latest_prediction_at,
                MAX(model_version) AS latest_model_version,
                COUNT(*) AS prediction_rows
            FROM demand_predictions
            """
        ),
    }


def safe_query(sql: str) -> pd.DataFrame:
    try:
        return query(sql)
    except Exception:
        return pd.DataFrame()


def metric_text(value) -> str:
    if value is None:
        return "-"
    try:
        if pd.isna(value):
            return "-"
    except (TypeError, ValueError):
        pass
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


frames = load_frames()
predictions = frames["predictions"]
actuals = frames["actuals"]
risks = frames["risks"]
status = frames["status"]

st.title("Pizza Pulse")

left, middle, right = st.columns(3)
if not status.empty:
    row = status.iloc[0]
    left.metric("Prediction rows", int(row.get("prediction_rows") or 0))
    middle.metric("Model version", metric_text(row.get("latest_model_version")))
    right.metric("Latest prediction", metric_text(row.get("latest_prediction_at")))
else:
    left.metric("Prediction rows", 0)
    middle.metric("Model version", "-")
    right.metric("Latest prediction", "-")

st.subheader("Demand Predictions")
if predictions.empty:
    st.info("No demand predictions yet.")
else:
    chart_df = predictions.head(40).copy()
    chart_df["label"] = chart_df["pizza_id"] + " @ " + chart_df["target_hour"].astype(str)
    st.bar_chart(chart_df.set_index("label")["predicted_quantity"])
    st.dataframe(predictions, use_container_width=True, hide_index=True)

st.subheader("Ingredient Risk")
if risks.empty:
    st.info("No ingredient risk predictions yet.")
else:
    st.dataframe(risks, use_container_width=True, hide_index=True)

st.subheader("Online Actual Demand")
if actuals.empty:
    st.info("No online demand aggregates yet.")
else:
    st.dataframe(actuals, use_container_width=True, hide_index=True)
