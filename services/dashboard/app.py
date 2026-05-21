from __future__ import annotations

import os
from typing import Any

import pandas as pd
import psycopg
import streamlit as st
from streamlit_autorefresh import st_autorefresh

try:
    import altair as alt
except ImportError:
    alt = None


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "pizza_serving")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")
REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "5"))
LOW_STOCK_THRESHOLD = float(os.getenv("DASHBOARD_LOW_STOCK_THRESHOLD", "15"))
TOP_PIZZA_COUNT = int(os.getenv("DASHBOARD_TOP_PIZZA_COUNT", "15"))


st.set_page_config(page_title="Pizza Pulse", layout="wide")
st_autorefresh(interval=REFRESH_SECONDS * 1000, key="pizza-pulse-refresh")

st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1.4rem;
        padding-bottom: 2rem;
    }
    .hero {
        border: 1px solid #e5e7eb;
        border-radius: 8px;
        padding: 22px 26px;
        background: #ffffff;
        margin-bottom: 16px;
    }
    .eyebrow {
        color: #64748b;
        font-size: 0.78rem;
        font-weight: 700;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        margin-bottom: 4px;
    }
    .hero-title {
        color: #111827;
        font-size: 2.4rem;
        font-weight: 760;
        line-height: 1.05;
    }
    .hero-subtitle {
        color: #475569;
        margin-top: 8px;
        font-size: 0.98rem;
    }
    div[data-testid="stMetric"] {
        border: 1px solid #e5e7eb;
        border-radius: 8px;
        padding: 12px 14px;
        background: #ffffff;
    }
    h2, h3 {
        letter-spacing: 0;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def connect():
    return psycopg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def query(sql: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            columns = [column.name for column in cur.description]
    return pd.DataFrame(rows, columns=columns)


@st.cache_data(ttl=REFRESH_SECONDS)
def load_frames() -> dict[str, pd.DataFrame]:
    return {
        "predictions": safe_query(
            """
            WITH latest AS (
                SELECT MAX(target_hour) AS target_hour
                FROM demand_predictions
            )
            SELECT
                dp.target_hour,
                dp.pizza_id,
                dp.pizza_name,
                dp.pizza_size,
                dp.pizza_category,
                dp.predicted_quantity,
                dp.model_version,
                dp.predicted_at
            FROM demand_predictions dp
            JOIN latest l
                ON dp.target_hour = l.target_hour
            ORDER BY dp.predicted_quantity DESC, dp.pizza_category, dp.pizza_name, dp.pizza_size
            LIMIT 500
            """
        ),
        "risks": safe_query(
            """
            WITH latest AS (
                SELECT MAX(target_hour) AS target_hour
                FROM ingredient_risk_predictions
            )
            SELECT
                ir.target_hour,
                ir.ingredient_id,
                i.ingredient_name,
                ir.predicted_usage::double precision AS predicted_usage,
                i.current_stock::double precision AS current_stock,
                (i.current_stock - ir.predicted_usage)::double precision AS projected_stock,
                CASE
                    WHEN i.current_stock - ir.predicted_usage < 0 THEN 'critical'
                    WHEN ir.predicted_usage >= i.current_stock * 0.8 THEN 'warning'
                    ELSE 'ok'
                END AS severity,
                ir.model_version,
                ir.predicted_at
            FROM ingredient_risk_predictions ir
            JOIN latest l
                ON ir.target_hour = l.target_hour
            JOIN ingredients i
                ON ir.ingredient_id = i.ingredient_id
            WHERE i.current_stock - ir.predicted_usage <= %s
            ORDER BY projected_stock ASC, ir.predicted_usage DESC, i.ingredient_name
            LIMIT 50
            """,
            (LOW_STOCK_THRESHOLD,),
        ),
        "actuals": safe_query(
            """
            WITH selected_day AS (
                SELECT COALESCE(
                    (SELECT MAX(target_hour)::date FROM demand_predictions),
                    (SELECT MAX(order_hour)::date FROM online_hourly_demand),
                    CURRENT_DATE
                ) AS day
            )
            SELECT
                date_trunc('hour', ohd.order_hour)::timestamp AS order_hour,
                SUM(ohd.quantity)::double precision AS quantity,
                SUM(ohd.revenue)::double precision AS revenue,
                SUM(ohd.order_count)::bigint AS order_count,
                COUNT(DISTINCT ohd.pizza_id)::int AS pizza_count
            FROM online_hourly_demand ohd
            CROSS JOIN selected_day sd
            WHERE ohd.order_hour::date = sd.day
            GROUP BY date_trunc('hour', ohd.order_hour)
            ORDER BY order_hour
            """
        ),
        "actual_categories": safe_query(
            """
            WITH selected_day AS (
                SELECT COALESCE(
                    (SELECT MAX(target_hour)::date FROM demand_predictions),
                    (SELECT MAX(order_hour)::date FROM online_hourly_demand),
                    CURRENT_DATE
                ) AS day
            )
            SELECT
                COALESCE(ohd.pizza_category, 'Unknown') AS pizza_category,
                SUM(ohd.quantity)::double precision AS quantity,
                SUM(ohd.revenue)::double precision AS revenue,
                SUM(ohd.order_count)::bigint AS order_count
            FROM online_hourly_demand ohd
            CROSS JOIN selected_day sd
            WHERE ohd.order_hour::date = sd.day
            GROUP BY COALESCE(ohd.pizza_category, 'Unknown')
            ORDER BY quantity DESC
            """
        ),
        "status": safe_query(
            """
            SELECT
                (SELECT MAX(target_hour) FROM demand_predictions) AS latest_target_hour,
                (
                    SELECT predicted_at
                    FROM demand_predictions
                    ORDER BY target_hour DESC, predicted_at DESC
                    LIMIT 1
                ) AS latest_prediction_at
            """
        ),
    }


def safe_query(sql: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    try:
        return query(sql, params)
    except Exception:
        return pd.DataFrame()


def normalize_frames(frames: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    for name, time_columns in {
        "predictions": ["target_hour", "predicted_at"],
        "risks": ["target_hour", "predicted_at"],
        "actuals": ["order_hour"],
        "status": ["latest_target_hour", "latest_prediction_at"],
    }.items():
        frame = frames.get(name, pd.DataFrame())
        for column in time_columns:
            if column in frame:
                frame[column] = pd.to_datetime(frame[column], errors="coerce")

    numeric_columns = {
        "predictions": ["predicted_quantity"],
        "risks": ["predicted_usage", "current_stock", "projected_stock"],
        "actuals": ["quantity", "revenue", "order_count", "pizza_count"],
        "actual_categories": ["quantity", "revenue", "order_count"],
    }
    for name, columns in numeric_columns.items():
        frame = frames.get(name, pd.DataFrame())
        for column in columns:
            if column in frame:
                frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0)
    return frames


def scalar(frame: pd.DataFrame, column: str, default=None):
    if frame.empty or column not in frame:
        return default
    value = frame.iloc[0].get(column)
    if pd.isna(value):
        return default
    return value


def time_label(value) -> str:
    if value is None or pd.isna(value):
        return "-"
    return pd.Timestamp(value).strftime("%Y-%m-%d %H:%M")


def hour_label(value) -> str:
    if value is None or pd.isna(value):
        return "-"
    return pd.Timestamp(value).strftime("%H:%M")


def pizza_label(row: pd.Series) -> str:
    name = row.get("pizza_name") or row.get("pizza_id") or "unknown"
    size = row.get("pizza_size") or "-"
    return f"{name} ({size})"


def chart_pizza_label(row: pd.Series) -> str:
    name = str(row.get("pizza_name") or row.get("pizza_id") or "unknown")
    name = name.removeprefix("The ").removesuffix(" Pizza")
    size = row.get("pizza_size") or "-"
    return f"{name} ({size})"


def render_prediction_section(predictions: pd.DataFrame) -> None:
    st.subheader("Dự báo pizza cần chuẩn bị")
    if predictions.empty:
        st.info("No demand predictions yet.")
        return

    top_predictions = predictions.head(TOP_PIZZA_COUNT).copy()
    top_predictions["label"] = top_predictions.apply(chart_pizza_label, axis=1)

    if alt is None:
        st.bar_chart(top_predictions.set_index("label")["predicted_quantity"])
    else:
        chart = (
            alt.Chart(top_predictions)
            .mark_bar(cornerRadiusEnd=4)
            .encode(
                x=alt.X("predicted_quantity:Q", title="Số lượng"),
                y=alt.Y(
                    "label:N",
                    sort="-x",
                    title=None,
                    axis=alt.Axis(labelLimit=460, labelFontSize=12, labelPadding=8),
                ),
                color=alt.Color("pizza_category:N", title="Category", scale=alt.Scale(scheme="tableau10")),
                tooltip=[
                    alt.Tooltip("pizza_id:N", title="Pizza ID"),
                    alt.Tooltip("pizza_name:N", title="Pizza"),
                    alt.Tooltip("pizza_size:N", title="Size"),
                    alt.Tooltip("pizza_category:N", title="Category"),
                    alt.Tooltip("predicted_quantity:Q", title="Dự đoán", format=",.0f"),
                ],
            )
            .properties(height=max(280, min(520, 28 * len(top_predictions) + 40)))
        )
        st.altair_chart(chart, use_container_width=True)

    table = predictions[
        ["pizza_id", "pizza_name", "pizza_size", "pizza_category", "predicted_quantity"]
    ].rename(
        columns={
            "pizza_id": "Pizza ID",
            "pizza_name": "Pizza",
            "pizza_size": "Size",
            "pizza_category": "Category",
            "predicted_quantity": "Số lượng",
        }
    )
    st.dataframe(
        table,
        use_container_width=True,
        hide_index=True,
        height=360,
    )


def render_ingredient_section(risks: pd.DataFrame) -> None:
    st.subheader("Nguyên liệu sắp thiếu")
    if risks.empty:
        st.success("Chưa có nguyên liệu nào ở mức rủi ro thấp trong khung dự báo mới nhất.")
        return

    left, right = st.columns([0.95, 1.25])
    with left:
        st.metric("Nguyên liệu cần chú ý", len(risks))
        st.metric("Tồn kho thấp nhất sau dự báo", f"{risks['projected_stock'].min():,.1f}")
        st.dataframe(
            risks[
                ["ingredient_name", "severity", "projected_stock", "current_stock", "predicted_usage"]
            ].rename(
                columns={
                    "ingredient_name": "Nguyên liệu",
                    "severity": "Mức độ",
                    "projected_stock": "Sau dự báo",
                    "current_stock": "Hiện còn",
                    "predicted_usage": "Dự kiến dùng",
                }
            ),
            use_container_width=True,
            hide_index=True,
            height=330,
        )

    with right:
        chart_source = risks.head(20)[["ingredient_name", "current_stock", "projected_stock"]].melt(
            id_vars=["ingredient_name"],
            var_name="stock_type",
            value_name="stock",
        )
        if alt is None:
            st.bar_chart(chart_source.pivot(index="ingredient_name", columns="stock_type", values="stock"))
        else:
            chart = (
                alt.Chart(chart_source)
                .mark_bar(cornerRadiusEnd=4)
                .encode(
                    x=alt.X("stock:Q", title="Stock"),
                    y=alt.Y(
                        "ingredient_name:N",
                        sort=alt.SortField("stock", order="ascending"),
                        title=None,
                        axis=alt.Axis(labelLimit=360, labelFontSize=12, labelPadding=8),
                    ),
                    color=alt.Color(
                        "stock_type:N",
                        title=None,
                        scale=alt.Scale(
                            domain=["current_stock", "projected_stock"],
                            range=["#94a3b8", "#ef4444"],
                        ),
                    ),
                    tooltip=[
                        alt.Tooltip("ingredient_name:N", title="Ingredient"),
                        alt.Tooltip("stock_type:N", title="Metric"),
                        alt.Tooltip("stock:Q", title="Stock", format=",.1f"),
                    ],
                )
                .properties(height=max(280, min(520, 30 * risks.head(20).shape[0] + 40)))
            )
            st.altair_chart(chart, use_container_width=True)


def render_actual_section(actuals: pd.DataFrame, actual_categories: pd.DataFrame) -> None:
    st.subheader("Doanh số thực tế trong ngày")
    if actuals.empty:
        st.info("No online demand aggregates for the selected day yet.")
        return

    total_quantity = actuals["quantity"].sum()
    total_orders = actuals["order_count"].sum()
    total_revenue = actuals["revenue"].sum()
    active_pizzas = actuals["pizza_count"].max()
    demand_day = pd.Timestamp(actuals["order_hour"].min()).strftime("%Y-%m-%d")

    a, b, c, d, e = st.columns(5)
    a.metric("Ngày", demand_day)
    b.metric("Đã bán", f"{total_quantity:,.0f}")
    c.metric("Đơn hàng", f"{total_orders:,.0f}")
    d.metric("Món có bán", f"{active_pizzas:,.0f}")
    e.metric("Doanh thu", f"${total_revenue:,.0f}")

    left, right = st.columns([1.4, 0.9])
    with left:
        if alt is None:
            st.line_chart(actuals.set_index("order_hour")["quantity"])
        else:
            hourly_chart = (
                alt.Chart(actuals)
                .mark_area(line=True, point=True, opacity=0.25, color="#2563eb")
                .encode(
                    x=alt.X("order_hour:T", title="Hour"),
                    y=alt.Y("quantity:Q", title="Pizza đã bán"),
                    tooltip=[
                        alt.Tooltip("order_hour:T", title="Hour", format="%Y-%m-%d %H:%M"),
                        alt.Tooltip("quantity:Q", title="Units", format=",.0f"),
                        alt.Tooltip("order_count:Q", title="Orders", format=",.0f"),
                        alt.Tooltip("pizza_count:Q", title="Active pizzas", format=",.0f"),
                    ],
                )
                .properties(height=320)
            )
            st.altair_chart(hourly_chart, use_container_width=True)

    with right:
        if actual_categories.empty:
            st.info("No category aggregates yet.")
        elif alt is None:
            st.bar_chart(actual_categories.set_index("pizza_category")["quantity"])
        else:
            category_chart = (
                alt.Chart(actual_categories)
                .mark_bar(cornerRadiusEnd=4)
                .encode(
                    x=alt.X("quantity:Q", title="Đã bán"),
                    y=alt.Y("pizza_category:N", sort="-x", title=None),
                    color=alt.Color("pizza_category:N", legend=None, scale=alt.Scale(scheme="set2")),
                    tooltip=[
                        alt.Tooltip("pizza_category:N", title="Category"),
                        alt.Tooltip("quantity:Q", title="Units", format=",.0f"),
                        alt.Tooltip("revenue:Q", title="Revenue", format="$,.0f"),
                    ],
                )
                .properties(height=320)
            )
            st.altair_chart(category_chart, use_container_width=True)

    st.dataframe(
        actuals.rename(
            columns={
                "order_hour": "Hour",
                "quantity": "Đã bán",
                "revenue": "Doanh thu",
                "order_count": "Đơn hàng",
                "pizza_count": "Món có bán",
            }
        ),
        use_container_width=True,
        hide_index=True,
        height=260,
    )


frames = normalize_frames(load_frames())
predictions = frames["predictions"]
risks = frames["risks"]
actuals = frames["actuals"]
actual_categories = frames["actual_categories"]
status = frames["status"]

latest_target = scalar(status, "latest_target_hour")
latest_prediction = scalar(status, "latest_prediction_at")

target_title = f"Dự đoán {hour_label(latest_target)}" if latest_target is not None else "Chưa có dự đoán"
st.markdown(
    f"""
    <div class="hero">
        <div class="eyebrow">Pizza Pulse</div>
        <div class="hero-title">{target_title}</div>
        <div class="hero-subtitle">
            Khung giờ: {time_label(latest_target)} · Cập nhật: {time_label(latest_prediction)}
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

total_predicted = predictions["predicted_quantity"].sum() if not predictions.empty else 0
top_pizza = pizza_label(predictions.iloc[0]) if not predictions.empty else "-"
left, middle, right = st.columns(3)
left.metric("Cần chuẩn bị", f"{total_predicted:,.0f} pizza")
middle.metric("Dự kiến bán nhiều nhất", top_pizza)
right.metric("Nguyên liệu cần chú ý", f"{len(risks):,.0f}")

render_prediction_section(predictions)
render_ingredient_section(risks)
render_actual_section(actuals, actual_categories)
