from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from taxi_etl.config import TaxiETLPaths, TaxiRuntimeConfig
from taxi_etl.warehouse import read_live_metrics
from observability.metrics import ObservabilityMetrics


def _read_table(db_url: str, table_name: str) -> pd.DataFrame:
    try:
        engine = create_engine(db_url)
        with engine.begin() as conn:
            if not inspect(conn).has_table(table_name):
                return pd.DataFrame()
            return pd.read_sql_table(table_name, conn)
    except SQLAlchemyError:
        return pd.DataFrame()


def _safe_health(db_url: str, pipeline_name: str) -> dict[str, int]:
    try:
        return ObservabilityMetrics(db_url).health(pipeline_name)
    except SQLAlchemyError:
        return {
            "active_runs": 0,
            "failed_runs": 0,
            "successful_runs": 0,
            "recovered_runs": 0,
            "healing_actions": 0,
            "successful_healing_actions": 0,
        }


def _safe_live_metrics(db_url: str, runtime: TaxiRuntimeConfig) -> dict[str, float]:
    try:
        return read_live_metrics(db_url, runtime)
    except SQLAlchemyError:
        return {
            "total_trips": 0,
            "total_revenue": 0.0,
            "average_fare": 0.0,
            "average_trip_distance": 0.0,
            "average_trip_duration": 0.0,
        }


paths = TaxiETLPaths()
runtime = TaxiRuntimeConfig()
tables = runtime.schema.table_names
warehouse_url = st.sidebar.text_input("Warehouse URL", paths.warehouse_url)
observability_db_url = st.sidebar.text_input("Observability DB URL", paths.quarantine_url)
refresh_seconds = st.sidebar.slider("Refresh seconds", 1, 30, 5)

st.set_page_config(page_title="Agentic Taxi ETL Platform", layout="wide")
st.title("Agentic Taxi ETL Platform")

metrics = _safe_live_metrics(warehouse_url, runtime)
cols = st.columns(5)
cols[0].metric("Total Trips", f"{metrics['total_trips']:,}")
cols[1].metric("Total Revenue", f"${metrics['total_revenue']:,.2f}")
cols[2].metric("Average Fare", f"${metrics['average_fare']:,.2f}")
cols[3].metric("Average Distance", f"{metrics['average_trip_distance']:.2f} mi")
cols[4].metric("Average Duration", f"{metrics['average_trip_duration']:.1f} min")

tab_health, tab_events, tab_rca, tab_timeline, tab_decisions, tab_analytics = st.tabs(
    [
        "Pipeline Health",
        "Live Events",
        "Root Cause Analysis",
        "Healing Timeline",
        "Agent Decisions",
        "Taxi Analytics",
    ]
)

with tab_health:
    health = _safe_health(observability_db_url, runtime.schema.pipeline_name)
    health_cols = st.columns(5)
    health_cols[0].metric("Active Runs", health["active_runs"])
    health_cols[1].metric("Failed Runs", health["failed_runs"])
    health_cols[2].metric("Successful Runs", health["successful_runs"])
    health_cols[3].metric("Recovered Runs", health["recovered_runs"])
    health_cols[4].metric("Healing Actions", health["healing_actions"])

with tab_events:
    events = _read_table(observability_db_url, "pipeline_events")
    if events.empty:
        st.info("No pipeline events recorded yet.")
    else:
        st.dataframe(events.sort_values("timestamp", ascending=False), use_container_width=True)

with tab_rca:
    events = _read_table(observability_db_url, "pipeline_events")
    failures = events[events["severity"].isin(["ERROR", "WARNING"])] if not events.empty else pd.DataFrame()
    if failures.empty:
        st.info("No RCA-triggering events recorded yet.")
    else:
        st.dataframe(
            failures[["timestamp", "event_type", "component", "message", "metadata_json", "trace_id"]]
            .sort_values("timestamp", ascending=False),
            use_container_width=True,
        )

with tab_timeline:
    events = _read_table(observability_db_url, "pipeline_events")
    if events.empty:
        st.info("No healing timeline recorded yet.")
    else:
        timeline = events[
            events["event_type"].isin(
                [
                    "SchemaDriftDetected",
                    "QuarantineTriggered",
                    "HealingStarted",
                    "HealingCompleted",
                    "HealingValidationFailed",
                    "PipelineRecovered",
                    "PipelineEscalated",
                ]
            )
        ]
        st.dataframe(timeline.sort_values("timestamp"), use_container_width=True)

with tab_decisions:
    actions = _read_table(observability_db_url, "healing_actions")
    approvals = _read_table(observability_db_url, "human_approvals")
    st.subheader("Healing Actions")
    if actions.empty:
        st.info("No agent healing actions recorded yet.")
    else:
        st.dataframe(actions.sort_values("timestamp", ascending=False), use_container_width=True)
    st.subheader("Human Approval Queue")
    if approvals.empty:
        st.info("No pending approval records.")
    else:
        st.dataframe(approvals.sort_values("requested_at", ascending=False), use_container_width=True)

with tab_analytics:
    fact = _read_table(warehouse_url, tables.fact)
    summary = _read_table(warehouse_url, tables.summary)
    left, right = st.columns(2)
    if not summary.empty:
        hourly = summary.groupby("pickup_hour", as_index=False)["total_trips"].sum()
        revenue = summary.groupby("pickup_hour", as_index=False)["total_revenue"].sum()
        left.subheader("Trips by Hour")
        left.bar_chart(hourly, x="pickup_hour", y="total_trips")
        right.subheader("Revenue Trend")
        right.line_chart(revenue, x="pickup_hour", y="total_revenue")

    if not fact.empty:
        left, right = st.columns(2)
        left.subheader("Fare Distribution")
        left.bar_chart(pd.to_numeric(fact["fare_amount"], errors="coerce").dropna())
        right.subheader("Trip Distance Distribution")
        right.bar_chart(pd.to_numeric(fact["trip_distance"], errors="coerce").dropna())

        st.subheader("Payment Type Analysis")
        payment = fact.groupby("payment_type", as_index=False)["trip_id"].count()
        st.bar_chart(payment, x="payment_type", y="trip_id")

st.caption(f"Auto-refresh every {refresh_seconds} seconds while the Streamlit session is active.")
st.markdown(
    f"<meta http-equiv='refresh' content='{refresh_seconds}'>",
    unsafe_allow_html=True,
)
