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

st.set_page_config(page_title="Agentic Taxi ETL Platform", layout="wide")


def _read_table(db_url: str, table_name: str) -> pd.DataFrame:
    try:
        engine = create_engine(db_url)
        with engine.begin() as conn:
            if not inspect(conn).has_table(table_name):
                return pd.DataFrame()
            return pd.read_sql_table(table_name, conn)
    except SQLAlchemyError:
        return pd.DataFrame()


def _safe_health(db_url: str, pipeline_name: str | None = None) -> dict[str, int]:
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


def _k8s_events(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()
    event_types = events["event_type"].astype(str)
    components = events["component"].astype(str)
    return events[event_types.str.startswith("K8s") | components.str.startswith("k8s_")]


def _k8s_related_events(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty or "trace_id" not in events.columns:
        return pd.DataFrame()
    k8s = _k8s_events(events)
    if k8s.empty:
        return pd.DataFrame()
    trace_ids = set(k8s["trace_id"].dropna().astype(str))
    return events[events["trace_id"].astype(str).isin(trace_ids)]


def _k8s_incidents(incidents: pd.DataFrame) -> pd.DataFrame:
    if incidents.empty or "source_domain" not in incidents.columns:
        return pd.DataFrame()
    return incidents[incidents["source_domain"].astype(str).str.upper() == "K8S"]


def _pipeline_names(events: pd.DataFrame, default_name: str) -> list[str]:
    names = {default_name}
    if not events.empty and "pipeline_name" in events.columns:
        names.update(str(name) for name in events["pipeline_name"].dropna().unique())
    return sorted(name for name in names if name)


def _filter_pipeline(df: pd.DataFrame, pipeline_name: str | None) -> pd.DataFrame:
    if not pipeline_name or df.empty or "pipeline_name" not in df.columns:
        return df
    return df[df["pipeline_name"].astype(str) == pipeline_name]


def _filter_by_trace_ids(df: pd.DataFrame, trace_ids: set[str], *, empty_when_no_traces: bool = False) -> pd.DataFrame:
    if df.empty or "trace_id" not in df.columns:
        return pd.DataFrame() if empty_when_no_traces else df
    if not trace_ids:
        return pd.DataFrame() if empty_when_no_traces else df
    return df[df["trace_id"].astype(str).isin(trace_ids)]


paths = TaxiETLPaths()
runtime = TaxiRuntimeConfig()
tables = runtime.schema.table_names
warehouse_url = st.sidebar.text_input("Warehouse URL", paths.warehouse_url)
observability_db_url = st.sidebar.text_input("Observability DB URL", paths.quarantine_url)
events_all = _read_table(observability_db_url, "pipeline_events")
actions_all = _read_table(observability_db_url, "healing_actions")
approvals_all = _read_table(observability_db_url, "human_approvals")
incidents_all = _read_table(observability_db_url, "incident_history")
scope_options = ["All pipelines", *_pipeline_names(events_all, runtime.schema.pipeline_name)]
scope_label = st.sidebar.selectbox("Observability scope", scope_options)
selected_pipeline = None if scope_label == "All pipelines" else scope_label
refresh_seconds = st.sidebar.slider("Auto-refresh seconds", 30, 300, 120, step=30)
if st.sidebar.button("Refresh now", use_container_width=True):
    st.rerun()
events_scoped = _filter_pipeline(events_all, selected_pipeline)
scoped_trace_ids = set(events_scoped["trace_id"].dropna().astype(str)) if not events_scoped.empty and "trace_id" in events_scoped.columns else set()
actions_scoped = _filter_by_trace_ids(actions_all, scoped_trace_ids, empty_when_no_traces=bool(selected_pipeline))
approvals_scoped = _filter_by_trace_ids(approvals_all, scoped_trace_ids, empty_when_no_traces=bool(selected_pipeline))
incidents_scoped = incidents_all

st.title("Agentic Taxi ETL Platform")

metrics = _safe_live_metrics(warehouse_url, runtime)
cols = st.columns(5)
cols[0].metric("Total Trips", f"{metrics['total_trips']:,}")
cols[1].metric("Total Revenue", f"${metrics['total_revenue']:,.2f}")
cols[2].metric("Average Fare", f"${metrics['average_fare']:,.2f}")
cols[3].metric("Average Distance", f"{metrics['average_trip_distance']:.2f} mi")
cols[4].metric("Average Duration", f"{metrics['average_trip_duration']:.1f} min")

(
    tab_health,
    tab_events,
    tab_rca,
    tab_timeline,
    tab_decisions,
    tab_analytics,
    tab_k8s_health,
    tab_cluster_events,
    tab_ai_sre_incidents,
    tab_k8s_actions,
    tab_recovery_metrics,
) = st.tabs(
    [
        "Pipeline Health",
        "Live Events",
        "Root Cause Analysis",
        "Healing Timeline",
        "Agent Decisions",
        "Taxi Analytics",
        "Kubernetes Health",
        "Cluster Events",
        "AI-SRE Incidents",
        "Healing Actions",
        "Recovery Metrics",
    ]
)

with tab_health:
    health = _safe_health(observability_db_url, selected_pipeline)
    if selected_pipeline:
        health["healing_actions"] = len(actions_scoped)
        health["successful_healing_actions"] = int(actions_scoped["success"].sum()) if not actions_scoped.empty and "success" in actions_scoped.columns else 0
    health_cols = st.columns(5)
    health_cols[0].metric("Active Runs", health["active_runs"])
    health_cols[1].metric("Failed Runs", health["failed_runs"])
    health_cols[2].metric("Successful Runs", health["successful_runs"])
    health_cols[3].metric("Recovered Runs", health["recovered_runs"])
    health_cols[4].metric("Healing Actions", health["healing_actions"])

with tab_events:
    events = events_scoped
    if events.empty:
        st.info("No pipeline events recorded yet.")
    else:
        st.dataframe(events.sort_values("timestamp", ascending=False), use_container_width=True)

with tab_rca:
    events = events_scoped
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
    events = events_scoped
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
    actions = actions_scoped
    approvals = approvals_scoped
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

with tab_k8s_health:
    events = _k8s_events(events_scoped)
    health_cols = st.columns(4)
    if events.empty:
        health_cols[0].metric("Pods Observed", 0)
        health_cols[1].metric("Degraded Pods", 0)
        health_cols[2].metric("CrashLoops", 0)
        health_cols[3].metric("OOMKilled", 0)
        st.info("No Kubernetes health events recorded yet.")
    else:
        pod_names = events["metadata_json"].astype(str).str.extract(r'"name":\s*"([^"]+)"')[0].dropna().nunique()
        degraded = events[events["event_type"].isin(["K8sDeploymentDegraded", "K8sHighRestartCount"])]
        crashloops = events[events["event_type"] == "K8sPodCrashLoopDetected"]
        oom = events[events["event_type"] == "K8sOOMKilled"]
        health_cols[0].metric("Pods Observed", int(pod_names))
        health_cols[1].metric("Degraded Pods", len(degraded))
        health_cols[2].metric("CrashLoops", len(crashloops))
        health_cols[3].metric("OOMKilled", len(oom))
        st.dataframe(events.sort_values("timestamp", ascending=False), use_container_width=True)

with tab_cluster_events:
    events = _k8s_events(events_scoped)
    if events.empty:
        st.info("No Kubernetes cluster events recorded yet.")
    else:
        columns = [col for col in ["timestamp", "event_type", "severity", "component", "message", "metadata_json", "trace_id"] if col in events.columns]
        st.dataframe(events[columns].sort_values("timestamp", ascending=False), use_container_width=True)

with tab_ai_sre_incidents:
    incidents = _k8s_incidents(incidents_scoped)
    if incidents.empty:
        st.info("No AI-SRE Kubernetes incidents recorded yet.")
    else:
        st.dataframe(incidents.sort_values("timestamp", ascending=False), use_container_width=True)

with tab_k8s_actions:
    actions = actions_scoped
    if actions.empty:
        st.info("No Kubernetes healing actions recorded yet.")
    else:
        k8s_actions = actions[actions["agent"].astype(str) == "K8sHealingEngine"]
        if k8s_actions.empty:
            st.info("No Kubernetes healing actions recorded yet.")
        else:
            st.dataframe(k8s_actions.sort_values("timestamp", ascending=False), use_container_width=True)

with tab_recovery_metrics:
    events = _k8s_related_events(events_scoped)
    if events.empty:
        st.info("No Kubernetes recovery metrics recorded yet.")
    else:
        k8s_failures = _k8s_events(events)
        failures = k8s_failures[k8s_failures["severity"].isin(["ERROR", "WARNING"])]
        recovered = events[events["event_type"] == "PipelineRecovered"]
        recovery_cols = st.columns(3)
        recovery_cols[0].metric("K8s Failure Events", len(failures))
        recovery_cols[1].metric("Recovered Events", len(recovered))
        recovery_cols[2].metric("Recovery Success Rate", f"{(len(recovered) / max(len(failures), 1)) * 100:.1f}%")
        if not failures.empty and not recovered.empty:
            failures = failures.copy()
            recovered = recovered.copy()
            failures["timestamp"] = pd.to_datetime(failures["timestamp"], errors="coerce")
            recovered["timestamp"] = pd.to_datetime(recovered["timestamp"], errors="coerce")
            first_failure = failures["timestamp"].min()
            first_recovery = recovered["timestamp"].max()
            if pd.notna(first_failure) and pd.notna(first_recovery):
                st.metric("Approx. MTTR", str(first_recovery - first_failure))

st.caption(f"Auto-refresh every {refresh_seconds} seconds while the Streamlit session is active.")
st.markdown(
    f"<meta http-equiv='refresh' content='{refresh_seconds}'>",
    unsafe_allow_html=True,
)
