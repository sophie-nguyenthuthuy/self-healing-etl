from __future__ import annotations

import json
from datetime import datetime
from sqlalchemy import (
    Boolean, Column, DateTime, Integer, String, Text, create_engine, inspect, text
)
from sqlalchemy.orm import DeclarativeBase, Session


class Base(DeclarativeBase):
    pass


class SchemaVersion(Base):
    __tablename__ = "schema_versions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_name = Column(String(256), nullable=False, index=True)
    version = Column(Integer, nullable=False)
    schema_json = Column(Text, nullable=False)   # JSON: {col: dtype_str}
    registered_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)

    def get_schema(self) -> dict[str, str]:
        return json.loads(self.schema_json)


class QuarantineRecord(Base):
    __tablename__ = "quarantine_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_name = Column(String(256), nullable=False, index=True)
    source_name = Column(String(256), nullable=False, index=True)
    run_id = Column(String(128), nullable=False, index=True)
    record_json = Column(Text, nullable=False)   # raw record
    error_type = Column(String(128), nullable=False, index=True)
    error_detail = Column(Text, nullable=False)
    root_cause_hint = Column(Text, nullable=True)
    quarantined_at = Column(DateTime, default=datetime.utcnow, index=True)
    schema_version = Column(Integer, nullable=True)
    resolved = Column(Boolean, default=False)
    resolved_at = Column(DateTime, nullable=True)


class DriftEvent(Base):
    __tablename__ = "drift_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_name = Column(String(256), nullable=False, index=True)
    source_name = Column(String(256), nullable=False, index=True)
    run_id = Column(String(128), nullable=False)
    drift_type = Column(String(64), nullable=False)  # added_columns | removed_columns | type_changed
    details_json = Column(Text, nullable=False)
    detected_at = Column(DateTime, default=datetime.utcnow, index=True)
    healed = Column(Boolean, default=False)
    healing_action = Column(Text, nullable=True)


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(128), nullable=False, unique=True, index=True)
    pipeline_name = Column(String(256), nullable=False, index=True)
    source_name = Column(String(256), nullable=False)
    status = Column(String(32), nullable=False, default="RUNNING")
    rows_extracted = Column(Integer, default=0)
    rows_loaded = Column(Integer, default=0)
    rows_quarantined = Column(Integer, default=0)
    drift_detected = Column(Boolean, default=False)
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)


class PipelineEvent(Base):
    __tablename__ = "pipeline_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    run_id = Column(String(128), nullable=False, index=True)
    pipeline_name = Column(String(256), nullable=False, index=True)
    event_type = Column(String(128), nullable=False, index=True)
    severity = Column(String(32), nullable=False, index=True)
    component = Column(String(128), nullable=False)
    message = Column(Text, nullable=False)
    metadata_json = Column(Text, nullable=True)
    trace_id = Column(String(128), nullable=False, index=True)
    span_id = Column(String(128), nullable=False, index=True)


class IncidentHistory(Base):
    __tablename__ = "incident_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    failure_signature = Column(String(512), nullable=False, index=True)
    root_cause = Column(Text, nullable=False)
    healing_action = Column(Text, nullable=True)
    successful = Column(Boolean, default=False, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)


class HealingAction(Base):
    __tablename__ = "healing_actions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(128), nullable=False, index=True)
    trace_id = Column(String(128), nullable=False, index=True)
    root_cause = Column(Text, nullable=False)
    action_taken = Column(Text, nullable=False)
    agent = Column(String(128), nullable=False)
    before_state = Column(Text, nullable=True)
    after_state = Column(Text, nullable=True)
    success = Column(Boolean, default=False, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)


class HumanApproval(Base):
    __tablename__ = "human_approvals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(128), nullable=False, index=True)
    trace_id = Column(String(128), nullable=False, index=True)
    plan_json = Column(Text, nullable=False)
    status = Column(String(32), nullable=False, default="PENDING", index=True)
    requested_at = Column(DateTime, default=datetime.utcnow, index=True)
    approved_at = Column(DateTime, nullable=True)
    approver = Column(String(256), nullable=True)


def init_db(db_url: str) -> "Engine":
    engine = create_engine(db_url, echo=False)
    Base.metadata.create_all(engine)
    return engine


def ensure_quarantine_schema(engine) -> None:
    """Apply additive quarantine schema updates for existing local SQLite databases."""
    if engine.dialect.name != "sqlite":
        return

    inspector = inspect(engine)
    if "quarantine_records" not in inspector.get_table_names():
        return

    columns = {column["name"] for column in inspector.get_columns("quarantine_records")}
    if "resolved_at" in columns:
        return

    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE quarantine_records ADD COLUMN resolved_at DATETIME"))
