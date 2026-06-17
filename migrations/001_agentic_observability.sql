-- Agentic self-healing metadata tables.
-- SQLAlchemy creates these tables automatically through models.init_db().
-- This script is provided for environments that prefer explicit SQL migration review.

CREATE TABLE IF NOT EXISTS pipeline_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp DATETIME,
    run_id VARCHAR(128) NOT NULL,
    pipeline_name VARCHAR(256) NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    severity VARCHAR(32) NOT NULL,
    component VARCHAR(128) NOT NULL,
    message TEXT NOT NULL,
    metadata_json TEXT,
    trace_id VARCHAR(128) NOT NULL,
    span_id VARCHAR(128) NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_pipeline_events_timestamp ON pipeline_events(timestamp);
CREATE INDEX IF NOT EXISTS ix_pipeline_events_run_id ON pipeline_events(run_id);
CREATE INDEX IF NOT EXISTS ix_pipeline_events_pipeline_name ON pipeline_events(pipeline_name);
CREATE INDEX IF NOT EXISTS ix_pipeline_events_event_type ON pipeline_events(event_type);
CREATE INDEX IF NOT EXISTS ix_pipeline_events_severity ON pipeline_events(severity);
CREATE INDEX IF NOT EXISTS ix_pipeline_events_trace_id ON pipeline_events(trace_id);
CREATE INDEX IF NOT EXISTS ix_pipeline_events_span_id ON pipeline_events(span_id);

CREATE TABLE IF NOT EXISTS incident_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    failure_signature VARCHAR(512) NOT NULL,
    root_cause TEXT NOT NULL,
    healing_action TEXT,
    successful BOOLEAN,
    timestamp DATETIME
);

CREATE INDEX IF NOT EXISTS ix_incident_history_failure_signature ON incident_history(failure_signature);
CREATE INDEX IF NOT EXISTS ix_incident_history_successful ON incident_history(successful);
CREATE INDEX IF NOT EXISTS ix_incident_history_timestamp ON incident_history(timestamp);

CREATE TABLE IF NOT EXISTS healing_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id VARCHAR(128) NOT NULL,
    trace_id VARCHAR(128) NOT NULL,
    root_cause TEXT NOT NULL,
    action_taken TEXT NOT NULL,
    agent VARCHAR(128) NOT NULL,
    before_state TEXT,
    after_state TEXT,
    success BOOLEAN,
    timestamp DATETIME
);

CREATE INDEX IF NOT EXISTS ix_healing_actions_run_id ON healing_actions(run_id);
CREATE INDEX IF NOT EXISTS ix_healing_actions_trace_id ON healing_actions(trace_id);
CREATE INDEX IF NOT EXISTS ix_healing_actions_success ON healing_actions(success);
CREATE INDEX IF NOT EXISTS ix_healing_actions_timestamp ON healing_actions(timestamp);

CREATE TABLE IF NOT EXISTS human_approvals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id VARCHAR(128) NOT NULL,
    trace_id VARCHAR(128) NOT NULL,
    plan_json TEXT NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    requested_at DATETIME,
    approved_at DATETIME,
    approver VARCHAR(256)
);

CREATE INDEX IF NOT EXISTS ix_human_approvals_run_id ON human_approvals(run_id);
CREATE INDEX IF NOT EXISTS ix_human_approvals_trace_id ON human_approvals(trace_id);
CREATE INDEX IF NOT EXISTS ix_human_approvals_status ON human_approvals(status);
CREATE INDEX IF NOT EXISTS ix_human_approvals_requested_at ON human_approvals(requested_at);

