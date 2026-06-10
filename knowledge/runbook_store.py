from __future__ import annotations


class RunbookStore:
    """Deterministic runbook hints used when LLM RCA is unavailable or unnecessary."""

    _RULES: tuple[tuple[str, str, list[str]], ...] = (
        ("missing column", "Missing Column", ["Backfill Missing Column", "Replay Failed Batch"]),
        ("removed", "Missing Column", ["Backfill Missing Column", "Replay Failed Batch"]),
        ("added", "Schema Evolution", ["Evolve Schema", "Validate Schema", "Replay Failed Batch"]),
        ("type", "Type Drift", ["Type Coercion", "Replay Failed Batch"]),
        ("coercion", "Type Coercion Failure", ["Isolate Bad Rows", "Replay Clean Rows"]),
        ("unmatched", "Join Key Mismatch", ["Replay Quarantine Records", "Escalate Human Review"]),
        ("no such table", "Missing Destination Table", ["Create Missing Table", "Retry Loading"]),
        ("permission", "Destination Permission Failure", ["Escalate Human Review"]),
        ("memory", "Resource Pressure", ["Split Batch", "Reduce Batch Size", "Retry"]),
        ("resource", "Resource Pressure", ["Split Batch", "Reduce Batch Size", "Retry"]),
        ("database", "Database Connectivity", ["Reconnect Database", "Retry Loading"]),
        ("connection", "Database Connectivity", ["Reconnect Database", "Retry Loading"]),
        ("transform", "Transform Logic Failure", ["Replay Quarantine Records", "Escalate Human Review"]),
    )

    def lookup(self, failure_text: str) -> tuple[str, list[str]]:
        text = failure_text.lower()
        for needle, root_cause, actions in self._RULES:
            if needle in text:
                return root_cause, actions
        return "Unknown ETL Failure", ["Collect More Telemetry", "Escalate Human Review"]

