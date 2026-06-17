from __future__ import annotations


class RunbookStore:
    """Deterministic runbook hints used when LLM RCA is unavailable or unnecessary."""

    _RULES: tuple[tuple[str, str, list[str]], ...] = (
        ("crashloopbackoff", "CrashLoopBackOff", ["Restart Deployment", "Rollback Deployment", "Inspect Pod Logs", "Escalate Human Review"]),
        ("crashloop", "CrashLoopBackOff", ["Restart Deployment", "Rollback Deployment", "Inspect Pod Logs", "Escalate Human Review"]),
        ("imagepullbackoff", "ImagePullBackOff", ["Inspect Image", "Validate Registry", "Rollback Deployment"]),
        ("imagepull", "ImagePullBackOff", ["Inspect Image", "Validate Registry", "Rollback Deployment"]),
        ("oomkilled", "OOMKilled", ["Increase Memory Limit", "Escalate Human Review"]),
        ("out of memory", "OOMKilled", ["Increase Memory Limit", "Escalate Human Review"]),
        ("failed job", "K8sJobFailed", ["Retry Job", "Inspect Pod Logs", "Escalate Human Review"]),
        ("job failed", "K8sJobFailed", ["Retry Job", "Inspect Pod Logs", "Escalate Human Review"]),
        ("deployment degraded", "K8sDeploymentDegraded", ["Scale Deployment", "Restart Deployment", "Escalate Human Review"]),
        ("unavailable replicas", "K8sDeploymentDegraded", ["Scale Deployment", "Restart Deployment", "Escalate Human Review"]),
        ("scheduling", "PodSchedulingFailure", ["Inspect Node Resources", "Escalate Human Review"]),
        ("unschedulable", "PodSchedulingFailure", ["Inspect Node Resources", "Escalate Human Review"]),
        ("secret", "MissingSecret", ["Refresh Secret", "Escalate Human Review"]),
        ("configmap", "MissingConfigMap", ["Refresh ConfigMap", "Escalate Human Review"]),
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
        ("429", "API Rate Limit", ["Apply Exponential Backoff", "Retry Extraction"]),
        ("rate limit", "API Rate Limit", ["Apply Exponential Backoff", "Retry Extraction"]),
        ("too many requests", "API Rate Limit", ["Apply Exponential Backoff", "Retry Extraction"]),
        ("timeout", "Connectivity Timeout", ["Retry Staging Task", "Retry Loading"]),
        ("destination unreachable", "Connectivity Timeout", ["Retry Staging Task", "Retry Loading"]),
        ("concurrent modification", "Concurrent Staging Write", ["Rollback Transaction", "Retry Staging Task"]),
        ("delta conflict", "Concurrent Staging Write", ["Rollback Transaction", "Retry Staging Task"]),
        ("constraint violation", "Staging Data Quality Violation", ["Isolate Bad Rows", "Replay Clean Rows"]),
        ("duplicate key", "Staging Data Quality Violation", ["Isolate Bad Rows", "Replay Clean Rows"]),
        ("unexpected null", "Staging Data Quality Violation", ["Isolate Bad Rows", "Replay Clean Rows"]),
        ("transform", "Transform Logic Failure", ["Replay Quarantine Records", "Escalate Human Review"]),
    )

    def lookup(self, failure_text: str) -> tuple[str, list[str]]:
        text = failure_text.lower()
        for needle, root_cause, actions in self._RULES:
            if needle in text:
                return root_cause, actions
        return "Unknown ETL Failure", ["Collect More Telemetry", "Escalate Human Review"]
