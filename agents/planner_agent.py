from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from agents.rca_agent import RCAResult


@dataclass
class PlanStep:
    step: str
    action_type: str
    parameters: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "step": self.step,
            "action_type": self.action_type,
            "parameters": self.parameters,
        }


class PlannerAgent:
    ACTION_MAP = {
        "schema": ["Evolve Schema", "Validate Schema", "Replay Failed Batch"],
        "missing column": ["Backfill Missing Column", "Replay Failed Batch"],
        "type": ["Type Coercion", "Replay Failed Batch"],
        "coercion": ["Isolate Bad Rows", "Replay Clean Rows"],
        "join": ["Replay Quarantine Records", "Escalate Human Review"],
        "destination": ["Add Missing Destination Column", "Retry Loading"],
        "table": ["Create Missing Table", "Retry Loading"],
        "database": ["Reconnect Database", "Retry Loading"],
        "api rate": ["Apply Exponential Backoff", "Retry Extraction"],
        "rate limit": ["Apply Exponential Backoff", "Retry Extraction"],
        "too many requests": ["Apply Exponential Backoff", "Retry Extraction"],
        "timeout": ["Retry Staging Task", "Retry Loading"],
        "connectivity": ["Retry Staging Task", "Retry Loading"],
        "concurrent": ["Rollback Transaction", "Retry Staging Task"],
        "staging write": ["Rollback Transaction", "Retry Staging Task"],
        "data quality": ["Isolate Bad Rows", "Replay Clean Rows"],
        "constraint": ["Isolate Bad Rows", "Replay Clean Rows"],
        "resource": ["Split Batch", "Reduce Batch Size", "Retry"],
        "permission": ["Escalate Human Review"],
        "crashloop": ["Restart Deployment", "Rollback Deployment", "Inspect Pod Logs", "Escalate Human Review"],
        "imagepull": ["Inspect Image", "Validate Registry", "Rollback Deployment"],
        "oomkilled": ["Increase Memory Limit", "Escalate Human Review"],
        "k8s job": ["Retry Job", "Inspect Pod Logs", "Escalate Human Review"],
        "deploymentdegraded": ["Scale Deployment", "Restart Deployment", "Escalate Human Review"],
        "deployment degraded": ["Scale Deployment", "Restart Deployment", "Escalate Human Review"],
        "scheduling": ["Inspect Node Resources", "Escalate Human Review"],
        "k8s secret": ["Refresh Secret", "Escalate Human Review"],
        "missingsecret": ["Refresh Secret", "Escalate Human Review"],
        "k8s configmap": ["Refresh ConfigMap", "Escalate Human Review"],
        "missingconfigmap": ["Refresh ConfigMap", "Escalate Human Review"],
        "unknown": ["Collect More Telemetry", "Escalate Human Review"],
    }

    def create_plan(self, rca: RCAResult, context: dict[str, Any] | None = None) -> list[PlanStep]:
        actions = rca.recommended_actions or self._actions_for_root_cause(rca.root_cause)
        return [
            PlanStep(
                step=action,
                action_type=self._normalize_action(action),
                parameters={"root_cause": rca.root_cause, "context": context or {}},
            )
            for action in actions
        ]

    def to_json(self, plan: list[PlanStep]) -> str:
        return json.dumps([step.as_dict() for step in plan], default=str)

    def _actions_for_root_cause(self, root_cause: str) -> list[str]:
        lower = root_cause.lower()
        for needle, actions in self.ACTION_MAP.items():
            if needle in lower:
                return actions
        return self.ACTION_MAP["unknown"]

    def _normalize_action(self, action: str) -> str:
        return action.lower().replace(" ", "_").replace("-", "_")
