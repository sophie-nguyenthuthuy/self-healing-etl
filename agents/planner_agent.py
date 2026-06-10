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
        "resource": ["Split Batch", "Reduce Batch Size", "Retry"],
        "permission": ["Escalate Human Review"],
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

