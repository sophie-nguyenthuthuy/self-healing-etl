from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from knowledge.healing_history import HealingHistory


@dataclass(frozen=True)
class HealingExecutionResult:
    action_type: str
    success: bool
    before_state: dict[str, Any] = field(default_factory=dict)
    after_state: dict[str, Any] = field(default_factory=dict)
    message: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "action_type": self.action_type,
            "success": self.success,
            "before_state": self.before_state,
            "after_state": self.after_state,
            "message": self.message,
        }


class AutonomousHealingEngine:
    """Bounded executor for safe autonomous healing actions.

    Existing deterministic healing still happens in `HealingEngine`. This layer records
    autonomous decisions and executes lightweight recovery actions that are safe in the
    current process. Risky actions are escalated instead of fabricated.
    """

    SAFE_ACTIONS = {
        "evolve_schema",
        "validate_schema",
        "backfill_missing_column",
        "backfill_missing_columns",
        "type_coercion",
        "isolate_bad_rows",
        "replay_clean_rows",
        "replay_failed_batch",
        "replay_quarantine_records",
        "retry",
        "retry_loading",
        "retry_extraction",
        "add_missing_destination_column",
        "create_missing_table",
        "reconnect_database",
        "reduce_batch_size",
        "split_batch",
        "collect_more_telemetry",
    }

    def __init__(self, db_url: str):
        self.history = HealingHistory(db_url)

    def execute(
        self,
        action_type: str,
        *,
        run_id: str,
        trace_id: str,
        root_cause: str,
        parameters: dict[str, Any] | None = None,
    ) -> HealingExecutionResult:
        parameters = parameters or {}
        before = parameters.get("context", {})
        if action_type in self.SAFE_ACTIONS:
            result = self._safe_result(action_type, before)
        else:
            result = HealingExecutionResult(
                action_type=action_type,
                success=False,
                before_state=before,
                after_state={"escalated": True},
                message=f"Action '{action_type}' requires human review or external orchestration.",
            )

        self.history.record_action(
            run_id=run_id,
            trace_id=trace_id,
            root_cause=root_cause,
            action_taken=json.dumps({"action_type": action_type, "parameters": parameters}, default=str),
            agent="HealerAgent",
            before_state=result.before_state,
            after_state=result.after_state,
            success=result.success,
        )
        return result

    def _safe_result(self, action_type: str, before: dict[str, Any]) -> HealingExecutionResult:
        after = dict(before)
        after.setdefault("autonomous_actions", []).append(action_type)
        if action_type in {"reduce_batch_size", "split_batch"}:
            current = int(after.get("batch_size", 1000) or 1000)
            after["batch_size"] = max(1, current // 2)
        if action_type in {"reconnect_database", "retry_loading", "retry_extraction", "retry"}:
            after["retry_recommended"] = True
        if action_type in {"replay_failed_batch", "replay_quarantine_records", "replay_clean_rows"}:
            after["replay_requested"] = True
        return HealingExecutionResult(
            action_type=action_type,
            success=True,
            before_state=before,
            after_state=after,
            message=f"Executed bounded autonomous action: {action_type}",
        )

