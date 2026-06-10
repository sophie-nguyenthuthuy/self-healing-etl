from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ValidationResult:
    success: bool
    confidence: float
    evidence: list[str]

    def as_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "confidence": self.confidence,
            "evidence": self.evidence,
        }


class ValidatorAgent:
    def validate(self, context: dict[str, Any]) -> ValidationResult:
        summary = context.get("summary") or {}
        action_results = context.get("action_results") or []
        destination_reachable = context.get("destination_reachable", True)
        quarantine_acceptable = summary.get("rows_quarantined", 0) <= context.get("max_quarantine_rows", 0)
        loaded_or_isolated = summary.get("rows_loaded", 0) > 0 or any(
            getattr(result, "success", False) for result in action_results
        )
        success = bool(destination_reachable and (quarantine_acceptable or loaded_or_isolated))
        evidence = [
            f"destination_reachable={destination_reachable}",
            f"rows_loaded={summary.get('rows_loaded', 0)}",
            f"rows_quarantined={summary.get('rows_quarantined', 0)}",
            f"actions_successful={sum(1 for result in action_results if getattr(result, 'success', False))}",
        ]
        return ValidationResult(success=success, confidence=0.91 if success else 0.54, evidence=evidence)

