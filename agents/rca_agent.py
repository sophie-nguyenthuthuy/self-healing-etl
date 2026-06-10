from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from config import AutonomousConfig
from knowledge.incident_store import IncidentStore
from knowledge.runbook_store import RunbookStore
from llm.ollama_client import OllamaClient
from observability.event_bus import ObservabilityEvent


@dataclass
class RCAResult:
    root_cause: str
    confidence: float
    evidence: list[str] = field(default_factory=list)
    recommended_actions: list[str] = field(default_factory=list)
    mode: str = "deterministic"

    def as_dict(self) -> dict[str, Any]:
        return {
            "root_cause": self.root_cause,
            "confidence": self.confidence,
            "evidence": self.evidence,
            "recommended_actions": self.recommended_actions,
            "mode": self.mode,
        }


class RCAAgent:
    PROMPT = """You are a Senior ETL Site Reliability Engineer.

Analyze the ETL failure.

Determine:
- root cause
- confidence
- evidence
- remediation actions

Return JSON only.
"""

    def __init__(
        self,
        db_url: str,
        config: AutonomousConfig | None = None,
        ollama: OllamaClient | None = None,
    ):
        self.config = config or AutonomousConfig()
        self.incidents = IncidentStore(db_url)
        self.runbooks = RunbookStore()
        self.ollama = ollama or OllamaClient(
            base_url=self.config.ollama_url,
            model=self.config.ollama_model,
            fallback_model=self.config.ollama_fallback_model,
        )

    def analyze(
        self,
        event: ObservabilityEvent,
        *,
        recent_events: list[Any] | None = None,
        telemetry: dict[str, Any] | None = None,
        historical_context: list[Any] | None = None,
    ) -> RCAResult:
        failure_text = self._failure_text(event, recent_events, telemetry)
        match = self.incidents.find_similar(failure_text)
        if match:
            return RCAResult(
                root_cause=match.root_cause,
                confidence=min(0.99, match.confidence),
                evidence=["Matched successful historical incident"],
                recommended_actions=[match.healing_action] if match.healing_action else [],
                mode="incident_memory",
            )

        if self.config.ollama_enabled:
            payload = self.ollama.generate_json(self._prompt(failure_text, event, telemetry, historical_context))
            parsed = self._parse_llm(payload)
            if parsed:
                parsed.mode = "ollama"
                return parsed

        root_cause, actions = self.runbooks.lookup(failure_text)
        return RCAResult(
            root_cause=root_cause,
            confidence=0.72 if root_cause != "Unknown ETL Failure" else 0.45,
            evidence=[event.message, f"event_type={event.event_type}", f"component={event.component}"],
            recommended_actions=actions,
            mode="deterministic",
        )

    def _prompt(
        self,
        failure_text: str,
        event: ObservabilityEvent,
        telemetry: dict[str, Any] | None,
        historical_context: list[Any] | None,
    ) -> str:
        context = {
            "failure": failure_text,
            "event": event.metadata,
            "telemetry": telemetry or {},
            "historical_context": [str(item) for item in historical_context or []],
        }
        return self.PROMPT + "\nFailure context:\n" + json.dumps(context, default=str)

    def _parse_llm(self, payload: dict[str, Any] | None) -> RCAResult | None:
        if not payload:
            return None
        try:
            return RCAResult(
                root_cause=str(payload["root_cause"]),
                confidence=float(payload.get("confidence", 0.7)),
                evidence=[str(item) for item in payload.get("evidence", [])],
                recommended_actions=[str(item) for item in payload.get("recommended_actions", [])],
            )
        except Exception:
            return None

    def _failure_text(
        self,
        event: ObservabilityEvent,
        recent_events: list[Any] | None,
        telemetry: dict[str, Any] | None,
    ) -> str:
        chunks = [event.event_type, event.component, event.message, json.dumps(event.metadata, default=str)]
        for recent in recent_events or []:
            chunks.append(getattr(recent, "message", str(recent)))
        if telemetry:
            chunks.append(json.dumps(telemetry, default=str))
        return " | ".join(chunks)

