from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from agents.approvals import ApprovalStore
from agents.healer_agent import HealerAgent
from agents.observer_agent import ObserverAgent
from agents.planner_agent import PlannerAgent
from agents.rca_agent import RCAAgent, RCAResult
from agents.validator_agent import ValidationResult, ValidatorAgent
from config import AutonomousConfig
from healing.autonomous_healing import AutonomousHealingEngine, HealingExecutionResult
from knowledge.incident_store import IncidentStore
from observability.event_bus import EventBus, ObservabilityEvent
from observability.telemetry import TelemetryReader
from observability.traces import new_span_id


@dataclass
class AutonomousLoopResult:
    success: bool
    attempts: int
    rca: RCAResult
    validation: ValidationResult
    actions: list[HealingExecutionResult]
    escalated: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "attempts": self.attempts,
            "rca": self.rca.as_dict(),
            "validation": self.validation.as_dict(),
            "actions": [action.as_dict() for action in self.actions],
            "escalated": self.escalated,
        }


class AutonomousHealingLoop:
    def __init__(
        self,
        db_url: str,
        config: AutonomousConfig,
        *,
        healing_engine: Any | None = None,
    ):
        self.db_url = db_url
        self.config = config
        self.event_bus = EventBus(db_url)
        self.observer = ObserverAgent()
        self.rca = RCAAgent(db_url, config)
        self.planner = PlannerAgent()
        self.healer = HealerAgent(healing_engine or AutonomousHealingEngine(db_url))
        self.validator = ValidatorAgent()
        self.telemetry = TelemetryReader(db_url)
        self.incidents = IncidentStore(db_url)
        self.approvals = ApprovalStore(db_url)

    def handle_failure(
        self,
        event: ObservabilityEvent,
        *,
        context: dict[str, Any] | None = None,
    ) -> AutonomousLoopResult:
        context = context or {}
        all_actions: list[HealingExecutionResult] = []
        validation = ValidationResult(False, 0.0, ["not evaluated"])
        rca = self.rca.analyze(event)

        for attempt in range(1, self.config.max_healing_attempts + 1):
            recent_events = self.event_bus.recent_events(trace_id=event.trace_id, limit=50)
            telemetry = self.telemetry.snapshot(event.pipeline_name).as_dict()
            rca = self.rca.analyze(event, recent_events=recent_events, telemetry=telemetry)
            plan = self.planner.create_plan(rca, context)
            plan_json = self.planner.to_json(plan)

            self.event_bus.emit(
                run_id=event.run_id,
                pipeline_name=event.pipeline_name,
                event_type="HealingStarted",
                severity="INFO",
                component="AutonomousHealingLoop",
                message=f"Attempt {attempt}: {rca.root_cause}",
                trace_id=event.trace_id,
                span_id=new_span_id("healing-loop"),
                metadata={"rca": rca.as_dict(), "plan": [step.as_dict() for step in plan]},
            )

            if self.config.require_human_approval:
                approval_id = self.approvals.request(
                    run_id=event.run_id,
                    trace_id=event.trace_id,
                    plan_json=plan_json,
                )
                validation = ValidationResult(False, 0.5, [f"approval_required={approval_id}"])
                self.event_bus.emit(
                    run_id=event.run_id,
                    pipeline_name=event.pipeline_name,
                    event_type="HumanApprovalRequested",
                    severity="WARNING",
                    component="PlannerAgent",
                    message="Autonomous healing paused for human approval",
                    trace_id=event.trace_id,
                    span_id=new_span_id("approval"),
                    metadata={"approval_id": approval_id, "plan": plan_json},
                )
                return AutonomousLoopResult(False, attempt, rca, validation, all_actions, escalated=True)

            action_results = self.healer.execute(
                plan,
                run_id=event.run_id,
                trace_id=event.trace_id,
                root_cause=rca.root_cause,
                context=context,
            )
            all_actions.extend(action_results)
            validation_context = {
                **context,
                "action_results": action_results,
                "max_quarantine_rows": context.get("max_quarantine_rows", 0),
            }
            validation = self.validator.validate(validation_context)
            self.event_bus.emit(
                run_id=event.run_id,
                pipeline_name=event.pipeline_name,
                event_type="HealingCompleted" if validation.success else "HealingValidationFailed",
                severity="INFO" if validation.success else "WARNING",
                component="ValidatorAgent",
                message="Autonomous healing validated" if validation.success else "Autonomous healing did not validate",
                trace_id=event.trace_id,
                span_id=new_span_id("validation"),
                metadata={"validation": validation.as_dict(), "actions": [r.as_dict() for r in action_results]},
            )

            if validation.success:
                action_summary = ", ".join(action.action_type for action in action_results)
                self.incidents.record(
                    failure_text=event.message,
                    root_cause=rca.root_cause,
                    healing_action=action_summary,
                    successful=True,
                    source_domain="K8S" if event.component.startswith("k8s_") else "ETL",
                )
                self.event_bus.emit(
                    run_id=event.run_id,
                    pipeline_name=event.pipeline_name,
                    event_type="PipelineRecovered",
                    severity="INFO",
                    component="AutonomousHealingLoop",
                    message=f"Recovered after {attempt} autonomous attempt(s)",
                    trace_id=event.trace_id,
                    span_id=new_span_id("recovery"),
                    metadata={"attempts": attempt, "root_cause": rca.root_cause},
                )
                return AutonomousLoopResult(True, attempt, rca, validation, all_actions)

        self.incidents.record(
            failure_text=event.message,
            root_cause=rca.root_cause,
            healing_action=", ".join(action.action_type for action in all_actions),
            successful=False,
            source_domain="K8S" if event.component.startswith("k8s_") else "ETL",
        )
        self.event_bus.emit(
            run_id=event.run_id,
            pipeline_name=event.pipeline_name,
            event_type="PipelineEscalated",
            severity="ERROR",
            component="AutonomousHealingLoop",
            message="Autonomous healing exhausted attempts; human review required",
            trace_id=event.trace_id,
            span_id=new_span_id("escalation"),
            metadata={"attempts": self.config.max_healing_attempts, "root_cause": rca.root_cause},
        )
        return AutonomousLoopResult(False, self.config.max_healing_attempts, rca, validation, all_actions, escalated=True)
