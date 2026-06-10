from __future__ import annotations

from typing import Any

from agents.planner_agent import PlanStep
from healing.autonomous_healing import AutonomousHealingEngine, HealingExecutionResult


class HealerAgent:
    def __init__(self, engine: AutonomousHealingEngine):
        self.engine = engine

    def execute(
        self,
        plan: list[PlanStep],
        *,
        run_id: str,
        trace_id: str,
        root_cause: str,
        context: dict[str, Any],
    ) -> list[HealingExecutionResult]:
        results: list[HealingExecutionResult] = []
        for step in plan:
            results.append(
                self.engine.execute(
                    step.action_type,
                    run_id=run_id,
                    trace_id=trace_id,
                    root_cause=root_cause,
                    parameters={**step.parameters, "context": context},
                )
            )
        return results

