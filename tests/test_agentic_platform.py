from __future__ import annotations

import tempfile
import unittest
import uuid
from pathlib import Path

from agents.autonomous_loop import AutonomousHealingLoop
from agents.observer_agent import ObserverAgent
from agents.planner_agent import PlannerAgent
from agents.rca_agent import RCAAgent
from agents.validator_agent import ValidatorAgent
from config import AutonomousConfig
from knowledge.incident_store import IncidentStore
from llm.ollama_client import OllamaClient
from observability.event_bus import EventBus
from observability.metrics import ObservabilityMetrics


class UnavailableOllama(OllamaClient):
    def generate_json(self, prompt: str, timeout_seconds: float = 8.0):
        return None


class AgenticPlatformTests(unittest.TestCase):
    def setUp(self) -> None:
        path = Path(tempfile.gettempdir()) / f"agentic_platform_tests_{uuid.uuid4().hex}.db"
        self.db_url = f"sqlite:///{path.as_posix()}"

    def test_event_bus_persists_traceable_event(self):
        bus = EventBus(self.db_url)
        bus.emit(
            run_id="run-1",
            pipeline_name="pipe",
            event_type="PipelineStarted",
            severity="INFO",
            component="test",
            message="started",
            trace_id="trace-1",
            span_id="span-1",
        )
        events = bus.recent_events(trace_id="trace-1")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].span_id, "span-1")

    def test_observer_detects_error_event(self):
        bus = EventBus(self.db_url)
        seen = []
        observer = ObserverAgent(on_failure=seen.append)
        event = bus.emit(
            run_id="run-1",
            pipeline_name="pipe",
            event_type="TransformFailed",
            severity="ERROR",
            component="transformer",
            message="bad transform",
            trace_id="trace-1",
        )
        observer.handle_event(event)
        self.assertEqual(len(seen), 1)

    def test_rca_falls_back_when_ollama_unavailable(self):
        bus = EventBus(self.db_url)
        event = bus.emit(
            run_id="run-1",
            pipeline_name="pipe",
            event_type="LoadFailed",
            severity="ERROR",
            component="loader",
            message="database connection refused",
            trace_id="trace-1",
        )
        rca = RCAAgent(
            self.db_url,
            AutonomousConfig(ollama_enabled=True),
            ollama=UnavailableOllama(),
        ).analyze(event)
        self.assertEqual(rca.mode, "deterministic")
        self.assertIn("Database", rca.root_cause)

    def test_incident_memory_lookup(self):
        store = IncidentStore(self.db_url)
        store.record(
            failure_text="missing column status",
            root_cause="Missing Column",
            healing_action="Backfill Missing Column",
            successful=True,
        )
        match = store.find_similar("missing column status")
        self.assertIsNotNone(match)
        self.assertEqual(match.root_cause, "Missing Column")

    def test_planner_and_validator(self):
        bus = EventBus(self.db_url)
        event = bus.emit(
            run_id="run-1",
            pipeline_name="pipe",
            event_type="SchemaDriftDetected",
            severity="WARNING",
            component="drift_detector",
            message="added=['region']",
            trace_id="trace-1",
        )
        rca = RCAAgent(self.db_url, AutonomousConfig(ollama_enabled=False)).analyze(event)
        plan = PlannerAgent().create_plan(rca)
        self.assertGreaterEqual(len(plan), 1)
        validation = ValidatorAgent().validate({"summary": {"rows_loaded": 3, "rows_quarantined": 0}})
        self.assertTrue(validation.success)

    def test_autonomous_loop_records_healing_action(self):
        bus = EventBus(self.db_url)
        event = bus.emit(
            run_id="run-1",
            pipeline_name="pipe",
            event_type="QuarantineTriggered",
            severity="ERROR",
            component="quarantine",
            message="coercion failure",
            trace_id="trace-1",
            metadata={"rows_quarantined": 1},
        )
        loop = AutonomousHealingLoop(
            self.db_url,
            AutonomousConfig(enable_autonomous_healing=True, ollama_enabled=False),
        )
        result = loop.handle_failure(event, context={"summary": {"rows_loaded": 3, "rows_quarantined": 1}})
        self.assertTrue(result.success)
        health = ObservabilityMetrics(self.db_url).health("pipe")
        self.assertGreater(health["healing_actions"], 0)


if __name__ == "__main__":
    unittest.main()
