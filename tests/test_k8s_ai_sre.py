from __future__ import annotations

import tempfile
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace

from sqlalchemy import select
from sqlalchemy.orm import Session

from agents.k8s_observer import K8sObserver
from agents.ai_sre_loop import AISRELoop
from agents.planner_agent import PlannerAgent
from agents.rca_agent import RCAAgent
from config import AutonomousConfig
from healing.k8s_healing import K8sHealingEngine
from models import HealingAction, HumanApproval, PipelineEvent, init_db
from observability.event_bus import EventBus


class FakeList:
    def __init__(self, items):
        self.items = items


class FakeCoreV1:
    def __init__(self, pods=None):
        self.pods = pods or []

    def list_namespace(self, limit=1):
        return FakeList([])

    def list_namespaced_pod(self, namespace):
        return FakeList(self.pods)

    def read_namespaced_pod_log(self, pod, namespace, tail_lines=200):
        return "pod failed because image pull failed"

    def list_node(self):
        node = SimpleNamespace(
            metadata=SimpleNamespace(name="node-1"),
            status=SimpleNamespace(capacity={"cpu": "4"}, allocatable={"cpu": "3"}),
        )
        return FakeList([node])


class FakeAppsV1:
    def __init__(self, deployments=None):
        self.deployments = deployments or []
        self.patched = []
        self.scaled = []

    def list_namespaced_deployment(self, namespace):
        return FakeList(self.deployments)

    def patch_namespaced_deployment(self, name, namespace, body):
        self.patched.append((name, namespace, body))

    def patch_namespaced_deployment_scale(self, name, namespace, body):
        self.scaled.append((name, namespace, body))

    def read_namespaced_deployment(self, name, namespace):
        container = SimpleNamespace(name="etl", image="self-healing-etl:latest")
        template_meta = SimpleNamespace(annotations={"self-healing-etl/previous-image": "self-healing-etl:latest"})
        return SimpleNamespace(
            spec=SimpleNamespace(
                template=SimpleNamespace(
                    metadata=template_meta,
                    spec=SimpleNamespace(containers=[container]),
                )
            )
        )


class FakeBatchV1:
    def __init__(self, jobs=None):
        self.jobs = jobs or []

    def list_namespaced_job(self, namespace):
        return FakeList(self.jobs)


def pod(name, reason, restart_count=0):
    status = SimpleNamespace(
        restart_count=restart_count,
        state=SimpleNamespace(waiting=SimpleNamespace(reason=reason)),
        last_state=SimpleNamespace(terminated=None),
    )
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, namespace="default", uid=f"uid-{name}"),
        status=SimpleNamespace(container_statuses=[status]),
    )


def deployment(name, replicas, available):
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, namespace="default", uid=f"uid-{name}"),
        status=SimpleNamespace(replicas=replicas, available_replicas=available),
    )


def job(name, failed):
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, namespace="default", uid=f"uid-{name}"),
        status=SimpleNamespace(failed=failed),
    )


class K8sAiSreTests(unittest.TestCase):
    def setUp(self) -> None:
        path = Path(tempfile.gettempdir()) / f"k8s_ai_sre_{uuid.uuid4().hex}.db"
        self.db_url = f"sqlite:///{path.as_posix()}"

    def test_k8s_observer_publishes_failure_events(self):
        observer = K8sObserver(
            self.db_url,
            core_v1=FakeCoreV1([pod("api", "CrashLoopBackOff", 5), pod("worker", "ImagePullBackOff")]),
            apps_v1=FakeAppsV1([deployment("etl", 2, 1)]),
            batch_v1=FakeBatchV1([job("nightly", 1)]),
        )
        events = observer.poll_once()
        event_types = {event.event_type for event in events}
        self.assertIn("K8sPodCrashLoopDetected", event_types)
        self.assertIn("K8sImagePullFailed", event_types)
        self.assertIn("K8sHighRestartCount", event_types)
        self.assertIn("K8sDeploymentDegraded", event_types)
        self.assertIn("K8sJobFailed", event_types)

    def test_k8s_observer_degrades_when_cluster_unavailable(self):
        observer = K8sObserver(self.db_url, core_v1=object(), apps_v1=object(), batch_v1=object())
        self.assertFalse(observer.probe_cluster())

    def test_rca_classifies_k8s_failures(self):
        event = EventBus(self.db_url).emit(
            run_id="k8s-api",
            pipeline_name="k8s_ai_sre",
            event_type="K8sPodCrashLoopDetected",
            severity="ERROR",
            component="k8s_observer",
            message="CrashLoopBackOff for pod api",
            trace_id="trace-1",
            metadata={"name": "api"},
        )
        rca = RCAAgent(self.db_url, AutonomousConfig(ollama_enabled=False)).analyze(event)
        self.assertEqual(rca.root_cause, "CrashLoopBackOff")

    def test_planner_generates_k8s_plan(self):
        event = EventBus(self.db_url).emit(
            run_id="k8s-worker",
            pipeline_name="k8s_ai_sre",
            event_type="K8sImagePullFailed",
            severity="ERROR",
            component="k8s_observer",
            message="ImagePullBackOff",
            trace_id="trace-1",
        )
        rca = RCAAgent(self.db_url, AutonomousConfig(ollama_enabled=False)).analyze(event)
        plan = PlannerAgent().create_plan(rca)
        self.assertEqual([step.action_type for step in plan], ["inspect_image", "validate_registry", "rollback_deployment"])

    def test_ai_sre_loop_deduplicates_repeated_events(self):
        apps = FakeAppsV1()
        k8s_observer = K8sObserver(
            self.db_url,
            core_v1=FakeCoreV1([pod("self-healing-etl-abc", "CrashLoopBackOff", 5)]),
            apps_v1=apps,
            batch_v1=FakeBatchV1(),
        )
        loop = AISRELoop(
            self.db_url,
            namespace="default",
            observer=k8s_observer,
            healing_engine=K8sHealingEngine(self.db_url, apps_v1=apps, core_v1=FakeCoreV1(), batch_v1=FakeBatchV1()),
        )
        first = loop.k8s_observer.poll_once()
        db = init_db(self.db_url)
        with Session(db) as session:
            actions_after_first = list(session.scalars(select(HealingAction)).all())
        second = loop.k8s_observer.poll_once()
        self.assertGreaterEqual(len(first), 1)
        self.assertGreaterEqual(len(second), 1)
        with Session(db) as session:
            actions_after_second = list(session.scalars(select(HealingAction)).all())
        self.assertGreaterEqual(len(actions_after_first), 1)
        self.assertEqual(len(actions_after_first), len(actions_after_second))

    def test_k8s_healing_allows_blocks_and_escalates(self):
        apps = FakeAppsV1()
        engine = K8sHealingEngine(self.db_url, apps_v1=apps, core_v1=FakeCoreV1(), batch_v1=FakeBatchV1())
        restart = engine.execute(
            "restart_deployment",
            run_id="run-1",
            trace_id="trace-1",
            root_cause="CrashLoopBackOff",
            parameters={"deployment": "etl"},
        )
        forbidden = engine.execute(
            "namespace_deletion",
            run_id="run-1",
            trace_id="trace-1",
            root_cause="Unsafe",
        )
        approval = engine.execute(
            "increase_memory_limit",
            run_id="run-1",
            trace_id="trace-1",
            root_cause="OOMKilled",
            parameters={"deployment": "etl"},
        )
        self.assertTrue(restart.success)
        self.assertFalse(forbidden.success)
        self.assertFalse(approval.success)
        self.assertIsNotNone(approval.approval_id)
        self.assertEqual(len(apps.patched), 1)

    def test_k8s_healing_audit_trail_is_recorded(self):
        engine = K8sHealingEngine(self.db_url, apps_v1=FakeAppsV1(), core_v1=FakeCoreV1(), batch_v1=FakeBatchV1())
        engine.execute(
            "collect_more_telemetry",
            run_id="run-1",
            trace_id="trace-1",
            root_cause="Unknown",
            parameters={"pod": "api"},
        )
        db = init_db(self.db_url)
        with Session(db) as session:
            actions = list(session.scalars(select(HealingAction)).all())
            approvals = list(session.scalars(select(HumanApproval)).all())
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0].agent, "K8sHealingEngine")
        self.assertEqual(len(approvals), 0)

    def test_dashboard_contains_k8s_tabs_and_queries_existing_tables(self):
        app = Path("taxi_etl/dashboard/app.py").read_text()
        self.assertIn("Kubernetes Health", app)
        self.assertIn("Cluster Events", app)
        self.assertIn("AI-SRE Incidents", app)
        self.assertIn("Recovery Metrics", app)

    def test_k8s_diagnostic_logs_are_persisted_to_pipeline_events(self):
        engine = K8sHealingEngine(self.db_url, apps_v1=FakeAppsV1(), core_v1=FakeCoreV1(), batch_v1=FakeBatchV1())
        result = engine.execute(
            "inspect_pod_logs",
            run_id="run-1",
            trace_id="trace-1",
            root_cause="CrashLoopBackOff",
            parameters={"pod": "api", "run_id": "run-1", "trace_id": "trace-1"},
        )
        self.assertTrue(result.success)
        db = init_db(self.db_url)
        with Session(db) as session:
            events = list(session.scalars(select(PipelineEvent).where(PipelineEvent.event_type == "K8sDiagnosticLog")).all())
        self.assertEqual(len(events), 1)


if __name__ == "__main__":
    unittest.main()
