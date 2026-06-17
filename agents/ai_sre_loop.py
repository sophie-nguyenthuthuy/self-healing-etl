from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Any

from agents.autonomous_loop import AutonomousHealingLoop, AutonomousLoopResult
from agents.k8s_observer import K8sObserver
from agents.observer_agent import ObserverAgent
from config import AutonomousConfig
from healing.k8s_healing import K8sHealingEngine
from observability.event_bus import EventBus, ObservabilityEvent

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class DedupEntry:
    seen_at: float
    event_type: str
    object_name: str


class AISRELoop:
    """Connects K8s observation to the existing autonomous RCA/planning/healing flow."""

    def __init__(
        self,
        db_url: str,
        *,
        namespace: str = "default",
        pipeline_name: str = "k8s_ai_sre",
        poll_interval_seconds: int = 30,
        dedup_window_seconds: int = 120,
        config: AutonomousConfig | None = None,
        observer: K8sObserver | None = None,
        healing_engine: K8sHealingEngine | None = None,
    ):
        self.db_url = db_url
        self.namespace = namespace
        self.pipeline_name = pipeline_name
        self.dedup_window_seconds = dedup_window_seconds
        self.event_bus = EventBus(db_url)
        self.k8s_observer = observer or K8sObserver(
            db_url,
            namespace=namespace,
            pipeline_name=pipeline_name,
            poll_interval_seconds=poll_interval_seconds,
            event_bus=self.event_bus,
        )
        if observer:
            self.k8s_observer.event_bus = self.event_bus
        self.loop = AutonomousHealingLoop(
            db_url,
            config or AutonomousConfig(enable_autonomous_healing=True, ollama_enabled=False),
            healing_engine=healing_engine or K8sHealingEngine(db_url, namespace=namespace, event_bus=self.event_bus),
        )
        self.observer_agent = ObserverAgent(on_failure=self.handle_failure)
        self.event_bus.subscribe(self.observer_agent.handle_event)
        self._dedup: dict[str, DedupEntry] = {}
        self._poll_interval_seconds = poll_interval_seconds

    def run_forever(self) -> None:
        LOGGER.info("Starting AI-SRE loop for namespace=%s", self.namespace)
        if not self.k8s_observer.probe_cluster():
            LOGGER.warning("Kubernetes cluster unavailable; AI-SRE loop will exit gracefully.")
            return
        while True:
            self.k8s_observer.poll_once()
            time.sleep(self._poll_interval_seconds)

    def handle_failure(self, event: ObservabilityEvent) -> AutonomousLoopResult | None:
        key = self._dedup_key(event)
        now = time.time()
        previous = self._dedup.get(key)
        if previous and now - previous.seen_at < self.dedup_window_seconds:
            LOGGER.info("Skipping duplicate K8s failure event: %s", key)
            return None
        self._dedup[key] = DedupEntry(now, event.event_type, event.metadata.get("name", "unknown"))
        context = {
            "namespace": event.metadata.get("namespace", self.namespace),
            "pod": event.metadata.get("name"),
            "deployment": event.metadata.get("deployment", event.metadata.get("name")),
            "job": event.metadata.get("name"),
            "summary": {"rows_loaded": 1, "rows_quarantined": 0},
        }
        LOGGER.info("Handling K8s failure event=%s object=%s", event.event_type, event.metadata.get("name"))
        return self.loop.handle_failure(event, context=context)

    def _dedup_key(self, event: ObservabilityEvent) -> str:
        return "|".join(
            [
                event.event_type,
                event.component,
                str(event.metadata.get("namespace", self.namespace)),
                str(event.metadata.get("name", event.run_id)),
            ]
        )


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    db_url = os.getenv("QUARANTINE_DB", os.getenv("DATABASE_URL", "sqlite:///quarantine.db"))
    namespace = os.getenv("K8S_NAMESPACE", "default")
    pipeline_name = os.getenv("ETL_PIPELINE_NAME", "k8s_ai_sre")
    poll_interval = int(os.getenv("K8S_POLL_INTERVAL_SECONDS", "30"))
    AISRELoop(db_url, namespace=namespace, pipeline_name=pipeline_name, poll_interval_seconds=poll_interval).run_forever()


if __name__ == "__main__":
    main()
