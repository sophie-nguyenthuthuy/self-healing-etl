from __future__ import annotations

from collections import deque
from typing import Callable

from observability.event_bus import ObservabilityEvent


class ObserverAgent:
    """Watches the event stream and triggers RCA for meaningful failures."""

    FAILURE_EVENTS = {
        "TransformFailed",
        "SchemaDriftDetected",
        "LoadFailed",
        "QuarantineTriggered",
        "PipelineFailed",
        "ResourceExhausted",
        "DatabaseConnectionFailed",
        "K8sPodCrashLoopDetected",
        "K8sImagePullFailed",
        "K8sOOMKilled",
        "K8sJobFailed",
        "K8sDeploymentDegraded",
        "K8sHighRestartCount",
    }

    def __init__(self, on_failure: Callable[[ObservabilityEvent], None] | None = None):
        self.on_failure = on_failure
        self.recent: deque[ObservabilityEvent] = deque(maxlen=200)

    def handle_event(self, event: ObservabilityEvent) -> None:
        self.recent.append(event)
        if self.is_failure(event) and self.on_failure:
            self.on_failure(event)

    def is_failure(self, event: ObservabilityEvent) -> bool:
        if event.severity == "ERROR":
            return True
        if event.event_type in self.FAILURE_EVENTS:
            return True
        if event.event_type == "QuarantineTriggered" and event.metadata.get("rows_quarantined", 0) > 0:
            return True
        return False
