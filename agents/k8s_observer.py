from __future__ import annotations

import logging
import threading
import time
from typing import Any

from observability.event_bus import EventBus, ObservabilityEvent
from observability.traces import new_trace_id

LOGGER = logging.getLogger(__name__)


class K8sObserver:
    """Polls Kubernetes workload health and emits failures through EventBus.

    The Kubernetes client is imported lazily so local ETL demos continue to work when
    the dependency or cluster access is unavailable.
    """

    def __init__(
        self,
        db_url: str,
        *,
        namespace: str = "default",
        pipeline_name: str = "k8s_ai_sre",
        poll_interval_seconds: int = 30,
        restart_threshold: int = 3,
        core_v1: Any | None = None,
        apps_v1: Any | None = None,
        batch_v1: Any | None = None,
        event_bus: EventBus | None = None,
    ):
        self.namespace = namespace
        self.pipeline_name = pipeline_name
        self.poll_interval_seconds = poll_interval_seconds
        self.restart_threshold = restart_threshold
        self.event_bus = event_bus or EventBus(db_url)
        self.core_v1 = core_v1
        self.apps_v1 = apps_v1
        self.batch_v1 = batch_v1
        self._stop = threading.Event()

    def probe_cluster(self) -> bool:
        try:
            self._ensure_clients()
            self.core_v1.list_namespaced_pod(self.namespace, limit=1)
            return True
        except Exception as exc:
            LOGGER.warning("Kubernetes cluster is unavailable: %s", exc)
            return False

    def start_background(self) -> threading.Thread:
        thread = threading.Thread(target=self.watch_loop, name="k8s-observer", daemon=True)
        thread.start()
        return thread

    def stop(self) -> None:
        self._stop.set()

    def watch_loop(self) -> None:
        if not self.probe_cluster():
            return
        while not self._stop.is_set():
            self.poll_once()
            self._stop.wait(self.poll_interval_seconds)

    def poll_once(self) -> list[ObservabilityEvent]:
        try:
            self._ensure_clients()
            events: list[ObservabilityEvent] = []
            pods = self.core_v1.list_namespaced_pod(self.namespace).items
            for pod in pods:
                if getattr(getattr(pod, "metadata", None), "deletion_timestamp", None):
                    continue
                events.extend(self._events_for_pod(pod))
            deployments = self.apps_v1.list_namespaced_deployment(self.namespace).items
            for deployment in deployments:
                event = self._event_for_deployment(deployment)
                if event:
                    events.append(event)
            jobs = self.batch_v1.list_namespaced_job(self.namespace).items
            for job in jobs:
                event = self._event_for_job(job)
                if event:
                    events.append(event)
            return events
        except Exception as exc:
            LOGGER.warning("Kubernetes observer poll failed: %s", exc)
            return []

    def _ensure_clients(self) -> None:
        if self.core_v1 and self.apps_v1 and self.batch_v1:
            return
        try:
            from kubernetes import client, config
        except Exception as exc:
            raise RuntimeError("kubernetes Python client is not installed") from exc

        try:
            config.load_incluster_config()
        except Exception:
            config.load_kube_config()
        self.core_v1 = client.CoreV1Api()
        self.apps_v1 = client.AppsV1Api()
        self.batch_v1 = client.BatchV1Api()

    def _events_for_pod(self, pod: Any) -> list[ObservabilityEvent]:
        events: list[ObservabilityEvent] = []
        metadata = self._object_metadata(pod)
        statuses = getattr(getattr(pod, "status", None), "container_statuses", None) or []
        for status in statuses:
            restart_count = int(getattr(status, "restart_count", 0) or 0)
            waiting = getattr(getattr(status, "state", None), "waiting", None)
            terminated = getattr(getattr(status, "last_state", None), "terminated", None)
            reason = (getattr(waiting, "reason", "") or getattr(terminated, "reason", "") or "").lower()
            if "crashloopbackoff" in reason:
                events.append(self._emit("K8sPodCrashLoopDetected", pod, "ERROR", "Pod is in CrashLoopBackOff", metadata))
            if "imagepullbackoff" in reason or "errimagepull" in reason:
                events.append(self._emit("K8sImagePullFailed", pod, "ERROR", "Pod image pull failed", metadata))
            if "oomkilled" in reason:
                events.append(self._emit("K8sOOMKilled", pod, "ERROR", "Pod container was OOMKilled", metadata))
            if restart_count >= self.restart_threshold:
                details = dict(metadata, restart_count=restart_count)
                events.append(self._emit("K8sHighRestartCount", pod, "WARNING", "Pod restart count exceeded threshold", details))
        return events

    def _event_for_deployment(self, deployment: Any) -> ObservabilityEvent | None:
        status = getattr(deployment, "status", None)
        desired = int(getattr(status, "replicas", 0) or 0)
        available = int(getattr(status, "available_replicas", 0) or 0)
        if desired == 0 or desired > available:
            metadata = self._object_metadata(deployment)
            metadata.update({"desired_replicas": desired, "available_replicas": available})
            return self._emit("K8sDeploymentDegraded", deployment, "WARNING", "Deployment has unavailable replicas", metadata)
        return None

    def _event_for_job(self, job: Any) -> ObservabilityEvent | None:
        failed = int(getattr(getattr(job, "status", None), "failed", 0) or 0)
        if failed > 0:
            metadata = self._object_metadata(job)
            metadata["failed_pods"] = failed
            return self._emit("K8sJobFailed", job, "ERROR", "Kubernetes Job has failed pods", metadata)
        return None

    def _object_metadata(self, obj: Any) -> dict[str, Any]:
        meta = getattr(obj, "metadata", None)
        labels = dict(getattr(meta, "labels", None) or {})
        owner_refs = getattr(meta, "owner_references", None) or []
        owner = owner_refs[0] if owner_refs else None
        owner_kind = getattr(owner, "kind", "") if owner else ""
        owner_name = getattr(owner, "name", "") if owner else ""
        deployment = labels.get("app") or labels.get("app.kubernetes.io/name") or ""
        if owner_kind == "ReplicaSet" and owner_name:
            deployment = "-".join(owner_name.split("-")[:-1]) or owner_name
        return {
            "namespace": getattr(meta, "namespace", self.namespace),
            "name": getattr(meta, "name", "unknown"),
            "uid": getattr(meta, "uid", ""),
            "labels": labels,
            "owner_kind": owner_kind,
            "owner_name": owner_name,
            "deployment": deployment,
        }

    def _emit(
        self,
        event_type: str,
        obj: Any,
        severity: str,
        message: str,
        metadata: dict[str, Any],
    ) -> ObservabilityEvent:
        name = metadata.get("name", "unknown")
        return self.event_bus.emit(
            run_id=f"k8s-{name}",
            pipeline_name=self.pipeline_name,
            event_type=event_type,
            severity=severity,
            component="k8s_observer",
            message=message,
            trace_id=new_trace_id(),
            metadata=metadata,
        )
