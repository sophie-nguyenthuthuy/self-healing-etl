from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from agents.approvals import ApprovalStore
from knowledge.healing_history import HealingHistory
from observability.event_bus import EventBus


@dataclass(frozen=True)
class K8sHealingResult:
    action_type: str
    success: bool
    before_state: dict[str, Any] = field(default_factory=dict)
    after_state: dict[str, Any] = field(default_factory=dict)
    message: str = ""
    approval_id: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "action_type": self.action_type,
            "success": self.success,
            "before_state": self.before_state,
            "after_state": self.after_state,
            "message": self.message,
            "approval_id": self.approval_id,
        }


class K8sHealingEngine:
    SAFE_ACTIONS = {
        "restart_deployment",
        "scale_deployment",
        "retry_job",
        "rollback_deployment",
        "refresh_configmap",
        "inspect_image",
        "inspect_pod_logs",
        "validate_registry",
        "inspect_node_resources",
        "collect_more_telemetry",
    }
    APPROVAL_REQUIRED_ACTIONS = {"increase_memory_limit", "refresh_secret"}
    FORBIDDEN_ACTIONS = {"cluster_deletion", "namespace_deletion", "node_termination", "privilege_escalation"}

    def __init__(
        self,
        db_url: str,
        *,
        namespace: str = "default",
        apps_v1: Any | None = None,
        core_v1: Any | None = None,
        batch_v1: Any | None = None,
        event_bus: EventBus | None = None,
    ):
        self.namespace = namespace
        self.apps_v1 = apps_v1
        self.core_v1 = core_v1
        self.batch_v1 = batch_v1
        self.history = HealingHistory(db_url)
        self.approvals = ApprovalStore(db_url)
        self.event_bus = event_bus or EventBus(db_url)

    def execute(
        self,
        action_type: str,
        *,
        run_id: str,
        trace_id: str,
        root_cause: str,
        parameters: dict[str, Any] | None = None,
    ) -> K8sHealingResult:
        parameters = parameters or {}
        context = parameters.get("context", {})
        if isinstance(context, dict):
            parameters = {**context, **parameters}
        parameters.setdefault("replicas", 1)
        before = dict(parameters)
        action_type = action_type.lower()
        if action_type in self.FORBIDDEN_ACTIONS:
            result = K8sHealingResult(action_type, False, before, {"blocked": True}, "Forbidden Kubernetes action blocked.")
        elif action_type in self.APPROVAL_REQUIRED_ACTIONS:
            approval_id = self.approvals.request(
                run_id=run_id,
                trace_id=trace_id,
                plan_json=json.dumps({"action_type": action_type, "parameters": parameters}, default=str),
            )
            result = K8sHealingResult(
                action_type,
                False,
                before,
                {"approval_required": True, "approval_id": approval_id},
                "Kubernetes action requires human approval.",
                approval_id,
            )
        elif action_type in self.SAFE_ACTIONS:
            result = self._execute_safe(action_type, before)
        else:
            result = K8sHealingResult(action_type, False, before, {"escalated": True}, "Unknown Kubernetes action escalated.")

        self.history.record_action(
            run_id=run_id,
            trace_id=trace_id,
            root_cause=root_cause,
            action_taken=json.dumps({"action_type": action_type, "parameters": parameters}, default=str),
            agent="K8sHealingEngine",
            before_state=result.before_state,
            after_state=result.after_state,
            success=result.success,
        )
        return result

    def _execute_safe(self, action_type: str, parameters: dict[str, Any]) -> K8sHealingResult:
        handlers = {
            "restart_deployment": self._restart_deployment,
            "scale_deployment": self._scale_deployment,
            "retry_job": self._retry_job,
            "rollback_deployment": self._rollback_deployment,
            "refresh_configmap": self._restart_deployment,
            "inspect_image": self._inspect_image,
            "inspect_pod_logs": self._inspect_pod_logs,
            "validate_registry": self._validate_registry,
            "inspect_node_resources": self._inspect_node_resources,
            "collect_more_telemetry": self._collect_more_telemetry,
        }
        try:
            after = handlers[action_type](parameters)
            return K8sHealingResult(action_type, True, parameters, after, f"Executed Kubernetes action: {action_type}")
        except Exception as exc:
            return K8sHealingResult(action_type, False, parameters, {"error": str(exc)}, f"Kubernetes action failed: {exc}")

    def _ensure_clients(self) -> None:
        if self.apps_v1 and self.core_v1 and self.batch_v1:
            return
        from kubernetes import client, config

        try:
            config.load_incluster_config()
        except Exception:
            config.load_kube_config()
        self.apps_v1 = client.AppsV1Api()
        self.core_v1 = client.CoreV1Api()
        self.batch_v1 = client.BatchV1Api()

    def _restart_deployment(self, parameters: dict[str, Any]) -> dict[str, Any]:
        self._ensure_clients()
        deployment = parameters["deployment"]
        namespace = parameters.get("namespace", self.namespace)
        body = {
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "self-healing-etl/restarted-at": datetime.utcnow().isoformat()
                        }
                    }
                }
            }
        }
        self.apps_v1.patch_namespaced_deployment(deployment, namespace, body)
        return {"deployment": deployment, "namespace": namespace, "rolling_restart": True}

    def _scale_deployment(self, parameters: dict[str, Any]) -> dict[str, Any]:
        self._ensure_clients()
        deployment = parameters["deployment"]
        namespace = parameters.get("namespace", self.namespace)
        replicas = int(parameters["replicas"])
        self.apps_v1.patch_namespaced_deployment_scale(deployment, namespace, {"spec": {"replicas": replicas}})
        return {"deployment": deployment, "namespace": namespace, "replicas": replicas}

    def _retry_job(self, parameters: dict[str, Any]) -> dict[str, Any]:
        self._ensure_clients()
        job = parameters["job"]
        namespace = parameters.get("namespace", self.namespace)
        existing = self.batch_v1.read_namespaced_job(job, namespace)
        self.batch_v1.delete_namespaced_job(job, namespace)
        existing.metadata.resource_version = None
        existing.metadata.uid = None
        self.batch_v1.create_namespaced_job(namespace, existing)
        return {"job": job, "namespace": namespace, "recreated": True}

    def _rollback_deployment(self, parameters: dict[str, Any]) -> dict[str, Any]:
        self._ensure_clients()
        deployment_name = parameters["deployment"]
        namespace = parameters.get("namespace", self.namespace)
        deployment = self.apps_v1.read_namespaced_deployment(deployment_name, namespace)
        annotations = getattr(getattr(deployment.spec.template, "metadata", None), "annotations", None) or {}
        previous_image = parameters.get("previous_image") or annotations.get("self-healing-etl/previous-image")
        if not previous_image:
            return self._restart_deployment(parameters) | {"rollback_requested": True, "previous_image_found": False}
        containers = getattr(deployment.spec.template.spec, "containers", [])
        previous_command = self._annotation_list(annotations.get("self-healing-etl/previous-command"))
        previous_args = self._annotation_list(annotations.get("self-healing-etl/previous-args"))
        patched_containers = []
        for container in containers:
            patch = {"name": container.name, "image": previous_image}
            if previous_command is not None:
                patch["command"] = previous_command or None
            if previous_args is not None:
                patch["args"] = previous_args or None
            patched_containers.append(patch)
        body = {
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "self-healing-etl/rolled-back-at": datetime.utcnow().isoformat()
                        }
                    },
                    "spec": {
                        "containers": patched_containers
                    },
                }
            }
        }
        self.apps_v1.patch_namespaced_deployment(deployment_name, namespace, body)
        return {"deployment": deployment_name, "namespace": namespace, "rollback_requested": True, "image": previous_image}

    def _inspect_image(self, parameters: dict[str, Any]) -> dict[str, Any]:
        image = parameters.get("image", "")
        deployment = parameters.get("deployment")
        namespace = parameters.get("namespace", self.namespace)
        if deployment and not image:
            self._ensure_clients()
            workload = self.apps_v1.read_namespaced_deployment(deployment, namespace)
            containers = getattr(workload.spec.template.spec, "containers", [])
            image = containers[0].image if containers else ""
        return {"image": image, "deployment": deployment, "inspection": "metadata captured"}

    def _inspect_pod_logs(self, parameters: dict[str, Any]) -> dict[str, Any]:
        self._ensure_clients()
        pod = parameters["pod"]
        namespace = parameters.get("namespace", self.namespace)
        logs = self.core_v1.read_namespaced_pod_log(pod, namespace, tail_lines=parameters.get("tail_lines", 200))
        self.event_bus.emit(
            run_id=parameters.get("run_id", f"k8s-{pod}"),
            pipeline_name=parameters.get("pipeline_name", "k8s_ai_sre"),
            event_type="K8sDiagnosticLog",
            severity="INFO",
            component="k8s_healing",
            message=f"Collected logs for pod {pod}",
            trace_id=parameters.get("trace_id", "k8s-diagnostic"),
            metadata={"pod": pod, "namespace": namespace, "logs": logs[-4000:]},
        )
        return {"pod": pod, "namespace": namespace, "logs_collected": True}

    def _validate_registry(self, parameters: dict[str, Any]) -> dict[str, Any]:
        image = parameters.get("image", "")
        registry = parameters.get("registry_url") or self._registry_url(image)
        if not registry:
            return {"image": image, "registry_reachable": False, "reason": "No registry URL inferred"}
        request = urllib.request.Request(registry, method="HEAD")
        with urllib.request.urlopen(request, timeout=5) as response:
            return {"registry_url": registry, "status": response.status, "registry_reachable": response.status < 500}

    def _inspect_node_resources(self, parameters: dict[str, Any]) -> dict[str, Any]:
        self._ensure_clients()
        nodes = self.core_v1.list_node().items
        return {
            "nodes": [
                {
                    "name": node.metadata.name,
                    "capacity": getattr(node.status, "capacity", {}),
                    "allocatable": getattr(node.status, "allocatable", {}),
                }
                for node in nodes
            ]
        }

    def _collect_more_telemetry(self, parameters: dict[str, Any]) -> dict[str, Any]:
        return {"telemetry_requested": True, "parameters": parameters}

    def _registry_url(self, image: str) -> str:
        if not image or "/" not in image:
            return ""
        registry = image.split("/", 1)[0]
        if "." not in registry and ":" not in registry:
            return ""
        return f"https://{registry}/v2/"

    def _annotation_list(self, value: str | None) -> list[str] | None:
        if value is None:
            return None
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
        return None
