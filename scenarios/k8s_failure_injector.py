from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any


@dataclass(frozen=True)
class InjectionResult:
    scenario: str
    namespace: str
    deployment: str
    message: str


class K8sFailureInjector:
    """Injects reversible Kubernetes failures for the local demo cluster."""

    def __init__(
        self,
        *,
        namespace: str = "self-healing-etl",
        deployment: str = "self-healing-etl",
        apps_v1: Any | None = None,
    ):
        self.namespace = namespace
        self.deployment = deployment
        self.apps_v1 = apps_v1

    def inject(self, scenario: str) -> InjectionResult:
        scenario = scenario.replace("-", "_").lower()
        if scenario == "crash_loop":
            return self.inject_crash_loop()
        if scenario == "image_pull":
            return self.inject_image_pull_backoff()
        if scenario == "oom":
            return self.inject_oom_killed()
        if scenario == "scale_zero":
            return self.inject_scale_zero()
        raise ValueError(f"Unsupported K8s failure scenario: {scenario}")

    def inject_crash_loop(self) -> InjectionResult:
        body = self._image_patch(
            image="busybox:1.36",
            command=["/bin/sh", "-c"],
            args=["echo injected crash loop; exit 1"],
        )
        self._apps().patch_namespaced_deployment(self.deployment, self.namespace, body)
        return InjectionResult("crash_loop", self.namespace, self.deployment, "Patched deployment to exit immediately.")

    def inject_image_pull_backoff(self) -> InjectionResult:
        body = self._image_patch(image="registry.local/does-not-exist:latest")
        self._apps().patch_namespaced_deployment(self.deployment, self.namespace, body)
        return InjectionResult("image_pull", self.namespace, self.deployment, "Patched deployment to an invalid image.")

    def inject_oom_killed(self) -> InjectionResult:
        body = {
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": "etl",
                                "resources": {
                                    "limits": {"memory": "1Mi"},
                                    "requests": {"memory": "1Mi"},
                                },
                            }
                        ]
                    }
                }
            }
        }
        self._apps().patch_namespaced_deployment(self.deployment, self.namespace, body)
        return InjectionResult("oom", self.namespace, self.deployment, "Patched deployment memory limit to 1Mi.")

    def inject_scale_zero(self) -> InjectionResult:
        self._apps().patch_namespaced_deployment_scale(self.deployment, self.namespace, {"spec": {"replicas": 0}})
        return InjectionResult("scale_zero", self.namespace, self.deployment, "Scaled deployment to zero replicas.")

    def _image_patch(
        self,
        *,
        image: str,
        command: list[str] | None = None,
        args: list[str] | None = None,
    ) -> dict[str, Any]:
        deployment = self._apps().read_namespaced_deployment(self.deployment, self.namespace)
        container = deployment.spec.template.spec.containers[0]
        annotations = dict(getattr(deployment.spec.template.metadata, "annotations", None) or {})
        annotations.setdefault("self-healing-etl/previous-image", container.image)
        annotations.setdefault("self-healing-etl/previous-command", json.dumps(container.command or []))
        annotations.setdefault("self-healing-etl/previous-args", json.dumps(container.args or []))
        patched_container: dict[str, Any] = {"name": container.name, "image": image}
        if command is not None:
            patched_container["command"] = command
        if args is not None:
            patched_container["args"] = args
        return {
            "spec": {
                "template": {
                    "metadata": {"annotations": annotations},
                    "spec": {"containers": [patched_container]},
                }
            }
        }

    def _apps(self):
        if self.apps_v1:
            return self.apps_v1
        from kubernetes import client, config

        config.load_kube_config()
        self.apps_v1 = client.AppsV1Api()
        return self.apps_v1
