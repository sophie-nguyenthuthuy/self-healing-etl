from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.table import Table

from scenarios.failure_injector import FailureInjectionResult, FailureInjector

K8S_SCENARIOS = {"crash_loop", "image_pull", "oom", "scale_zero"}
ETL_SCENARIOS = {"api_rate_limit", "staging_data_quality", "timeout", "concurrent_modification"}

console = Console()


def run(args: list[str], *, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(args, check=check, text=True, capture_output=True)


def kubectl(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return run(["kubectl", *args], check=check)


def load_kube_client():
    try:
        from kubernetes import client, config
    except Exception as exc:
        raise RuntimeError("Install dependencies first: pip install -r requirements.txt") from exc
    config.load_kube_config()
    return client


def deployment_ready(namespace: str, deployment: str) -> bool:
    result = kubectl(
        "get",
        "deployment",
        deployment,
        "-n",
        namespace,
        "-o",
        "json",
        check=False,
    )
    if result.returncode != 0:
        return False
    payload = json.loads(result.stdout)
    metadata = payload.get("metadata", {})
    spec = payload.get("spec", {})
    status = payload.get("status", {})
    desired = int(spec.get("replicas") or 0)
    return (
        desired > 0
        and int(status.get("observedGeneration") or 0) >= int(metadata.get("generation") or 0)
        and int(status.get("updatedReplicas") or 0) == desired
        and int(status.get("readyReplicas") or 0) == desired
        and int(status.get("availableReplicas") or 0) == desired
        and int(status.get("unavailableReplicas") or 0) == 0
    )


def pod_status_table(namespace: str) -> Table:
    result = kubectl("get", "pods", "-n", namespace, "--no-headers", check=False)
    table = Table(title=f"Pods in {namespace}")
    table.add_column("Name")
    table.add_column("Ready")
    table.add_column("Status")
    table.add_column("Restarts")
    table.add_column("Age")
    if result.returncode != 0:
        table.add_row("kubectl error", "", result.stderr.strip(), "", "")
        return table
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 5:
            table.add_row(parts[0], parts[1], parts[2], parts[3], parts[4])
    return table


def print_recent_ai_sre_logs(namespace: str) -> None:
    result = kubectl(
        "logs",
        "deployment/ai-sre",
        "-n",
        namespace,
        "--tail=40",
        check=False,
    )
    if result.stdout.strip():
        console.rule("AI-SRE Logs")
        console.print(result.stdout.strip())


def wait_for_recovery(namespace: str, deployment: str, timeout_seconds: int) -> float | None:
    started = time.monotonic()
    while time.monotonic() - started < timeout_seconds:
        console.clear()
        console.print(pod_status_table(namespace))
        print_recent_ai_sre_logs(namespace)
        if deployment_ready(namespace, deployment):
            return time.monotonic() - started
        time.sleep(5)
    return None


def print_etl_result(result: FailureInjectionResult) -> None:
    table = Table(title=f"{result.scenario} result")
    table.add_column("Field")
    table.add_column("Value")
    table.add_row("Domain", result.domain)
    table.add_row("Status", result.status)
    table.add_row("Message", result.message)
    table.add_row("MTTR", f"{result.mttr_seconds:.2f}s")
    table.add_row("Observability DB", result.db_url)
    for key, value in result.evidence.items():
        table.add_row(str(key), str(value))
    console.print(table)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a local K8s or ETL self-healing demo.")
    parser.add_argument(
        "--scenario",
        choices=sorted(K8S_SCENARIOS | ETL_SCENARIOS),
        default="image_pull",
    )
    parser.add_argument("--namespace", default="self-healing-etl")
    parser.add_argument("--deployment", default="self-healing-etl")
    parser.add_argument("--timeout-seconds", type=int, default=240)
    parser.add_argument("--observability-db", default=None)
    args = parser.parse_args()

    console.print("[bold]Self-Healing demo[/bold]")
    if args.scenario in ETL_SCENARIOS:
        injector = FailureInjector(db_url=args.observability_db)
        result = injector.inject(args.scenario)
        print_etl_result(result)
        return 0 if result.status in {"RECOVERED", "SUCCESS"} else 2

    cluster = kubectl("cluster-info", check=False)
    if cluster.returncode != 0:
        console.print("[red]kubectl cannot reach a cluster. Start Docker Desktop Kubernetes, Minikube, or Kind first.[/red]")
        console.print(cluster.stderr.strip())
        return 1

    client = load_kube_client()
    injector = FailureInjector(
        db_url=args.observability_db,
        namespace=args.namespace,
        deployment=args.deployment,
        apps_v1=client.AppsV1Api(),
    )

    console.print(pod_status_table(args.namespace))
    console.rule("Injecting Failure")
    injected_at = datetime.utcnow()
    result = injector.inject(args.scenario)
    console.print(f"[yellow]{result.message}[/yellow]")
    console.print(f"Injected at: {injected_at.isoformat()}Z")

    mttr = wait_for_recovery(args.namespace, args.deployment, args.timeout_seconds)
    console.rule("Summary")
    if mttr is None:
        console.print(f"[red]Deployment did not recover within {args.timeout_seconds} seconds.[/red]")
        console.print("Inspect with: kubectl describe pods -n self-healing-etl")
        return 2
    console.print(f"[green]Deployment recovered.[/green] Approximate MTTR: {mttr:.1f}s")
    console.print(f"Run dashboard: streamlit run {Path('taxi_etl/dashboard/app.py')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
