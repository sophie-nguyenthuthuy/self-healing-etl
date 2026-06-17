# Documentation Index

## Core Documents

| Document | Purpose |
|---|---|
| [README.md](../README.md) | Project overview, setup, CLI usage, and taxi demo quick start. |
| [HLD.md](HLD.md) | High-level architecture, design decisions, runtime modes, and system flow. |
| [LLD.md](LLD.md) | Low-level component design, interfaces, execution flow, and extension points. |
| [DATA_MODEL.md](DATA_MODEL.md) | Metadata tables, taxi warehouse model, star schema, and metric formulas. |
| [AGENTIC_SELF_HEALING.md](AGENTIC_SELF_HEALING.md) | Autonomous observability, RCA, planning, healing, validation, and audit flow. |
| [K8S_AI_SRE.md](K8S_AI_SRE.md) | Kubernetes AI-SRE extension, Docker Desktop demo flow, event types, and bounded healing actions. |
| [AUTO_HEALING_SCENARIOS.md](AUTO_HEALING_SCENARIOS.md) | Scenario catalog for drift, healing, quarantine, join failures, and hard-failure drills. |
| [OPERATIONS.md](OPERATIONS.md) | Commands, validation checklist, failure triage, quarantine review, and production hardening. |
| [ETL_FAILURE_MANUAL_RERUN_RUNBOOK.md](ETL_FAILURE_MANUAL_RERUN_RUNBOOK.md) | Manual rerun failure scenarios across ingestion, transformation, loading, resources, and orchestration. |

## Recommended Reading Order

1. Start with [README.md](../README.md) for project usage.
2. Read [HLD.md](HLD.md) for architecture and design intent.
3. Read [LLD.md](LLD.md) for implementation-level behavior.
4. Read [AGENTIC_SELF_HEALING.md](AGENTIC_SELF_HEALING.md) for autonomous mode.
5. Read [K8S_AI_SRE.md](K8S_AI_SRE.md) when running the local Docker Kubernetes demo.
6. Use [AUTO_HEALING_SCENARIOS.md](AUTO_HEALING_SCENARIOS.md) when demonstrating ETL failure coverage.
7. Use [OPERATIONS.md](OPERATIONS.md) while running or troubleshooting the project.
8. Use [DATA_MODEL.md](DATA_MODEL.md) when explaining persistence, warehouse, MTTD, and MTTR.

## Demo Narrative

For presentations or interviews:

1. Show the high-level architecture in `HLD.md`.
2. Run `python -B main.py --failure-scenarios`.
3. Explain how each scenario proves a real ETL failure mode.
4. Run `python -B main.py --taxi-demo --taxi-records 30`.
5. Show `DATA_MODEL.md` to connect the taxi pipeline to warehouse analytics.
6. For the local Kubernetes story, run `powershell -ExecutionPolicy Bypass -File .\deploy_local_demo.ps1`.
7. Run `python demo_k8s.py --scenario image_pull` to show live AI-SRE detection and rollback.
8. Use `OPERATIONS.md` to explain how manual review and rerun would work in production.
