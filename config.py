from __future__ import annotations

import os
from pathlib import Path
from pydantic import BaseModel, Field


class AlertConfig(BaseModel):
    slack_webhook_url: str | None = Field(default=None)
    email_to: list[str] = Field(default_factory=list)
    min_severity: str = "WARNING"  # DEBUG | INFO | WARNING | ERROR | CRITICAL


class QuarantineConfig(BaseModel):
    db_url: str = "sqlite:///quarantine.db"
    max_records_per_batch: int = 10_000
    auto_purge_days: int = 30


class HealingConfig(BaseModel):
    enable_type_coercion: bool = True
    enable_column_backfill: bool = True
    enable_schema_evolution: bool = True   # auto-register new schema version on drift
    max_coercion_loss_pct: float = 5.0     # quarantine batch if >5% of rows need coercion


class AutonomousConfig(BaseModel):
    enable_autonomous_healing: bool = False
    require_human_approval: bool = False
    max_healing_attempts: int = 3
    ollama_enabled: bool = True
    ollama_url: str = "http://localhost:11434"
    ollama_model: str = "llama3.2:latest"
    ollama_fallback_model: str = "llama3.2:latest"


class SchemaRegistryConfig(BaseModel):
    db_url: str = "sqlite:///schema_registry.db"
    strict_mode: bool = False  # True = reject any schema drift without healing


class ETLConfig(BaseModel):
    pipeline_name: str = "etl_pipeline"
    batch_size: int = 1_000
    max_retries: int = 3
    retry_delay_seconds: int = 30
    schema_registry: SchemaRegistryConfig = Field(default_factory=SchemaRegistryConfig)
    quarantine: QuarantineConfig = Field(default_factory=QuarantineConfig)
    healing: HealingConfig = Field(default_factory=HealingConfig)
    autonomous: AutonomousConfig = Field(default_factory=AutonomousConfig)
    alerts: AlertConfig = Field(default_factory=AlertConfig)
    data_dir: Path = Field(default=Path("./data"))

    @classmethod
    def from_env(cls) -> "ETLConfig":
        return cls(
            pipeline_name=os.getenv("ETL_PIPELINE_NAME", "etl_pipeline"),
            schema_registry=SchemaRegistryConfig(
                db_url=os.getenv("SCHEMA_REGISTRY_DB", "sqlite:///schema_registry.db"),
                strict_mode=os.getenv("SCHEMA_STRICT_MODE", "false").lower() == "true",
            ),
            quarantine=QuarantineConfig(
                db_url=os.getenv("QUARANTINE_DB", "sqlite:///quarantine.db"),
            ),
            alerts=AlertConfig(
                slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL"),
                email_to=list(filter(None, os.getenv("ALERT_EMAIL_TO", "").split(","))),
            ),
            autonomous=AutonomousConfig(
                enable_autonomous_healing=os.getenv("ENABLE_AUTONOMOUS_HEALING", "false").lower() == "true",
                require_human_approval=os.getenv("REQUIRE_HEALING_APPROVAL", "false").lower() == "true",
                ollama_enabled=os.getenv("OLLAMA_ENABLED", "true").lower() == "true",
                ollama_url=os.getenv("OLLAMA_URL", "http://localhost:11434"),
                ollama_model=os.getenv("OLLAMA_MODEL", "llama3.2:latest"),
                ollama_fallback_model=os.getenv("OLLAMA_FALLBACK_MODEL", "llama3.2:latest"),
            ),
        )
