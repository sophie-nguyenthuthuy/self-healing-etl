from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from config import AutonomousConfig, ETLConfig, HealingConfig, QuarantineConfig, SchemaRegistryConfig


@dataclass(frozen=True)
class TaxiWarehouseTables:
    raw: str = "raw_taxi_trip"
    fact: str = "fact_taxi_trip"
    summary: str = "trip_summary"
    dim_date: str = "dim_date"
    dim_payment: str = "dim_payment"


@dataclass(frozen=True)
class TaxiSchemaConfig:
    source_name: str = "nyc_taxi_trips"
    pipeline_name: str = "taxi_realtime_pipeline"
    table_names: TaxiWarehouseTables = field(default_factory=TaxiWarehouseTables)
    canonical_columns: tuple[str, ...] = (
        "trip_id",
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "payment_type",
    )
    column_aliases: dict[str, str] = field(default_factory=lambda: {
        "VendorID": "vendor_id",
        "vendor_id": "vendor_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "dropoff_datetime": "dropoff_datetime",
    })
    numeric_columns: tuple[str, ...] = (
        "trip_id",
        "vendor_id",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "tip_amount",
    )
    required_positive_columns: tuple[str, ...] = ("trip_distance", "fare_amount")
    nullable_defaults: dict[str, object] = field(default_factory=lambda: {
        "passenger_count": 0,
        "tip_amount": 0,
        "payment_type": "Unknown",
    })


@dataclass(frozen=True)
class TaxiSyntheticConfig:
    seed: int = 42
    start_time: datetime = datetime(2025, 1, 1, 8, 0, 0)
    payment_types: tuple[str, ...] = ("Credit Card", "Cash", "Mobile")
    vendor_ids: tuple[int, ...] = (1, 2)
    min_duration_minutes: int = 5
    max_duration_minutes: int = 40
    min_distance: float = 0.6
    max_distance: float = 12.0
    base_fare: float = 3.0
    min_fare_per_mile: float = 2.2
    max_fare_per_mile: float = 4.1
    tip_rates: tuple[float, ...] = (0, 0.12, 0.18, 0.22)


@dataclass(frozen=True)
class DriftMutation:
    column: str
    value: object


@dataclass(frozen=True)
class TaxiDriftConfig:
    enabled: bool = True
    start_after_record: int | None = None
    start_fraction: float = 0.5
    currency_columns: tuple[str, ...] = ("fare_amount",)
    added_columns: tuple[DriftMutation, ...] = (
        DriftMutation("congestion_surcharge", 2.5),
    )

    def starts_after(self, total_records: int) -> int:
        if self.start_after_record is not None:
            return self.start_after_record
        return max(1, int(total_records * self.start_fraction))


@dataclass(frozen=True)
class TaxiRuntimeConfig:
    schema: TaxiSchemaConfig = field(default_factory=TaxiSchemaConfig)
    synthetic: TaxiSyntheticConfig = field(default_factory=TaxiSyntheticConfig)
    drift: TaxiDriftConfig = field(default_factory=TaxiDriftConfig)


@dataclass(frozen=True)
class TaxiETLPaths:
    root: Path = Path("data/taxi")

    @property
    def raw_dir(self) -> Path:
        return self.root / "raw"

    @property
    def incoming_dir(self) -> Path:
        return self.root / "incoming"

    @property
    def processed_dir(self) -> Path:
        return self.root / "processed"

    @property
    def failed_dir(self) -> Path:
        return self.root / "failed"

    @property
    def logs_dir(self) -> Path:
        return self.root / "logs"

    @property
    def warehouse_url(self) -> str:
        return f"sqlite:///{(self.root / 'taxi_warehouse.db').as_posix()}"

    @property
    def schema_registry_url(self) -> str:
        return f"sqlite:///{(self.root / 'taxi_schema_registry.db').as_posix()}"

    @property
    def quarantine_url(self) -> str:
        return f"sqlite:///{(self.root / 'taxi_quarantine.db').as_posix()}"

    def ensure(self) -> None:
        for path in (
            self.raw_dir,
            self.incoming_dir,
            self.processed_dir,
            self.failed_dir,
            self.logs_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)


def taxi_etl_config(
    paths: TaxiETLPaths,
    runtime: TaxiRuntimeConfig | None = None,
    autonomous_mode: bool = False,
    require_human_approval: bool = False,
) -> ETLConfig:
    runtime = runtime or TaxiRuntimeConfig()
    return ETLConfig(
        pipeline_name=runtime.schema.pipeline_name,
        batch_size=250,
        schema_registry=SchemaRegistryConfig(db_url=paths.schema_registry_url),
        quarantine=QuarantineConfig(db_url=paths.quarantine_url),
        healing=HealingConfig(
            enable_type_coercion=True,
            enable_column_backfill=True,
            enable_schema_evolution=True,
            max_coercion_loss_pct=20.0,
        ),
        autonomous=AutonomousConfig(
            enable_autonomous_healing=autonomous_mode,
            require_human_approval=require_human_approval,
        ),
    )
