from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from models import PipelineEvent, PipelineRun, QuarantineRecord, init_db


@dataclass(frozen=True)
class TelemetrySnapshot:
    active_runs: int
    failed_runs: int
    recovered_runs: int
    unresolved_quarantine: int
    error_events: int

    def as_dict(self) -> dict[str, int]:
        return {
            "active_runs": self.active_runs,
            "failed_runs": self.failed_runs,
            "recovered_runs": self.recovered_runs,
            "unresolved_quarantine": self.unresolved_quarantine,
            "error_events": self.error_events,
        }


class TelemetryReader:
    def __init__(self, db_url: str):
        self.engine = init_db(db_url)

    def snapshot(self, pipeline_name: str | None = None) -> TelemetrySnapshot:
        with Session(self.engine) as session:
            runs = list(session.scalars(select(PipelineRun)).all())
            events = list(session.scalars(select(PipelineEvent)).all())
            quarantine = list(session.scalars(select(QuarantineRecord)).all())

        if pipeline_name:
            runs = [run for run in runs if run.pipeline_name == pipeline_name]
            events = [event for event in events if event.pipeline_name == pipeline_name]
            quarantine = [record for record in quarantine if record.pipeline_name == pipeline_name]

        return TelemetrySnapshot(
            active_runs=sum(1 for run in runs if run.status == "RUNNING"),
            failed_runs=sum(1 for run in runs if run.status == "FAILED"),
            recovered_runs=sum(1 for event in events if event.event_type == "PipelineRecovered"),
            unresolved_quarantine=sum(1 for record in quarantine if not record.resolved),
            error_events=sum(1 for event in events if event.severity == "ERROR"),
        )

