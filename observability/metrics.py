from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from models import HealingAction, PipelineEvent, PipelineRun, init_db


class ObservabilityMetrics:
    def __init__(self, db_url: str):
        self.engine = init_db(db_url)

    def health(self, pipeline_name: str | None = None) -> dict[str, int]:
        with Session(self.engine) as session:
            runs = list(session.scalars(select(PipelineRun)).all())
            events = list(session.scalars(select(PipelineEvent)).all())
            actions = list(session.scalars(select(HealingAction)).all())

        if pipeline_name:
            runs = [run for run in runs if run.pipeline_name == pipeline_name]
            events = [event for event in events if event.pipeline_name == pipeline_name]

        return {
            "active_runs": sum(1 for run in runs if run.status == "RUNNING"),
            "failed_runs": sum(1 for run in runs if run.status == "FAILED"),
            "successful_runs": sum(1 for run in runs if run.status == "SUCCESS"),
            "recovered_runs": sum(1 for event in events if event.event_type == "PipelineRecovered"),
            "healing_actions": len(actions),
            "successful_healing_actions": sum(1 for action in actions if action.success),
        }

