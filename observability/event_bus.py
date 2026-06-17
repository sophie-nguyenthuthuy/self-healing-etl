from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

from sqlalchemy import select
from sqlalchemy.orm import Session

from models import PipelineEvent, init_db
from observability.traces import new_span_id


@dataclass(frozen=True)
class ObservabilityEvent:
    run_id: str
    pipeline_name: str
    event_type: str
    severity: str
    component: str
    message: str
    trace_id: str
    span_id: str
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime | None = None


class EventBus:
    """Small synchronous event bus that persists every event before notifying subscribers."""

    def __init__(self, db_url: str):
        self.engine = init_db(db_url)
        self._subscribers: list[Callable[[ObservabilityEvent], None]] = []

    def subscribe(self, handler: Callable[[ObservabilityEvent], None]) -> None:
        self._subscribers.append(handler)

    def emit(
        self,
        *,
        run_id: str,
        pipeline_name: str,
        event_type: str,
        severity: str = "INFO",
        component: str,
        message: str,
        trace_id: str,
        span_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ObservabilityEvent:
        event = ObservabilityEvent(
            run_id=run_id,
            pipeline_name=pipeline_name,
            event_type=event_type,
            severity=severity.upper(),
            component=component,
            message=message,
            metadata=metadata or {},
            trace_id=trace_id,
            span_id=span_id or new_span_id(component),
            timestamp=datetime.utcnow(),
        )
        with Session(self.engine) as session:
            session.add(
                PipelineEvent(
                    timestamp=event.timestamp,
                    run_id=event.run_id,
                    pipeline_name=event.pipeline_name,
                    event_type=event.event_type,
                    severity=event.severity,
                    component=event.component,
                    message=event.message,
                    metadata_json=json.dumps(event.metadata, default=str),
                    trace_id=event.trace_id,
                    span_id=event.span_id,
                )
            )
            session.commit()

        for handler in list(self._subscribers):
            handler(event)
        return event

    def recent_events(
        self,
        *,
        run_id: str | None = None,
        trace_id: str | None = None,
        limit: int = 100,
    ) -> list[PipelineEvent]:
        with Session(self.engine) as session:
            query = select(PipelineEvent).order_by(PipelineEvent.timestamp.desc()).limit(limit)
            if run_id:
                query = query.where(PipelineEvent.run_id == run_id)
            if trace_id:
                query = query.where(PipelineEvent.trace_id == trace_id)
            return list(session.scalars(query).all())

