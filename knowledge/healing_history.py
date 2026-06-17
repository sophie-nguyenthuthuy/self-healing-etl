from __future__ import annotations

import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from models import HealingAction, init_db


class HealingHistory:
    def __init__(self, db_url: str):
        self.engine = init_db(db_url)

    def record_action(
        self,
        *,
        run_id: str,
        trace_id: str,
        root_cause: str,
        action_taken: str,
        agent: str,
        before_state: dict[str, Any] | None,
        after_state: dict[str, Any] | None,
        success: bool,
    ) -> None:
        with Session(self.engine) as session:
            session.add(
                HealingAction(
                    run_id=run_id,
                    trace_id=trace_id,
                    root_cause=root_cause,
                    action_taken=action_taken,
                    agent=agent,
                    before_state=json.dumps(before_state or {}, default=str),
                    after_state=json.dumps(after_state or {}, default=str),
                    success=success,
                )
            )
            session.commit()

    def recent(self, limit: int = 100) -> list[HealingAction]:
        with Session(self.engine) as session:
            return list(
                session.scalars(
                    select(HealingAction).order_by(HealingAction.timestamp.desc()).limit(limit)
                ).all()
            )

