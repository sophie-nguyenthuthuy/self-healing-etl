from __future__ import annotations

from sqlalchemy.orm import Session

from models import HumanApproval, init_db


class ApprovalStore:
    def __init__(self, db_url: str):
        self.engine = init_db(db_url)

    def request(self, *, run_id: str, trace_id: str, plan_json: str) -> int:
        with Session(self.engine) as session:
            approval = HumanApproval(
                run_id=run_id,
                trace_id=trace_id,
                plan_json=plan_json,
                status="PENDING",
            )
            session.add(approval)
            session.commit()
            session.refresh(approval)
            return approval.id

