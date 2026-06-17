from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher

from sqlalchemy import select
from sqlalchemy.orm import Session

from models import IncidentHistory, init_db


@dataclass(frozen=True)
class IncidentMatch:
    root_cause: str
    healing_action: str
    confidence: float


class IncidentStore:
    def __init__(self, db_url: str):
        self.engine = init_db(db_url)

    def signature(self, text: str) -> str:
        return " ".join(text.lower().strip().split())[:512]

    def find_similar(self, failure_text: str, threshold: float = 0.86) -> IncidentMatch | None:
        signature = self.signature(failure_text)
        with Session(self.engine) as session:
            incidents = list(
                session.scalars(
                    select(IncidentHistory).where(IncidentHistory.successful == True)
                ).all()
            )
        best: tuple[float, IncidentHistory] | None = None
        for incident in incidents:
            score = SequenceMatcher(None, signature, incident.failure_signature).ratio()
            if score >= threshold and (best is None or score > best[0]):
                best = (score, incident)
        if best is None:
            return None
        score, incident = best
        return IncidentMatch(
            root_cause=incident.root_cause,
            healing_action=incident.healing_action or "",
            confidence=score,
        )

    def record(
        self,
        *,
        failure_text: str,
        root_cause: str,
        healing_action: str,
        successful: bool,
        source_domain: str = "ETL",
    ) -> None:
        with Session(self.engine) as session:
            session.add(
                IncidentHistory(
                    failure_signature=self.signature(failure_text),
                    root_cause=root_cause,
                    healing_action=healing_action,
                    source_domain=source_domain,
                    successful=successful,
                )
            )
            session.commit()
