from __future__ import annotations

import tempfile
import unittest
import uuid
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from models import HealingAction, QuarantineRecord, init_db
from scenarios.failure_injector import FailureInjector


class FailureInjectorTests(unittest.TestCase):
    def setUp(self) -> None:
        path = Path(tempfile.gettempdir()) / f"failure_injector_{uuid.uuid4().hex}.db"
        self.db_url = f"sqlite:///{path.as_posix()}"

    def test_api_rate_limit_uses_backoff_and_retry(self):
        result = FailureInjector(db_url=self.db_url).inject("api_rate_limit")
        self.assertEqual(result.status, "RECOVERED")
        self.assertIn("apply_exponential_backoff", result.evidence["actions"])
        self.assertIn("retry_extraction", result.evidence["actions"])

    def test_timeout_uses_idempotent_staging_retry(self):
        result = FailureInjector(db_url=self.db_url).inject("timeout")
        self.assertEqual(result.status, "RECOVERED")
        self.assertIn("retry_staging_task", result.evidence["actions"])

    def test_concurrent_modification_rolls_back_and_retries(self):
        result = FailureInjector(db_url=self.db_url).inject("concurrent_modification")
        self.assertEqual(result.status, "RECOVERED")
        self.assertIn("rollback_transaction", result.evidence["actions"])
        self.assertIn("retry_staging_task", result.evidence["actions"])

    def test_staging_data_quality_quarantines_bad_rows(self):
        result = FailureInjector(db_url=self.db_url).inject("staging_data_quality")
        self.assertEqual(result.status, "RECOVERED")
        self.assertEqual(result.evidence["rows_quarantined"], 3)
        self.assertEqual(result.evidence["rows_loaded"], 1)
        db = init_db(self.db_url)
        with Session(db) as session:
            quarantined = list(session.scalars(select(QuarantineRecord)).all())
            actions = list(session.scalars(select(HealingAction)).all())
        self.assertEqual(len(quarantined), 3)
        self.assertGreaterEqual(len(actions), 1)


if __name__ == "__main__":
    unittest.main()
