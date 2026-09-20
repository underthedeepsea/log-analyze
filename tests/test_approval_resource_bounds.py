from __future__ import annotations

import inspect

from logrisk.approval_service import ApprovalService


def test_review_unit_of_work_is_scoped_to_target_job():
    source = inspect.getsource(ApprovalService.review)
    assert "job_ids={str(job_id)}" in source
