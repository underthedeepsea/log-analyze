from __future__ import annotations

from pathlib import Path

import pytest

from logrisk.drain_eval.schema import DrainQualityError
from logrisk.drain_eval.service import DrainQualityService


def gold(record_id: str = "r1") -> dict:
    return {"schema_version": "drain_gold_v1", "record_id": record_id, "source_type": "system", "component": "kernel", "message_core": "error", "gold_group_id": "g", "gold_template": "error", "semantic_fields": {}, "protected_tokens": [], "expected_risk_type": "critical", "annotation_status": "approved"}


def service(tmp_path):
    subject = DrainQualityService(tmp_path / "quality", tmp_path / "profiles", Path("configs/drain3_recommended.ini"))
    dataset = subject.datasets.create({"name": "gold", "records": [gold()]})
    return subject, dataset


def test_prediction_cannot_override_gold_fields(tmp_path):
    subject, dataset = service(tmp_path)
    with pytest.raises(DrainQualityError, match="预测字段"):
        subject.create_eval_run({"dataset_id": dataset["dataset_id"], "predictions": [{"record_id": "r1", "predicted_group_id": "p", "predicted_template": "x", "gold_template": "forged"}]})


def test_duplicate_and_unknown_prediction_ids_are_rejected(tmp_path):
    subject, dataset = service(tmp_path)
    prediction = {"record_id": "r1", "predicted_group_id": "p", "predicted_template": "x"}
    with pytest.raises(DrainQualityError):
        subject.create_eval_run({"dataset_id": dataset["dataset_id"], "predictions": [prediction, prediction]})
    with pytest.raises(DrainQualityError):
        subject.create_eval_run({"dataset_id": dataset["dataset_id"], "predictions": [dict(prediction, record_id="unknown")]})
