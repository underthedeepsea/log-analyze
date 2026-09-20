from __future__ import annotations

from pathlib import Path

import pytest

from logrisk.drain_eval.schema import DrainQualityError
from logrisk.drain_eval.service import DrainQualityService


def test_client_imported_reference_run_cannot_publish_even_if_claimed_server_run(tmp_path):
    subject = DrainQualityService(tmp_path / "quality", tmp_path / "profiles", Path("configs/drain3_recommended.ini"))
    dataset = subject.datasets.create({"name": "gold", "records": [{"schema_version": "drain_gold_v1", "record_id": "r", "source_type": "system", "component": "kernel", "message_core": "error", "gold_group_id": "g", "gold_template": "error", "semantic_fields": {}, "protected_tokens": [], "expected_risk_type": "critical", "annotation_status": "approved"}]})
    candidate = subject.configs.create_candidate({"source_config_id": "baseline", "name": "candidate"})
    run = subject.create_eval_run({"dataset_id": dataset["dataset_id"], "config_id": candidate["config_id"], "config_version": 1, "config_hash": candidate["content_hash"], "server_run": True, "predictions": [{"record_id": "r", "predicted_group_id": "g", "predicted_template": "error"}], "expected_downstream": {"critical_risks": ["r"]}, "actual_downstream": {"critical_risks": ["r"]}})

    assert run["provenance"] == "reference"
    with pytest.raises(DrainQualityError, match="可信服务端 runner"):
        subject.publish_config(candidate["config_id"], 1, {"confirmed": True, "eval_run_id": run["run_id"]})
