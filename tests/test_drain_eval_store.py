from __future__ import annotations

import json

import pytest

from logrisk.drain_eval.annotation_store import AnnotationStore
from logrisk.drain_eval.dataset import DatasetStore
from logrisk.drain_eval.schema import DrainQualityError, validate_gold_record


def _record(record_id: str = "log-1") -> dict[str, object]:
    return {
        "schema_version": "drain_gold_v1",
        "record_id": record_id,
        "source_type": "system",
        "component": "kernel",
        "message_core": "NVRM: Xid 79, GPU has fallen off the bus",
        "gold_group_id": "kernel-nvidia-xid-79",
        "gold_template": "NVRM: Xid <XID_CODE>, GPU has fallen off the bus",
        "semantic_fields": {"xid_code": 79},
        "protected_tokens": ["fallen off the bus"],
        "expected_risk_type": "gpu_bus_disconnect",
        "annotation_status": "approved",
    }


def test_gold_schema_rejects_missing_required_field():
    record = _record()
    del record["gold_group_id"]

    with pytest.raises(DrainQualityError, match="gold_group_id"):
        validate_gold_record(record)


def test_dataset_store_uses_versioned_atomic_manifest(tmp_path):
    store = DatasetStore(tmp_path)
    created = store.create({"name": "kernel-gold", "records": [_record()]})

    detail = store.get(created["dataset_id"])
    manifest = json.loads((tmp_path / "datasets.json").read_text(encoding="utf-8"))

    assert detail["schema_version"] == "drain_dataset_v1"
    assert detail["record_count"] == 1
    assert manifest["schema_version"] == "drain_dataset_index_v1"
    assert not list(tmp_path.glob("*.tmp"))


def test_annotation_events_replay_accept_edit_split_merge_and_review(tmp_path):
    store = AnnotationStore(tmp_path)
    accepted = store.append({"cluster_id": "c1", "action": "accept", "reviewer": "alice"})
    store.append({"cluster_id": "c1", "action": "edit", "template": "error <CODE>", "reviewer": "alice"})
    store.append({"cluster_id": "c1", "action": "split", "target_cluster_ids": ["c1-a", "c1-b"], "reviewer": "alice"})
    store.append({"cluster_id": "c2", "action": "merge", "target_cluster_ids": ["c1-a"], "reviewer": "alice"})
    store.review(accepted["annotation_id"], {"decision": "approved", "reviewer": "bob"})

    state = store.replay()

    assert state["c1"]["status"] == "split"
    assert state["c1"]["template"] == "error <CODE>"
    assert state["c2"]["status"] == "merged"
    assert state["c1"]["review_status"] == "approved"
