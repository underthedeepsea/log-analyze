from __future__ import annotations

from logrisk.database import SQLiteDatabase
from logrisk.sqlite_stores import SQLiteDrainQualityService


def test_sqlite_drain_quality_persists_templates_datasets_and_config_pointer(tmp_path):
    database = SQLiteDatabase(tmp_path / "state" / "logrisk.sqlite3")
    service = SQLiteDrainQualityService(database, "configs/drain3_profiles", "configs/drain3_recommended.ini")
    service.templates.import_templates([{"template_hash": "hash-1", "template": "error <*> ", "component": "kernel", "count": 2}])
    dataset = service.datasets.create({
        "name": "gold",
        "records": [{
            "schema_version": "drain_gold_v1", "record_id": "r1", "source_type": "system", "component": "kernel", "message_core": "error 5",
            "gold_group_id": "g1", "gold_template": "error <NUM>", "semantic_fields": {}, "protected_tokens": [],
            "expected_risk_type": "kernel_error", "annotation_status": "approved",
        }],
    })
    candidate = service.configs.create_candidate({"name": "candidate", "operator": "qa"})
    service.configs.rollback("baseline", 1, {"confirmed": True, "operator": "qa"})

    restored = SQLiteDrainQualityService(database, "configs/drain3_profiles", "configs/drain3_recommended.ini")
    assert restored.templates.get_template("hash-1")["count"] == 2
    assert restored.datasets.get(dataset["dataset_id"])["record_count"] == 1
    assert restored.configs.active_snapshot()["config_id"] == "baseline"
    assert candidate["version"] == 1
    assert not (tmp_path / "state" / "drain-quality-artifacts" / "template_overrides.json").exists()
