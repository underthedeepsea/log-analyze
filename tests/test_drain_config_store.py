from pathlib import Path

import pytest

from logrisk.drain_eval.config_store import DrainConfigStore
from logrisk.drain_eval.schema import DrainQualityError


BASELINE = Path("configs/drain3_recommended.ini").resolve()


def test_baseline_is_read_only_and_candidate_versions_are_append_only(tmp_path):
    store = DrainConfigStore(tmp_path / "quality", BASELINE)
    baseline = store.active_snapshot()

    candidate = store.create_candidate({
        "source_config_id": "baseline",
        "name": "kernel tuned",
        "operator": "qa",
    })
    changed = store.save_version(candidate["config_id"], {
        "expected_version": 1,
        "ini_content": candidate["ini_content"].replace("sim_th = 0.40", "sim_th = 0.45"),
        "operator": "qa",
    })

    assert baseline["config_id"] == "baseline"
    assert baseline["path"] == str(BASELINE)
    assert candidate["version"] == 1
    assert changed["version"] == 2
    assert store.get_version(candidate["config_id"], 1)["parameters"]["sim_th"] == 0.40
    assert store.get_version(candidate["config_id"], 2)["parameters"]["sim_th"] == 0.45
    with pytest.raises(DrainQualityError, match="基线"):
        store.save_version("baseline", {"expected_version": 1, "ini_content": baseline["ini_content"]})


def test_rejects_invalid_parameter_masking_json_and_regex(tmp_path):
    store = DrainConfigStore(tmp_path / "quality", BASELINE)
    candidate = store.create_candidate({"source_config_id": "baseline", "name": "invalid checks"})

    with pytest.raises(DrainQualityError, match="sim_th"):
        store.save_version(candidate["config_id"], {
            "expected_version": 1,
            "ini_content": candidate["ini_content"].replace("sim_th = 0.40", "sim_th = 1.50"),
        })
    with pytest.raises(DrainQualityError, match="masking"):
        store.save_version(candidate["config_id"], {
            "expected_version": 1,
            "ini_content": candidate["ini_content"].replace('"mask_with": "TS"', '"mask_with": "TS" BROKEN', 1),
        })
    with pytest.raises(DrainQualityError, match="正则"):
        store.save_version(candidate["config_id"], {
            "expected_version": 1,
            "ini_content": candidate["ini_content"].replace('"regex_pattern":"\\\\b\\\\d{4}', '"regex_pattern":"[', 1),
        })


def test_rejects_stale_version_and_generates_safe_identifier(tmp_path):
    store = DrainConfigStore(tmp_path / "quality", BASELINE)
    candidate = store.create_candidate({"source_config_id": "baseline", "name": "K8s 参数 / 测试"})

    assert candidate["config_id"].replace("-", "").replace("_", "").isalnum()
    with pytest.raises(DrainQualityError, match="版本冲突"):
        store.save_version(candidate["config_id"], {
            "expected_version": 0,
            "ini_content": candidate["ini_content"],
        })


def test_publish_and_rollback_atomically_change_active_snapshot(tmp_path):
    store = DrainConfigStore(tmp_path / "quality", BASELINE)
    candidate = store.create_candidate({"source_config_id": "baseline", "name": "publish test"})
    second = store.save_version(candidate["config_id"], {
        "expected_version": 1,
        "ini_content": candidate["ini_content"].replace("sim_th = 0.40", "sim_th = 0.45"),
        "operator": "qa",
    })

    with pytest.raises(DrainQualityError, match="人工确认"):
        store.publish(candidate["config_id"], 2, {"confirmed": False})
    published = store.publish(candidate["config_id"], 2, {"confirmed": True, "operator": "qa"})
    assert published["status"] == "published"
    assert store.active_snapshot()["content_hash"] == second["content_hash"]

    rolled_back = store.rollback(candidate["config_id"], 1, {"confirmed": True, "operator": "qa"})
    assert rolled_back["status"] == "published"
    assert store.active_snapshot()["version"] == 1
    events = (tmp_path / "quality" / "config_events.jsonl").read_text(encoding="utf-8")
    assert '"action": "publish"' in events
    assert '"action": "rollback"' in events


def test_validation_returns_structured_parameters_and_masking_rules(tmp_path):
    store = DrainConfigStore(tmp_path / "quality", BASELINE)
    result = store.validate_version("baseline", 1)

    assert result["valid"] is True
    assert result["parameters"]["sim_th"] == 0.40
    assert result["parameters"]["max_clusters"] == 8192
    assert any(rule["mask_with"] == "IP" for rule in result["masking_rules"])
    assert result["content_hash"]
