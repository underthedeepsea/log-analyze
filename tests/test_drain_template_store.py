from __future__ import annotations

import pytest

from logrisk.drain_eval.schema import DrainQualityError
from logrisk.drain_eval.template_store import TemplateStore


def _template(template_hash: str = "h1") -> dict[str, object]:
    return {
        "template_hash": template_hash,
        "template": "error <NUM>",
        "component": "kernel",
        "count": 2,
        "risk_levels": ["high"],
    }


def test_template_edit_preserves_original_and_writes_history(tmp_path):
    store = TemplateStore(tmp_path)
    item = store.import_templates([_template()])[0]
    edited = store.change_template("h1", {
        "action": "edit",
        "template": "error <CODE>",
        "expected_version": item["version"],
        "confirmed": True,
        "operator": "alice",
    })

    assert edited["original_template"] == "error <NUM>"
    assert edited["effective_template"] == "error <CODE>"
    assert edited["version"] == 2
    assert store.history("h1")[-1]["action"] == "edit"


def test_template_changes_require_confirmation_and_current_version(tmp_path):
    store = TemplateStore(tmp_path)
    store.import_templates([_template()])

    with pytest.raises(DrainQualityError, match="确认"):
        store.change_template("h1", {"action": "delete", "expected_version": 1})
    with pytest.raises(DrainQualityError, match="版本冲突"):
        store.change_template("h1", {"action": "ignore", "expected_version": 0, "confirmed": True})


def test_soft_delete_merge_and_rollback_are_auditable(tmp_path):
    store = TemplateStore(tmp_path)
    store.import_templates([_template(), _template("h2")])
    store.change_template("h1", {"action": "merge", "target_template_hash": "h2", "expected_version": 1, "confirmed": True})
    deleted = store.change_template("h1", {"action": "delete", "expected_version": 2, "confirmed": True})
    restored = store.rollback("h1", target_version=1, expected_version=3, confirmed=True, operator="bob")

    assert deleted["status"] == "deleted"
    assert restored["status"] == "active"
    assert restored["effective_template"] == "error <NUM>"
    assert restored["version"] == 4
    assert [event["action"] for event in store.history("h1")] == ["import", "merge", "delete", "rollback"]
