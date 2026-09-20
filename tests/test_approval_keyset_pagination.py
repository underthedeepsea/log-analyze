from __future__ import annotations

from logrisk.approval_queue import build_review_groups


def test_review_key_keyset_does_not_skip_after_earlier_groups_are_removed():
    def item(candidate_id: str) -> dict:
        return {
            "candidate_id": candidate_id, "status": "pending", "feature_type": "pattern",
            "title": candidate_id, "template_signatures": [{"template_hash": candidate_id}],
            "components": ["kernel"], "schema_version": "approved_rule_v1",
        }

    groups = build_review_groups([item(name) for name in ("a", "b", "c", "d")])
    first_keys = [group["review_key"] for group in groups[:2]]
    after = first_keys[-1]
    remaining = [group for group in groups if group["review_key"] > after]

    assert len(remaining) == 2
    assert all(group["review_key"] not in first_keys for group in remaining)


def test_database_cursor_survives_approvals_and_rebuild_invalidates_it(tmp_path, monkeypatch):
    import pytest
    from logrisk.database import SQLiteDatabase
    from logrisk.approval_projection import rebuild_projection
    from tests.test_approval_throughput import seed_candidates
    from logrisk.sqlite_stores import SQLiteFeatureJobStore
    database = SQLiteDatabase(tmp_path / "queue.sqlite3")
    seed_candidates(database,count=4,distinct=True)
    store = SQLiteFeatureJobStore(database)
    monkeypatch.setattr(store,"list_candidates",lambda *a,**kw: (_ for _ in ()).throw(AssertionError("full read")))
    page = store.approval_page(status="pending",page_size=2)
    for group in page["items"]:
        store.update_candidate_review_state(group["representative"]["candidate_id"],{"status":"rejected"},expected_status="pending")
    second = store.approval_page(status="pending",page_size=2,cursor=page["next_cursor"])
    assert len(second["items"]) == 2
    assert not set(g["review_key"] for g in page["items"]) & set(g["review_key"] for g in second["items"])
    with pytest.raises(ValueError,match="invalid_cursor"):
        store.approval_page(status="approved",page_size=2,cursor=page["next_cursor"])
    with database.transaction() as connection:
        rebuild_projection(connection)
    with pytest.raises(ValueError,match="invalid_cursor"):
        store.approval_page(status="pending",page_size=2,cursor=page["next_cursor"])
