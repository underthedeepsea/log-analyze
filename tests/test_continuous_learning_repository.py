from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager

import pytest

from logrisk.continuous_learning import ContinuousLearningError, ContinuousLearningRepository
from logrisk.database import SQLiteDatabase


VALID_GOLD_RECORD = {
    "schema_version": "drain_gold_v1",
    "record_id": "record-1",
    "source_type": "system",
    "component": "kernel",
    "message_core": "unable to load module",
    "gold_group_id": "group-1",
    "gold_template": "unable to load <*> ",
    "semantic_fields": {},
    "protected_tokens": [],
    "expected_risk_type": "availability",
    "annotation_status": "approved",
}
SECOND_VALID_GOLD_RECORD = {
    **VALID_GOLD_RECORD,
    "record_id": "record-2",
    "gold_group_id": "group-2",
}


@pytest.fixture
def database(tmp_path):
    return SQLiteDatabase(tmp_path / "logrisk.sqlite3")


@pytest.fixture
def seeded_candidate(database):
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO feature_jobs(job_id, status, job_json, created_at, updated_at) "
            "VALUES (?, 'completed', ?, ?, ?)",
            ("job-1", "{}", "2026-08-23T00:00:00+00:00", "2026-08-23T00:00:00+00:00"),
        )
        connection.execute(
            "INSERT INTO feature_candidates(candidate_id, job_id, entity_id, status, candidate_json, created_at, updated_at) "
            "VALUES (?, ?, ?, 'pending', ?, ?, ?)",
            (
                "candidate-1",
                "job-1",
                "entity-1",
                json.dumps({"candidate_id": "candidate-1", "title": "Kernel feature"}),
                "2026-08-23T00:00:00+00:00",
                "2026-08-23T00:00:00+00:00",
            ),
        )
    return {"candidate_id": "candidate-1", "job_id": "job-1"}


def test_append_feedback_is_idempotent_and_never_overwrites_a_decision(database, seeded_candidate):
    repository = ContinuousLearningRepository(database)
    first = repository.append_feedback(
        candidate_id=seeded_candidate["candidate_id"],
        job_id=seeded_candidate["job_id"],
        outcome="rejected",
        reason_code="false_positive",
        note="known deployment pattern",
        actor="reviewer-a",
        request_id="req-1",
        idempotency_key="key-1",
    )
    repeated = repository.append_feedback(
        candidate_id=seeded_candidate["candidate_id"],
        job_id=seeded_candidate["job_id"],
        outcome="rejected",
        reason_code="false_positive",
        note="known deployment pattern",
        actor="reviewer-a",
        request_id="req-1",
        idempotency_key="key-1",
    )

    assert repeated["feedback_id"] == first["feedback_id"]
    assert repository.list_feedback(candidate_id=seeded_candidate["candidate_id"])[0]["schema_version"] == "continuous_learning_feedback_v1"

    changed_request = repository.append_feedback(
        candidate_id=seeded_candidate["candidate_id"],
        job_id=seeded_candidate["job_id"],
        outcome="approved",
        reason_code="validated_reuse",
        note="different request must not overwrite",
        actor="reviewer-b",
        request_id="req-2",
        idempotency_key="key-1",
    )
    assert changed_request == first


def test_feedback_foreign_key_blocks_candidate_delete_without_losing_history(database, seeded_candidate):
    repository = ContinuousLearningRepository(database)
    repository.append_feedback(
        candidate_id=seeded_candidate["candidate_id"],
        job_id=seeded_candidate["job_id"],
        outcome="rejected",
        reason_code="false_positive",
        note="keep this history",
        actor="reviewer-a",
        request_id="req-1",
        idempotency_key="key-1",
    )

    with pytest.raises(sqlite3.IntegrityError):
        with database.transaction() as connection:
            connection.execute(
                "DELETE FROM feature_candidates WHERE candidate_id=?", (seeded_candidate["candidate_id"],)
            )

    assert len(repository.list_feedback(candidate_id=seeded_candidate["candidate_id"])) == 1


def test_dataset_revision_rejects_forbidden_payload_keys(database):
    repository = ContinuousLearningRepository(database)

    with pytest.raises(ContinuousLearningError, match="敏感字段"):
        repository.create_dataset_revision(
            family_id="linux-kernel",
            name="Linux kernel",
            description="",
            split="validation",
            records=[{"samples": ["raw log"]}],
            parent_dataset_id=None,
            actor="reviewer-a",
            request_id="req-1",
        )


def test_dataset_revisions_are_append_only_and_content_hash_locked(database):
    repository = ContinuousLearningRepository(database)
    first = repository.create_dataset_revision(
        family_id="linux-kernel",
        name="Linux kernel",
        description="",
        split="validation",
        records=[VALID_GOLD_RECORD],
        parent_dataset_id=None,
        actor="reviewer-a",
        request_id="req-1",
    )
    second = repository.create_dataset_revision(
        family_id="linux-kernel",
        name="Linux kernel",
        description="",
        split="validation",
        records=[VALID_GOLD_RECORD, SECOND_VALID_GOLD_RECORD],
        parent_dataset_id=first["dataset_id"],
        actor="reviewer-a",
        request_id="req-2",
    )

    assert (first["revision_number"], second["revision_number"]) == (1, 2)
    assert first["content_sha256"] != second["content_sha256"]
    assert repository.get_dataset_revision(first["dataset_id"])["record_count"] == 1
    assert [item["dataset_id"] for item in repository.list_dataset_revisions("linux-kernel")] == [
        second["dataset_id"],
        first["dataset_id"],
    ]


def test_dataset_revision_transition_is_optimistic_and_append_only(database):
    repository = ContinuousLearningRepository(database)
    revision = repository.create_dataset_revision(
        family_id="linux-kernel",
        name="Linux kernel",
        description="",
        split="validation",
        records=[VALID_GOLD_RECORD],
        parent_dataset_id=None,
        actor="reviewer-a",
        request_id="req-1",
    )

    approved = repository.transition_dataset_revision(
        revision["dataset_id"], "approved", actor="reviewer-a", request_id="req-2"
    )

    assert approved["lifecycle_status"] == "approved"
    with pytest.raises(ContinuousLearningError, match="状态"):
        repository.transition_dataset_revision(
            revision["dataset_id"], "candidate", actor="reviewer-a", request_id="req-3"
        )


def test_dataset_revision_rejects_missing_or_mismatched_parent(database):
    repository = ContinuousLearningRepository(database)
    first = repository.create_dataset_revision(
        family_id="linux-kernel",
        name="Linux kernel",
        description="",
        split="validation",
        records=[VALID_GOLD_RECORD],
        parent_dataset_id=None,
        actor="reviewer-a",
        request_id="req-1",
    )

    with pytest.raises(ContinuousLearningError, match="parent_dataset_id"):
        repository.create_dataset_revision(
            family_id="linux-kernel",
            name="Linux kernel",
            description="",
            split="validation",
            records=[SECOND_VALID_GOLD_RECORD],
            parent_dataset_id=None,
            actor="reviewer-a",
            request_id="req-2",
        )
    with pytest.raises(ContinuousLearningError, match="父 Dataset 不存在"):
        repository.create_dataset_revision(
            family_id="another-family",
            name="Another",
            description="",
            split="validation",
            records=[SECOND_VALID_GOLD_RECORD],
            parent_dataset_id="missing",
            actor="reviewer-a",
            request_id="req-3",
        )
    assert first["revision_number"] == 1


def test_dataset_revision_rejects_content_tampering(database):
    repository = ContinuousLearningRepository(database)
    revision = repository.create_dataset_revision(
        family_id="linux-kernel",
        name="Linux kernel",
        description="",
        split="validation",
        records=[VALID_GOLD_RECORD],
        parent_dataset_id=None,
        actor="reviewer-a",
        request_id="req-1",
    )
    with database.transaction() as connection:
        connection.execute(
            "UPDATE drain_datasets SET dataset_json=? WHERE dataset_id=?",
            (json.dumps({"records": [SECOND_VALID_GOLD_RECORD]}), revision["dataset_id"]),
        )

    with pytest.raises(ContinuousLearningError, match="摘要"):
        repository.get_dataset_revision(revision["dataset_id"])


def test_dataset_parent_reference_has_a_database_foreign_key(database):
    with database.connect() as connection:
        foreign_keys = connection.execute("PRAGMA foreign_key_list(drain_datasets)").fetchall()

    assert any(row[2] == "drain_datasets" and row[3] == "parent_dataset_id" for row in foreign_keys)


class _PostgresBehaviorDatabase:
    provider = "postgres"

    def __init__(self, database):
        self._database = database

    def connect(self):
        return self._database.connect()

    @contextmanager
    def transaction(self):
        with self._database.transaction() as connection:
            yield connection


def test_dataset_revision_serializes_json_for_postgres_parameter_binding(database):
    repository = ContinuousLearningRepository(_PostgresBehaviorDatabase(database))
    revision = repository.create_dataset_revision(
        family_id="postgres-family",
        name="Postgres family",
        description="",
        split="validation",
        records=[VALID_GOLD_RECORD],
        parent_dataset_id=None,
        actor="reviewer-a",
        request_id="req-1",
    )

    with database.connect() as connection:
        stored = connection.execute(
            "SELECT dataset_json FROM drain_datasets WHERE dataset_id=?", (revision["dataset_id"],)
        ).fetchone()[0]
    assert isinstance(stored, str)
    assert json.loads(stored)["records"] == [VALID_GOLD_RECORD]


def test_dataset_hash_uses_sorted_canonical_json(database):
    repository = ContinuousLearningRepository(database)
    records = [{"record_id": "record-1", "z": 1, "a": 2}]
    revision = repository.create_dataset_revision(
        family_id="family-a",
        name="Family A",
        description="",
        split="validation",
        records=records,
        parent_dataset_id=None,
        actor="reviewer-a",
        request_id="req-1",
    )

    canonical = json.dumps(records, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    assert revision["content_sha256"] == hashlib.sha256(canonical.encode("utf-8")).hexdigest()
