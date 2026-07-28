from __future__ import annotations

from logrisk.database import SQLiteDatabase
from logrisk.observability import ObservabilityRepository, SpanRecorder


def test_observability_migration_creates_normalized_tables(tmp_path):
    database = SQLiteDatabase(tmp_path / "state" / "logrisk.sqlite3")

    with database.connect() as connection:
        names = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }

    assert {
        "observability_runs",
        "observability_spans",
        "replay_runs",
        "replay_events",
    } <= names


def test_repository_records_parented_spans_and_finishes_idempotently(tmp_path):
    repository = ObservabilityRepository(
        SQLiteDatabase(tmp_path / "state" / "logrisk.sqlite3")
    )
    observation = repository.create_observation(job_id="job-1")
    root = repository.start_span(
        observation_id=observation["observation_id"],
        name="feature-job",
        stage="aggregate",
        idempotency_key="job-1:aggregate",
    )
    child = repository.start_span(
        observation_id=observation["observation_id"],
        parent_span_id=root["span_id"],
        trace_id="trace-1",
        name="model",
        stage="model",
    )
    finished = repository.finish_span(child["span_id"], status="success")
    duplicate = repository.start_span(
        observation_id=observation["observation_id"],
        name="duplicate",
        stage="aggregate",
        idempotency_key="job-1:aggregate",
    )

    assert finished["parent_span_id"] == root["span_id"]
    assert finished["duration_ms"] >= 0
    assert duplicate["span_id"] == root["span_id"]


def test_span_recorder_removes_raw_and_secret_fields(tmp_path):
    recorder = SpanRecorder(
        ObservabilityRepository(
            SQLiteDatabase(tmp_path / "state" / "logrisk.sqlite3")
        )
    )
    observation = recorder.ensure_observation(job_id="job-1")

    span = recorder.record(
        observation_id=observation["observation_id"],
        name="evidence",
        stage="evidence",
        status="success",
        attributes={
            "template_hash": "abc",
            "samples": ["raw"],
            "nested": {"api_key": "secret", "count": 3},
        },
    )

    assert span is not None
    assert span["attributes"] == {
        "template_hash": "abc",
        "nested": {"count": 3},
    }


def test_span_recorder_marks_observation_completed_or_failed(tmp_path):
    repository = ObservabilityRepository(
        SQLiteDatabase(tmp_path / "state" / "logrisk.sqlite3")
    )
    recorder = SpanRecorder(repository)
    completed = recorder.ensure_observation(job_id="job-completed")
    failed = recorder.ensure_observation(job_id="job-failed")

    recorder.finish_observation(completed["observation_id"])
    recorder.finish_observation(failed["observation_id"], failed=True)

    assert repository.get_observation(completed["observation_id"])["status"] == "completed"
    assert repository.get_observation(failed["observation_id"])["status"] == "failed"
