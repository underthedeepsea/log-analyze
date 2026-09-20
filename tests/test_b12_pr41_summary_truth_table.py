from __future__ import annotations

from typing import Any

import pytest

from logrisk.database import SQLiteDatabase
from logrisk.incremental_sources import FileIncrementalSource, SourceCursor
from logrisk.streaming_state import StreamingStateError, StreamingStateRepository


MISSING = object()


def repository(tmp_path):
    source = tmp_path / "source.log"
    source.write_text("safe\n", encoding="utf-8")
    subject = StreamingStateRepository(SQLiteDatabase(tmp_path / "db.sqlite3"))
    task = subject.create_or_load(
        descriptor=FileIncrementalSource(source).descriptor(), config_hash="c" * 64,
    )
    return subject, task


def legacy_summary(**overrides: Any) -> dict[str, Any]:
    value = {
        "record_count": 0,
        "template_count": 0,
        "unknown_template_count": 0,
        "risk_semantic_matches": 0,
        "template_event_count": 0,
        "partition_count": 0,
        "worker_count": 0,
        "parallel": False,
        "node_risk_enabled": False,
        "process_start_method": "not_applicable",
    }
    value.update(overrides)
    return value


def v2_summary(**overrides: Any) -> dict[str, Any]:
    value = legacy_summary(summary_schema_version=2)
    value.update(overrides)
    return value


def commit_values(subject, task_id: str, field: str, values: list[Any]) -> None:
    for index, value in enumerate(values):
        summary = legacy_summary()
        if value is MISSING:
            summary.pop(field)
        else:
            summary[field] = value
        subject.commit_window(
            task_id,
            window_id=f"batch-{index}",
            cursor=SourceCursor("file", {"offset": index + 1}),
            templates=[],
            windows=[],
            summary=summary,
        )


BOOLEAN_CASES = [
    pytest.param([False], False, False, False, id="false"),
    pytest.param([True], True, False, False, id="true"),
    pytest.param([False, False], False, False, False, id="all-false"),
    pytest.param([True, False], True, False, False, id="true-then-false"),
    pytest.param([False, True], True, False, False, id="false-then-true"),
    pytest.param([True, None], True, True, False, id="true-then-none"),
    pytest.param([None, True], True, True, False, id="none-then-true"),
    pytest.param([True, MISSING], True, True, False, id="true-then-missing"),
    pytest.param([MISSING, True], True, True, False, id="missing-then-true"),
    pytest.param([False, None], None, True, False, id="false-then-none"),
    pytest.param([None, False], None, True, False, id="none-then-false"),
    pytest.param([False, MISSING], None, True, False, id="false-then-missing"),
    pytest.param([MISSING, False], None, True, False, id="missing-then-false"),
    pytest.param([None, None], None, True, False, id="all-none"),
    pytest.param([MISSING, MISSING], None, True, False, id="all-missing"),
    pytest.param([None, MISSING], None, True, False, id="none-and-missing"),
    pytest.param(["false"], None, False, True, id="invalid-string"),
    pytest.param([1], None, False, True, id="invalid-one"),
    pytest.param([0], None, False, True, id="invalid-zero"),
    pytest.param([True, "false"], None, False, True, id="true-then-invalid"),
    pytest.param(["false", True], None, False, True, id="invalid-then-true"),
    pytest.param([], False, False, False, id="empty-task"),
]


@pytest.mark.parametrize("field", ["parallel", "node_risk_enabled"])
@pytest.mark.parametrize("values,expected,unknown,invalid", BOOLEAN_CASES)
def test_boolean_summary_truth_table(tmp_path, field, values, expected, unknown, invalid):
    subject, task = repository(tmp_path)
    commit_values(subject, task["task_id"], field, values)

    summary = subject.committed_summary(task["task_id"])

    assert summary[field] is expected
    assert (field in summary["unknown_fields"]) is unknown
    assert (field in summary["invalid_fields"]) is invalid
    assert summary["completeness"] == ("legacy_partial" if unknown or invalid else "complete")


METHOD_CASES = [
    pytest.param(["spawn"], "spawn", False, False, id="spawn"),
    pytest.param(["fork"], "fork", False, False, id="fork"),
    pytest.param(["forkserver"], "forkserver", False, False, id="forkserver"),
    pytest.param(["not_applicable"], "not_applicable", False, False, id="not-applicable"),
    pytest.param(["spawn", "fork"], "mixed", False, False, id="spawn-fork"),
    pytest.param(["fork", "spawn"], "mixed", False, False, id="fork-spawn"),
    pytest.param(["spawn", "not_applicable"], "spawn", False, False, id="spawn-serial"),
    pytest.param(["not_applicable", "spawn"], "spawn", False, False, id="serial-spawn"),
    pytest.param(["spawn", None], "unknown", True, False, id="spawn-none"),
    pytest.param([None, "spawn"], "unknown", True, False, id="none-spawn"),
    pytest.param(["spawn", MISSING], "unknown", True, False, id="spawn-missing"),
    pytest.param([MISSING, "spawn"], "unknown", True, False, id="missing-spawn"),
    pytest.param(["bad"], "unknown", False, True, id="invalid-only"),
    pytest.param([7], "unknown", False, True, id="invalid-type"),
    pytest.param(["spawn", "bad"], "unknown", False, True, id="spawn-invalid"),
    pytest.param(["spawn", "fork", "bad"], "unknown", False, True, id="mixed-invalid"),
    pytest.param(["bad", "not_applicable"], "unknown", False, True, id="invalid-serial"),
]


@pytest.mark.parametrize("values,expected,unknown,invalid", METHOD_CASES)
def test_process_start_method_truth_table(tmp_path, values, expected, unknown, invalid):
    subject, task = repository(tmp_path)
    commit_values(subject, task["task_id"], "process_start_method", values)

    summary = subject.committed_summary(task["task_id"])

    assert summary["process_start_method"] == expected
    assert ("process_start_method" in summary["unknown_fields"]) is unknown
    assert ("process_start_method" in summary["invalid_fields"]) is invalid
    assert summary["completeness"] == ("legacy_partial" if unknown or invalid else "complete")


INVALID_V2_CASES = [
    pytest.param("parallel", "false", "parallel", id="parallel-string"),
    pytest.param("node_risk_enabled", 1, "node_risk_enabled", id="node-risk-integer"),
    pytest.param("worker_count", True, "worker_count", id="bool-is-not-integer"),
    pytest.param("process_start_method", "bad", "process_start_method", id="invalid-method"),
]


@pytest.mark.parametrize("field,value,error", INVALID_V2_CASES)
def test_v2_write_rejects_invalid_summary_without_partial_commit(tmp_path, field, value, error):
    subject, task = repository(tmp_path)
    task_before = subject.get_task(task["task_id"])

    with pytest.raises(StreamingStateError, match=error):
        subject.commit_window(
            task["task_id"],
            window_id="invalid",
            cursor=SourceCursor("file", {"offset": 1}),
            templates=[],
            windows=[],
            summary=v2_summary(**{field: value}),
        )

    assert subject.list_commits(task["task_id"]) == []
    assert subject.get_task(task["task_id"]) == task_before
