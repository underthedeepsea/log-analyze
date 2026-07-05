import json

from logrisk.ai_harness.trace_logger import AITraceLogger


def test_trace_logger_appends_jsonl(tmp_path):
    path = tmp_path / "traces" / "ai.jsonl"
    logger = AITraceLogger(path)

    logger.append({"trace_id": "one", "provider": "ollama"})
    logger.append({"trace_id": "two", "provider": "ollama"})

    lines = path.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["trace_id"] for line in lines] == ["one", "two"]


def test_trace_logger_lists_filters_and_skips_bad_lines(tmp_path):
    path = tmp_path / "ai.jsonl"
    path.write_text("not-json\n", encoding="utf-8")
    logger = AITraceLogger(path)

    logger.append({"trace_id": "old", "job_id": "job-a", "status": "success", "prompt_id": "p1", "created_at": "2026-07-01T00:00:00Z", "latency_ms": 100})
    logger.append({"trace_id": "new", "job_id": "job-b", "status": "model_failed", "prompt_id": "p2", "created_at": "2026-07-01T00:01:00Z", "latency_ms": 300})

    assert [item["trace_id"] for item in logger.list_traces()] == ["new", "old"]
    assert logger.list_traces(job_id="job-a")[0]["trace_id"] == "old"
    assert logger.list_traces(status="model_failed")[0]["trace_id"] == "new"
    logger.append({"trace_id": "prompt-old", "prompt_id": "p3", "prompt_hash": "old", "created_at": "2026-07-02T00:02:00Z"})
    logger.append({"trace_id": "prompt-new", "prompt_id": "p3", "prompt_hash": "new", "created_at": "2026-07-02T00:03:00Z"})
    assert [item["trace_id"] for item in logger.list_traces(prompt_id="p3", prompt_hash="new")] == ["prompt-new"]
    assert logger.get_trace("old")["job_id"] == "job-a"
    assert logger.summary_today(now="2026-07-01T12:00:00Z") == {
        "today_calls": 2,
        "cache_hits": 0,
        "success_rate": 0.5,
        "avg_latency_ms": 200,
    }


def test_trace_logger_disabled_does_not_write(tmp_path):
    path = tmp_path / "disabled.jsonl"

    AITraceLogger(path, enabled=False).append({"trace_id": "nope"})

    assert not path.exists()
