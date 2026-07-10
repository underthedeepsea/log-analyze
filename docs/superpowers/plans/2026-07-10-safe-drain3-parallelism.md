# Safe Drain3 Parallelism Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Process independent large-file Drain3 partitions on multiple CPU cores while preserving record order and exclusive miner state ownership.

**Architecture:** `drain_miner.py` will group normalized records by an explicit partition key and dispatch complete partitions to a standard-library `ProcessPoolExecutor`. Each worker owns a `Drain3ShardManager`, processes its partition sequentially, and returns indexed events; the parent restores input order. The large-file pipeline opts into node/component partitions and reports execution facts.

**Tech Stack:** Python 3.10, Drain3, `concurrent.futures`, pytest.

## Global Constraints

- Do not add dependencies, Kafka, Elasticsearch, databases, or external services.
- Preserve serial mining behavior for existing callers by default.
- Never send raw logs, samples, or raw samples to Ollama.
- Each code update also updates `releas.md`; keep version `1.14.0` because this is an M7 optimization.
- Keep files in `log_analyze_ai_harness_plans/` untracked.

---

### Task 1: Parallel Drain3 partition miner

**Files:**
- Modify: `src/logrisk/drain_miner.py`
- Test: `tests/test_drain_miner.py`

**Interfaces:**
- Consumes: normalized `list[dict[str, Any]]` with `cluster`, `node`, `source_type`, and `component`.
- Produces: `mine_template_events(..., worker_count=1, partition_by_node=False) -> list[dict[str, Any]]`.

- [ ] **Step 1: Write the failing tests**

```python
def test_parallel_mining_preserves_input_order_and_uses_node_partitions(tmp_path):
    records = [
        _record("node-a", "kernel", "error alpha"),
        _record("node-b", "kernel", "error beta"),
        _record("node-a", "kernel", "error gamma"),
    ]
    events, meta = mine_template_events(
        records, "configs/drain3_recommended.ini", tmp_path,
        worker_count=2, partition_by_node=True, return_metadata=True,
    )
    assert [event["event_id"] for event in events] == ["0", "1", "2"]
    assert meta == {"partition_count": 2, "worker_count": 2, "parallel": True}

def test_single_partition_stays_serial_even_when_workers_requested(tmp_path):
    _, meta = mine_template_events(
        [_record("node-a", "kernel", "error")],
        "configs/drain3_recommended.ini", tmp_path,
        worker_count=8, partition_by_node=True, return_metadata=True,
    )
    assert meta["worker_count"] == 1
    assert meta["parallel"] is False
```

- [ ] **Step 2: Run the focused tests and verify failure**

Run: `.venv/bin/python -m pytest tests/test_drain_miner.py -q`

Expected: FAIL because `worker_count`, `partition_by_node`, and `return_metadata` are not accepted.

- [ ] **Step 3: Implement the minimum partitioned miner**

```python
def mining_partition_key(record: dict[str, Any], *, partition_by_node: bool) -> tuple[str, ...]:
    key = (str(record.get("cluster") or "default"),)
    if partition_by_node:
        key += (str(record.get("node") or "unknown"),)
    return key + (
        str(record.get("source_type") or "unknown"),
        str(record.get("component") or "unknown"),
    )

def mine_template_events(..., worker_count: int = 1, partition_by_node: bool = False,
                         return_metadata: bool = False):
    indexed = list(enumerate(records))
    partitions = _partition(indexed, partition_by_node=partition_by_node)
    effective_workers = min(max(1, worker_count), len(partitions))
    # Use the existing sequential miner when effective_workers == 1.
    # Otherwise execute one whole partition per process and order by input index.
```

Worker functions must be module-level so macOS spawn mode can pickle them. Workers create their own `Drain3ShardManager`, and no partition shares a state filename.

- [ ] **Step 4: Run focused tests and verify pass**

Run: `.venv/bin/python -m pytest tests/test_drain_miner.py -q`

Expected: PASS.

### Task 2: Large-file execution metadata and progress

**Files:**
- Modify: `src/logrisk/large_file_pipeline.py`
- Modify: `src/pipeline/dashboard_server.py`
- Modify: `tests/test_large_file_pipeline.py`
- Modify: `tests/test_dashboard_server.py`

**Interfaces:**
- Consumes: `run_large_file_pipeline(..., worker_count: int | None = None)`.
- Produces: `summary.drain3_parallel`, `summary.drain3_worker_count`, `summary.drain3_partition_count`, and progress fields `drain3_partitions_total`, `drain3_partitions_completed`, `drain3_records_processed`.

- [ ] **Step 1: Write the failing tests**

```python
def test_large_file_pipeline_records_parallel_mining_metadata(tmp_path):
    result = run_large_file_pipeline(..., worker_count=4)
    assert result["summary"]["drain3_parallel"] is True
    assert result["summary"]["drain3_worker_count"] == 2
    assert result["summary"]["drain3_partition_count"] == 2

def test_input_job_progress_exposes_drain3_partition_fields(dashboard):
    # Submit a two-node upload and poll /api/input-jobs/{id}.
    assert "drain3_partitions_total" in progress
```

- [ ] **Step 2: Run tests and verify failure**

Run: `.venv/bin/python -m pytest tests/test_large_file_pipeline.py tests/test_dashboard_server.py -q`

Expected: FAIL because metadata and progress fields are absent.

- [ ] **Step 3: Implement the minimum integration**

```python
requested_workers = worker_count or (os.cpu_count() or 1)
result = analyze_records(
    records, ..., drain_worker_count=requested_workers,
    drain_partition_by_node=True,
)
```

Extend `analyze_records()` and its private processor only with optional keyword arguments defaulting to serial compatibility. Use a progress callback from the miner to update the existing `emit("drain3_mining", ...)` event. The dashboard passes the machine default; no new API setting is necessary.

- [ ] **Step 4: Run integration tests and verify pass**

Run: `.venv/bin/python -m pytest tests/test_large_file_pipeline.py tests/test_dashboard_server.py -q`

Expected: PASS.

### Task 3: Documentation and release record

**Files:**
- Modify: `README.md`
- Modify: `releas.md`
- Modify: `AGENTS.md` only if a command needs documentation.

- [ ] **Step 1: Document execution behavior**

Add a short README note explaining node/component parallelism, serial fallback for one partition, and the summary metadata fields.

- [ ] **Step 2: Update release record**

Add the safe Drain3 parallel mining optimization under `1.14.0` in `releas.md`.

- [ ] **Step 3: Verify the complete change**

Run: `.venv/bin/python -m pytest -q && bash -n scripts/*.sh && git diff --check`

Expected: all tests pass, shell validation succeeds, and no whitespace errors are reported.

- [ ] **Step 4: Commit**

```bash
git add src/logrisk/drain_miner.py src/logrisk/large_file_pipeline.py \
  src/pipeline/manual_import_pipeline.py src/pipeline/dashboard_server.py \
  tests/test_drain_miner.py tests/test_large_file_pipeline.py \
  tests/test_dashboard_server.py README.md releas.md
git commit -m "feat: parallelize drain3 large file mining"
```
