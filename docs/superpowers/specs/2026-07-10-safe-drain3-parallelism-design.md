# Safe Drain3 Parallelism Design

## Goal

Accelerate large-file Drain3 mining with multiple CPU cores without concurrent mutation of a TemplateMiner or its persisted state.

## Safety Contract

- A Drain3 miner is owned by exactly one worker process at a time.
- Records inside one mining partition keep their original input order.
- A persisted state file belongs to one partition only; workers never write the same file.
- Output events are restored to original input order before aggregation.
- Existing callers retain serial `cluster + source_type + component` behavior by default.

## Large-file Strategy

For uploaded large files, normalize records first, then partition the normalized records by `cluster + node + source_type + component`. Node is included only in the large-file parallel mode, which creates independent state files under the per-job state directory. This gives cluster uploads many independent, deterministic mining partitions while keeping a node/component stream serial.

The parent process schedules partitions through the standard-library `ProcessPoolExecutor`. Worker count is bounded by the configured limit, CPU count, and partition count. A single partition runs serially because parallel writes to one online Drain3 model would change its learned templates.

## Progress and Observability

Large-file job progress reports partition totals, completed partitions, and completed Drain3 records. The result summary records the parallel mode, effective worker count, and partition count for auditability.

## Limits

This design cannot speed up one single node/component stream while preserving exact online Drain3 semantics. Finer hash-based sub-sharding would change clustering behavior and is intentionally excluded.

## Validation

Tests verify node/component partitions run independently, event order is preserved, serial mode remains compatible, worker limits are bounded, and large-file progress/result metadata expose the parallel execution facts.
