from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Iterable, Mapping


FRESH_SECONDS = 45
ALIGN_SECONDS = 5
RESULT_TARGET_SECONDS = 120
MAX_PARTITIONS = 1000
MAX_SNAPSHOT_BYTES = 512 * 1024
OFFSET_MAX = 2**63 - 1
OFFSET_SEMANTICS = "next_offset/read_uncommitted"
FRONTIER_ERRORS = {
    "topic_identity_changed", "partition_set_changed", "offset_out_of_range",
    "group_frontier_changed", "checkpoint_conflict", "frontier_incomparable",
}

METRICS = {
    "undurable_offset_gap": ("broker_end", "durable"),
    "group_offset_gap": ("broker_end", "group_committed"),
    "unconfirmed_commit_offset_gap": ("durable", "group_committed"),
    "read_undurable_offset_gap": ("delivered", "durable"),
    "batch_remaining_offset_gap": ("batch_target", "durable"),
}


def offset(value: Any) -> int | None:
    if isinstance(value, str) and len(value) <= 19 and value.isascii() and value.isdecimal():
        value = int(value)
    return value if type(value) is int and 0 <= value <= OFFSET_MAX else None


def offset_vector(value: Any) -> dict[int, int]:
    if not isinstance(value, Mapping):
        return {}
    result = {}
    for key, item in value.items():
        partition = offset(key)
        position = offset(item)
        if partition is not None and partition <= 2**31 - 1 and position is not None:
            result[partition] = position
    return result


def elapsed(now: str | None, then: str | None) -> float | None:
    if not now or not then:
        return None
    try:
        delta = (datetime.fromisoformat(now.replace("Z", "+00:00")) -
                 datetime.fromisoformat(then.replace("Z", "+00:00"))).total_seconds()
    except (AttributeError, TypeError, ValueError):
        return None
    return delta


def current(now: str, then: str | None, budget: int = FRESH_SECONDS) -> bool:
    age = elapsed(now, then)
    return age is not None and 0 <= age <= budget


def broker_sample_current(now: str, sample: Mapping[str, Any]) -> bool:
    return all(current(now, sample.get(field)) for field in ("sampled_at", "end_at", "group_at"))


def reading(
    value: int | None, *, epoch: str | None, collector_id: str, observed_at: str | None = None,
    availability: str = "unknown", reason_code: str | None = "not_observed",
    last_known_value: int | None = None, last_success_at: str | None = None, last_changed_at: str | None = None,
) -> dict[str, Any]:
    known = offset(value) is not None
    result = {
        "value": str(value) if known else None, "availability": "known" if known else availability,
        "reason_code": None if known else reason_code, "observed_at": observed_at if known else None,
        "last_success_at": observed_at if known else last_success_at,
        "collector_id": collector_id, "epoch": epoch, "last_changed_at": last_changed_at,
    }
    if last_known_value is not None and not known:
        result["last_known_value"] = str(last_known_value)
    return result


def offset_gap(
    left: Mapping[str, Any], right: Mapping[str, Any], *, align: bool = True,
    require_group_writer: bool = False, exclusive: bool = False,
) -> tuple[int | None, str | None, int | None]:
    a, b = offset(left.get("value")), offset(right.get("value"))
    if left.get("availability") != "known" or a is None:
        return None, left.get("reason_code") or "not_observed", None
    if right.get("availability") != "known" or b is None:
        return None, right.get("reason_code") or "not_observed", None
    if not left.get("epoch") or left["epoch"] != right.get("epoch"):
        return None, "epoch_mismatch", None
    if align:
        delta = elapsed(left.get("observed_at"), right.get("observed_at"))
        if delta is None or abs(delta) > ALIGN_SECONDS:
            return None, "sample_misaligned", None
    if a < b:
        return None, "frontier_incomparable", a - b
    if require_group_writer and not exclusive:
        return None, "group_writer_unverified", None
    return a - b, None, None


def aggregate_gaps(
    partitions: Iterable[Mapping[str, Any]], total: int, *, exclusive: bool,
) -> dict[str, Any]:
    rows = tuple(partitions)
    result = {}
    for metric, (left, right) in METRICS.items():
        samples = [
            offset_gap(row[left], row[right], align=metric != "batch_remaining_offset_gap",
                       require_group_writer=metric == "unconfirmed_commit_offset_gap", exclusive=exclusive)
            for row in rows
        ]
        known = [value for value, _, _ in samples if value is not None]
        reasons = {reason for _, reason, _ in samples if reason is not None}
        coverage = "unknown" if not known else "complete" if len(known) == total else "lower_bound"
        entry = {
            "value": str(sum(known)) if known else None, "unit": "offset", "coverage": coverage,
            "known_partitions": len(known), "total_partitions": total,
            "reason_code": "frontier_incomparable" if "frontier_incomparable" in reasons
            else ("partition_limit" if total > len(rows) else (min(reasons) if reasons else None)),
        }
        signed = [difference for _, _, difference in samples if difference is not None]
        if len(signed) == 1:
            entry["raw_signed_difference"] = str(signed[0])
        result[metric] = entry
    return result


def durable_rate(current_sample: Mapping[str, Any], previous: Mapping[str, Any], now: str) -> dict[str, Any]:
    value = None
    sample_seconds = elapsed(current_sample.get("sampled_at"), previous.get("sampled_at"))
    seconds = elapsed(current_sample.get("durable_at"), previous.get("durable_at"))
    count, prior = current_sample.get("records_processed"), previous.get("records_processed")
    epoch = current_sample.get("epoch")
    partitions = current_sample.get("partitions")
    if (current(now, current_sample.get("sampled_at")) and sample_seconds is not None
            and 0 < sample_seconds <= FRESH_SECONDS
            and current(now, current_sample.get("durable_at")) and seconds is not None and 0 < seconds <= FRESH_SECONDS
            and current_sample.get("worker_epoch") and current_sample["worker_epoch"] == previous.get("worker_epoch")
            and current_sample.get("durable_task_id") and current_sample["durable_task_id"] == previous.get("durable_task_id")
            and epoch and epoch == previous.get("epoch") == current_sample.get("durable_epoch") == previous.get("durable_epoch")
            and partitions and partitions == previous.get("partitions")
            and type(count) is int and type(prior) is int and count >= prior >= 0):
        rate = (count - prior) / seconds
        if math.isfinite(rate):
            value = round(rate, 3)
    return {"value": value, "unit": "records/s", "reason_code": None if value is not None else "baseline_required"}


def axis(status: str, reason_code: str | None = None) -> dict[str, Any]:
    return {"status": status, "reason_code": reason_code}


def states(
    *, source: Mapping[str, Any], worker: Mapping[str, Any], task: Mapping[str, Any] | None,
    sample: Mapping[str, Any], derived: Mapping[str, Any], coverage: Mapping[str, Any], now: str,
) -> tuple[dict[str, Any], str | None]:
    status = sample.get("status")
    partial_error = sample.get("group_error")
    sample_fresh = current(now, sample.get("sampled_at"))
    if status == "known":
        if sample_fresh and partial_error:
            source_axis = axis("error", partial_error)
        elif not broker_sample_current(now, sample):
            source_axis = axis("stale", "sample_expired")
        elif sample.get("recovery_pending"):
            source_axis = axis("recovering", "sample_recovery_pending")
        else:
            source_axis = axis("available")
    elif status in {"error", "forbidden"}:
        source_axis = axis("error", sample.get("reason_code")) if sample_fresh else axis("stale", "sample_expired")
    elif source["mode"] == "continuous_bounded" and not source["enabled"]:
        source_axis = axis("disabled", "worker_disabled")
    else:
        source_axis = axis("unknown", "no_current_sample")
    task = task or {}
    scheduler = worker.get("scheduler") or {}
    if source["mode"] == "continuous_bounded" and not source["enabled"]:
        worker_axis = axis("stopped", "worker_disabled")
    elif task.get("status") == "running" and worker.get("last_heartbeat_at"):
        worker_axis = axis("running") if current(now, worker["last_heartbeat_at"]) else axis("stale", "heartbeat_expired")
    elif scheduler.get("state") == "active":
        reason = {"awaiting_batch": "scheduler_waiting", "retry_backoff": "scheduler_retry_backoff"}
        worker_axis = axis("waiting", reason.get(scheduler.get("phase"), "scheduler_checking"))
    elif scheduler.get("state") == "stale":
        worker_axis = axis("stale", "scheduler_heartbeat_expired")
    elif task.get("status") == "running":
        worker_axis = axis("unknown", "process_unverified")
    elif task.get("status") == "completed":
        worker_axis = axis("completed", "bounded_batch_completed")
    else:
        worker_axis = axis("unknown", "process_unverified")

    def positive(metric: str) -> bool:
        value = derived[metric]["value"]
        return value is not None and int(value) > 0

    inconsistent = any(metric.get("reason_code") == "frontier_incomparable" for metric in derived.values())
    pending = elapsed(now, task.get("pending_external_commit_at"))
    rate = derived["durable_records_per_second"]["value"]
    block_reason = source.get("block_reason")
    if block_reason in FRONTIER_ERRORS:
        progress_axis = axis("incomparable", block_reason)
    elif block_reason == "batch_time_budget_exceeded":
        progress_axis = axis("processing", block_reason)
    elif block_reason and source.get("block_stage") == "group_commit":
        progress_axis = axis("commit_failed", "external_commit_failed")
    elif block_reason:
        progress_axis = axis("blocked", block_reason)
    elif inconsistent:
        progress_axis = axis("incomparable", "frontier_incomparable")
    elif pending is not None and pending > 60 and positive("unconfirmed_commit_offset_gap"):
        progress_axis = axis("commit_delayed", "external_commit_pending")
    elif (source_axis["status"] == "available" and worker_axis["status"] == "running"
          and sample.get("stalled_partitions") and positive("undurable_offset_gap")):
        progress_axis = axis("stalled", "durable_frontier_stationary")
    elif worker_axis["status"] == "running" and rate is not None and rate > 0:
        progress_axis = axis("advancing")
    elif (worker_axis["status"] == "running" and current(now, worker.get("last_poll_returned_at")) and
          all(derived[key]["value"] == "0" and derived[key]["coverage"] == "complete"
              for key in ("undurable_offset_gap", "group_offset_gap"))):
        progress_axis = axis("idle")
    elif worker_axis["status"] == "running" and positive("read_undurable_offset_gap"):
        progress_axis = axis("processing", "delivered_not_durable")
    elif task.get("status") == "completed":
        progress_axis = axis("batch_completed", "bounded_batch_completed")
    else:
        progress_axis = axis("unknown", "insufficient_evidence")

    result_axis = axis(coverage["status"], coverage.get("reason_code"))
    action = {
        "incomparable": "inspect_frontier_identity", "stalled": "inspect_worker_phase",
        "commit_delayed": "inspect_pending_commit", "commit_failed": "inspect_pending_commit",
        "blocked": "inspect_worker_phase",
    }.get(progress_axis["status"])
    if action is None:
        action = {"error": "inspect_sampler_error", "stale": "inspect_sampler_freshness"}.get(source_axis["status"])
    if action is None and result_axis["status"] == "delayed":
        action = "inspect_result_projection"
    return {"source": source_axis, "worker": worker_axis, "progress": progress_axis, "result": result_axis}, action
