from __future__ import annotations

from typing import Any, Mapping

from logrisk.streaming_observability.contracts import (
    FRESH_SECONDS, MAX_PARTITIONS, OFFSET_SEMANTICS, RESULT_TARGET_SECONDS,
    aggregate_gaps, current, durable_rate, elapsed, offset, offset_vector, reading, states,
)
from logrisk.streaming_observability.repository import (
    ObservabilityError, ObservabilityRepository, decode_cursor, digest, encode_cursor, public_source,
)
from logrisk.streaming_observability.sampler import durable_vector, task_cursor


def result_coverage(
    task: Mapping[str, Any], completed: Mapping[str, Any], evidence: Mapping[str, Any], now: str,
) -> dict[str, Any]:
    durable = durable_vector(task, completed)
    frontier = offset_vector(task_cursor({"cursor": evidence.get("frontier")}).get("partitions"))
    current_identity = task_cursor(task).get("topic_identity") or task_cursor(completed).get("topic_identity")
    result_identity = ((evidence.get("frontier") or {}).get("value") or {}).get("topic_identity")
    same_source = (current_identity is not None and current_identity == result_identity) or (
        evidence.get("task_id") is not None and evidence["task_id"] == task.get("task_id")
        and current_identity == result_identity
    )
    uncovered_at = None
    checked_at = None
    reason = "coverage_unverified"
    status = "unknown"
    wait_seconds = None
    if durable and same_source and set(durable) == set(frontier) and durable == frontier:
        status, reason, checked_at = "covered", None, now
    elif durable and evidence.get("task_id") != task.get("task_id"):
        uncovered_at = evidence.get("first_durable_commit_at")
    elif durable and same_source and set(frontier) <= set(durable) and all(
        frontier[key] <= durable[key] for key in frontier
    ):
        uncovered_at = evidence.get("first_uncovered_at")
    if uncovered_at:
        wait = elapsed(now, uncovered_at)
        if wait is not None and wait >= 0:
            checked_at = now
            wait_seconds = int(wait)
            status = "delayed" if wait > RESULT_TARGET_SECONDS else "pending"
            reason = "result_frontier_behind"
    return {
        "status": status, "reason_code": reason, "generation": evidence.get("generation"),
        "task_id": evidence.get("task_id"), "published_at": evidence.get("published_at"),
        "coverage_checked_at": checked_at, "oldest_uncovered_commit_at": uncovered_at,
        "coverage_target_seconds": RESULT_TARGET_SECONDS, "wait_seconds": wait_seconds,
    }


def _broker_reading(sample: Mapping[str, Any], kind: str, key: str, now: str) -> dict[str, Any]:
    raw = (sample.get(kind) or {}).get(key)
    at = sample.get(f"{kind}_at")
    reason = sample.get(f"{kind}_error") or (sample.get("reason_code") if f"{kind}_at" not in sample else None)
    last = ((sample.get("last_success") or {}).get(kind) or {}).get(key) or [None, None]
    known = offset(raw)
    if known is not None and current(now, at):
        return reading(known, epoch=sample.get("epoch"), collector_id="kafka_offset_sampler", observed_at=at)
    availability = "unknown"
    if reason:
        availability = "forbidden" if reason == "authorization" else "error"
    elif known is not None:
        availability, reason = "stale", "sample_expired"
    elif kind == "group" and key in (sample.get(kind) or {}) and current(now, at):
        reason = "never_committed"
    elif current(now, at):
        reason = "missing_partition"
    if sample.get("sampled_at") and not current(now, sample["sampled_at"]) and not current(now, at):
        availability, reason = "stale", "sample_expired"
    return reading(
        None, epoch=sample.get("epoch"), collector_id="kafka_offset_sampler",
        availability=availability, reason_code=reason or "not_sampled",
        last_known_value=offset(last[0]), last_success_at=last[1],
    )


class ObservabilityService:
    def __init__(self, repository: ObservabilityRepository, *, kafka_enabled: bool) -> None:
        self.repository = repository
        self.kafka_enabled = kafka_enabled

    def sources(self, *, cursor: str | None, limit: int) -> dict[str, Any]:
        return self.repository.list_sources(after=cursor or "", limit=limit, kafka_enabled=self.kafka_enabled)

    def _snapshot(self, source_id: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        source_row, sample, task, completed, worker, evidence = self.repository.facts(source_id)
        now = self.repository.clock()
        source = public_source(source_row, kafka_enabled=self.kafka_enabled)
        batch = task.get("kafka_batch") or {}
        topic_identity = batch.get("identity") or task_cursor(task).get("topic_identity") or source_row.get("broker_identity")
        local_epoch = topic_identity or source_id
        epoch = sample.get("epoch") or local_epoch
        source["source_epoch"] = epoch
        changed_topic = bool(topic_identity and sample.get("identity_verified") and epoch != topic_identity)
        if changed_topic:
            source["block_reason"] = "topic_identity_changed"
            source["block_stage"] = "source_check"
        target = offset_vector(batch.get("target") or task_cursor(task).get("high_water"))
        target_at = batch.get("locked_at")
        live_durable = durable_vector(task, completed)
        sampled_durable = offset_vector(sample.get("durable"))
        # A cached D is usable for comparisons only in the actual aligned
        # observation. Show live D when the cached value is absent or changed.
        paired_durable = (sample.get("durable_epoch") == local_epoch and bool(sampled_durable)
                          and sampled_durable == live_durable and current(now, sample.get("durable_at")))
        confirmed_at = task.get("last_external_commit_at") or completed.get("last_external_commit_at")
        durable_changed_at = task.get("last_durable_commit_at") or completed.get("last_durable_commit_at")
        partition_changed = {**(completed.get("partition_durable_changed_at") or {}),
                             **(task.get("partition_durable_changed_at") or {})}
        d_at = sample.get("durable_at") if paired_durable else durable_changed_at
        frontier_section = task_cursor({"cursor": evidence.get("frontier")})
        result_frontier = offset_vector(frontier_section.get("partitions"))
        result_epoch = frontier_section.get("topic_identity") or source_id
        live_worker = (task.get("status") == "running" and current(now, worker.get("last_heartbeat_at")))
        delivered = offset_vector(worker.get("delivered")) if live_worker else {}
        sampled_keys = sample.get("partitions") or ()
        all_keys = set(live_durable) | set(target) | set(delivered) | set(result_frontier) | set(sampled_keys)
        partition_count = len(all_keys) + max(0, int(sample.get("partition_count") or 0) - len(sampled_keys))
        exclusive = source_row["writer_scope"] == "exclusive_configured"
        rows = []
        for partition in sorted(all_keys)[:MAX_PARTITIONS]:
            key = str(partition)
            rows.append({
                "partition": partition,
                "broker_end": _broker_reading(sample, "end", key, now),
                "group_committed": _broker_reading(sample, "group", key, now),
                "durable": reading(live_durable.get(partition), epoch=local_epoch, collector_id="durable_checkpoint",
                                   observed_at=d_at, last_changed_at=partition_changed.get(key)),
                "delivered": reading(
                    delivered.get(partition), epoch=local_epoch, collector_id="worker_yield",
                    observed_at=worker.get("last_heartbeat_at"),
                    last_changed_at=(worker.get("delivered_at") or {}).get(key),
                    availability="stale" if worker and not live_worker else "unknown",
                    reason_code="worker_epoch_expired" if worker and not live_worker else "not_observed",
                ),
                "batch_target": reading(target.get(partition), epoch=local_epoch, collector_id="batch_checkpoint", observed_at=target_at),
                "result_frontier": reading(
                    result_frontier.get(partition), epoch=result_epoch, collector_id="ready_result",
                    observed_at=evidence.get("published_at"),
                ),
            })
        derived = aggregate_gaps(rows, partition_count, exclusive=exclusive)
        derived["durable_records_per_second"] = durable_rate(sample, sample.get("previous") or {}, now)
        coverage = result_coverage(task, completed, evidence, now)
        if changed_topic:
            coverage.update(status="unknown", reason_code="topic_identity_changed", coverage_checked_at=None,
                            oldest_uncovered_commit_at=None, wait_seconds=None)
        worker_public = {
            key: worker.get(key) for key in (
                "worker_epoch", "last_heartbeat_at", "last_poll_returned_at", "last_record_received_at", "phase",
            )
        }
        scheduler_token = source_row["scheduler_token"]
        scheduler_at = source_row["scheduler_heartbeat_at"]
        scheduler_live = bool(
            scheduler_token and source_row["enabled"] and source_row["scheduler_control_epoch"] == source_row["control_epoch"]
            and current(now, scheduler_at)
        )
        worker_public["scheduler"] = {
            "state": "active" if scheduler_live else ("stale" if scheduler_token else "inactive"),
            "phase": source_row["scheduler_phase"], "last_heartbeat_at": scheduler_at, "owner_epoch": scheduler_token,
        }
        worker_public.update(last_durable_commit_at=durable_changed_at, last_external_commit_at=confirmed_at,
                             assignment_mode="manual", group_membership="not_applicable")
        task_public = {key: task.get(key) for key in (
            "task_id", "status", "stage", "records_processed", "windows_committed", "pending_external_commit_at", "updated_at",
        )} if task else None
        axes, action = states(
            source=source, worker=worker_public, task=task_public, sample=sample,
            derived=derived, coverage=coverage, now=now,
        )
        if not self.kafka_enabled and source["mode"] == "bounded_high_water" and not sample:
            axes["source"] = {"status": "disabled", "reason_code": "worker_disabled"}
        detail = {
            "schema_version": "streaming_observability_v1", "source_id": source_id, "source_epoch": epoch,
            "snapshot_id": digest([
                source_id, source_row["active_task_id"], source_row["completed_task_id"], sample.get("sample_id"),
                sample.get("sampled_at"), worker.get("last_heartbeat_at"), target, live_durable, result_frontier,
                evidence.get("generation"), task.get("lease_token"), task_public, source["enabled"],
                source["block_reason"], source["block_stage"],
                current(now, sample.get("sampled_at")), current(now, sample.get("end_at")),
                current(now, sample.get("group_at")), paired_durable, live_worker,
                scheduler_token, scheduler_at, scheduler_live, source_row["scheduler_phase"],
            ])[:32],
            "source": source, "sampled_at": sample.get("sampled_at"), "served_at": now, "sample_ttl_seconds": FRESH_SECONDS,
            "assignment_mode": "manual", "offset_semantics": OFFSET_SEMANTICS, "end_offset_kind": "read_uncommitted_fetch_end",
            "topic_identity_verified": bool(sample.get("identity_verified") or (not sample and topic_identity)),
            "group_writer_scope": source_row["writer_scope"], "partition_count": partition_count,
            "state": axes, "worker": worker_public, "task": task_public, "result": coverage,
            "derived": derived, "next_action": action,
        }
        return detail, rows

    def detail(self, source_id: str) -> dict[str, Any]:
        return self._snapshot(source_id)[0]

    def partitions(
        self, source_id: str, *, snapshot_id: str | None, cursor: str | None, limit: int,
    ) -> dict[str, Any]:
        page = decode_cursor(cursor, 3)
        if page and (page[0] != source_id or page[1] != snapshot_id or type(page[2]) is not int
                     or not 0 <= page[2] < MAX_PARTITIONS):
            raise ObservabilityError("invalid_cursor", "分页游标必须对应同一个来源快照")
        if not snapshot_id:
            raise ObservabilityError("snapshot_required", "分区分页需要来源快照标识")
        detail, rows = self._snapshot(source_id)
        if detail["snapshot_id"] != snapshot_id:
            raise ObservabilityError("snapshot_expired", "来源快照已更新，请重新加载来源详情", 409)
        start = page[2] if page else 0
        next_page = start + limit
        return {
            "snapshot_id": snapshot_id, "partitions": rows[start:next_page],
            "partition_count": detail["partition_count"],
            "next_cursor": encode_cursor([source_id, snapshot_id, next_page]) if next_page < len(rows) else None,
        }
