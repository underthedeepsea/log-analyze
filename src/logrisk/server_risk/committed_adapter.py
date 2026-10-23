from __future__ import annotations

import threading
from typing import Any

from logrisk.server_risk.contracts import ServerRiskError, decode, digest, json_text, typed_fields
from logrisk.server_risk.replay import EvaluationContext

MAX_COMMITS = 16


class CommittedObservationAdapter:
    """Independent registered bridge; commit integrity never proves coverage."""

    def __init__(self, repository: Any, observations: Any, policies: Any, clock: Any, *, stage_hook: Any = None) -> None:
        self.repository, self.observations, self.policies, self.clock = repository, observations, policies, clock
        self.stage_hook = stage_hook
        self.lock = threading.Lock()
        self.scan_after: dict[str, tuple[str, str, str]] = {}
        self.cancelled = lambda: False

    def hit(self, stage: str, **details: Any) -> None:
        if self.stage_hook:
            self.stage_hook(stage, details)

    def registrations(self) -> list[tuple[dict[str, Any], dict[str, Any]]]:
        registered = []
        for source_id in self.policies.sources:
            try:
                source = self.policies.source(source_id)
            except ServerRiskError:
                continue
            adapter = source.get("commit_adapter", {})
            if not adapter.get("enabled", False):
                continue
            if not isinstance(adapter, dict) or adapter.get("actor") not in source["permitted_actors"] or not isinstance(adapter.get("source_namespace"), str) or not adapter["source_namespace"] or adapter.get("mode", "current") not in ("current", "historical") or not isinstance(adapter.get("mappings"), dict):
                raise ServerRiskError("commit_adapter_config_invalid", "提交适配器登记不完整")
            registered.append((source, adapter))
        return registered

    @property
    def enabled(self) -> bool:
        return bool(self.registrations())

    def advance(self, source_id: str, task_id: str, commit_id: str) -> dict[str, Any]:
        from logrisk.streaming_state import StreamingIncompleteError, _stored_payload_hash
        self.hit("registered_adapter_admission", source_id=source_id)
        registration = next(((s, a) for s, a in self.registrations() if s["source_id"] == source_id), None)
        if registration is None:
            raise ServerRiskError("commit_adapter_unregistered", "来源未登记自动提交适配器", 403)
        source, adapter = registration
        if adapter.get("task_ids") and task_id not in adapter["task_ids"]:
            raise ServerRiskError("commit_scope_mismatch", "任务不在登记来源范围", 403)
        namespace = "commit:" + digest([source_id, source["source_epoch"], source["config_revision"], task_id, commit_id])
        with self.lock, self.repository.database.transaction() as conn:
            self.hit("commit_integrity_and_all_parts", task_id=task_id, commit_id=commit_id)
            commit = conn.execute("SELECT * FROM streaming_window_commits WHERE task_id=? AND window_id=?", (task_id, commit_id)).fetchone()
            if not commit:
                raise ServerRiskError("commit_not_found", "成功提交不存在", 409)
            size = conn.execute("SELECT COUNT(*) AS rows,SUM(length(window_json)) AS bytes FROM streaming_batch_windows WHERE task_id=? AND window_id=?", (task_id, commit_id)).fetchone()
            if size["rows"] > 10000 or (size["bytes"] or 0) > 134217728:
                raise ServerRiskError("commit_resource_limit", "提交超过派生输入资源上限")
            summary = decode(commit["summary_json"])
            try:
                actual, count = _stored_payload_hash(conn, task_id, commit_id, decode(commit["cursor_json"]), summary)
            except (StreamingIncompleteError, ValueError, TypeError, KeyError):
                raise ServerRiskError("commit_incomplete", "提交窗口不完整", 409) from None
            if not commit["payload_hash"] or actual != commit["payload_hash"] or summary.get("template_count") != count:
                raise ServerRiskError("commit_integrity_invalid", "提交内容摘要或完整数量不匹配", 409)
            old = conn.execute("SELECT input_revision,status FROM server_risk_projection_state WHERE input_namespace=?", (namespace,)).fetchone()
            if old:
                if old["input_revision"] != actual:
                    raise ServerRiskError("commit_revision_conflict", "已派生提交发生变化", 409)
                if old["status"] == "complete":
                    return {"commit_ref": commit_id, "projection_complete": True, "idempotent_replay": True}
            conn.execute("INSERT INTO server_risk_projection_state(input_namespace,input_revision,status,updated_at) VALUES (?,?,'pending',?) ON CONFLICT(input_namespace) DO NOTHING", (namespace, actual, self.clock()))
            items, physicals, subjects = [], [], set()
            after = -1
            while True:
                if self.cancelled():
                    raise ServerRiskError("commit_cancelled", "提交派生已取消")
                page = conn.execute("SELECT * FROM streaming_batch_windows WHERE task_id=? AND window_id=? AND item_index>? ORDER BY item_index LIMIT 250", (task_id, commit_id, after)).fetchall()
                if not page:
                    break
                for row in page:
                    window = decode(row["window_json"])
                    after = row["item_index"]
                    if window.get("source_namespace") != adapter["source_namespace"]:
                        raise ServerRiskError("commit_namespace_mismatch", "提交来源命名空间不匹配", 403)
                    semantic = window.get("risk_semantic") or {}
                    mapping = adapter["mappings"].get(semantic.get("risk_type"))
                    if not mapping:
                        continue
                    self.hit("sanitized_observation_projection", item_index=after)
                    if window.get("entity_type") != "node" or window.get("cluster") != source["cluster"] or window.get("entity_id") not in source["node_ids"]:
                        raise ServerRiskError("commit_scope_mismatch", "窗口节点不在来源范围", 403)
                    fields = typed_fields(semantic.get("semantic_fields"))
                    object_field = mapping.get("object_field")
                    object_id = fields.get(object_field) if object_field else mapping.get("object_id")
                    physical = self.repository._physical(row, window)
                    if not physical or not isinstance(object_id, str) or not object_id:
                        raise ServerRiskError("commit_identity_invalid", "提交缺少稳定物理贡献或对象身份")
                    if not window.get("first_seen") or not window.get("last_seen"):
                        raise ServerRiskError("commit_time_invalid", "提交缺少可信发生时间")
                    identity = {"cluster": window["cluster"], "node_id": window["entity_id"], "object_kind": mapping["object_kind"], "object_id": object_id}
                    if fields.get("boot_id"):
                        identity["boot_id"] = fields["boot_id"]
                    item = {"identity": identity, "kind": mapping["kind"], "signal_code": mapping["signal_code"], "risk_type": source["signals"][mapping["signal_code"]]["risk_type"], "source_sequence": int(after), "source_position_ref": "commit:" + physical, "typed_fields": fields}
                    if window["first_seen"] == window["last_seen"]:
                        item["occurred_at"] = window["first_seen"]
                    else:
                        item.update(interval_start=window["first_seen"], interval_end=window["last_seen"])
                    if item["kind"] == "adverse":
                        item["count"] = window.get("count")
                        item["typed_fields"]["severity"] = semantic.get("severity", "high")
                    if item["kind"] == "coverage":
                        # Only an explicitly registered producer's separate metadata
                        # certificate can prove coverage. Log fields never supply it.
                        if adapter.get("coverage_certificate_source") != "committed_summary":
                            raise ServerRiskError("commit_coverage_unregistered", "覆盖证明来源未登记")
                        certificate = summary.get("server_risk_coverage", {}).get(physical)
                        if not isinstance(certificate, dict):
                            raise ServerRiskError("commit_coverage_missing", "提交缺少独立覆盖证明")
                        item["coverage"] = dict(certificate)
                    subjects.add((identity["cluster"], identity["node_id"]))
                    items.append(item)
                    physicals.append(physical)
                    if len(items) > 10000:
                        raise ServerRiskError("commit_resource_limit", "单次派生超过观测资源上限")
            if len(set(physicals)) != len(physicals):
                raise ServerRiskError("commit_contribution_conflict", "提交物理贡献重复", 409)
            for part in range(0, len(items), 500):
                if self.cancelled():
                    raise ServerRiskError("commit_cancelled", "提交派生已取消")
                key = digest([namespace, actual, part // 500])
                payload = {"schema_version": "server_risk_observation_batch_v1", "source_id": source_id, "source_epoch": source["source_epoch"], "config_revision": source["config_revision"], "batch_id": key, "idempotency_key": key, "mode": adapter.get("mode", "current"), "observations": items[part:part + 500]}
                for item in payload["observations"]:
                    if item["kind"] == "coverage":
                        item["coverage"]["input_frontier_ref"] = "batch:" + key
                self.observations.submit(payload, adapter["actor"], connection=conn, physical_keys=physicals[part:part + 500], defer_persist=True)
                self.hit("commit_part_complete", part=part // 500, count=len(payload["observations"]))
            end = max((item.get("interval_end", item.get("occurred_at")) for item in items), default=None)
            conn.execute("UPDATE server_risk_projection_state SET status='complete',last_fact_ref=?,evaluated_through=?,updated_at=? WHERE input_namespace=?", (json_text({"task_ref": task_id, "commit_ref": commit_id, "parts": (len(items) + 499) // 500, "row_count": count, "observations": len(items)}), end, self.clock(), namespace))
            context = EvaluationContext.historical(end) if end and adapter.get("mode") == "historical" else None
            for cluster, node in subjects:
                self.hit("deterministic_recovery_and_expiry", cluster=cluster, node=node)
                self.observations.assessment.persist(cluster, node, conn, self.clock(), context=context)
            self.hit("commit_frontier_complete", commit_ref=commit_id, count=count)
        return {"commit_ref": commit_id, "projection_complete": True, "idempotent_replay": False, "accepted_count": len(items)}

    def tick(self) -> list[dict[str, Any]]:
        results = []
        for source, adapter in self.registrations():
            remaining = MAX_COMMITS - len(results)
            if remaining <= 0:
                break
            after = self.scan_after.get(source["source_id"], ("", "", ""))
            with self.repository.database.connect() as conn:
                task_ids = adapter.get("task_ids", [])
                task_filter = " AND task_id IN (" + ",".join("?" for _ in task_ids) + ")" if task_ids else ""
                commits = conn.execute("SELECT task_id,window_id,committed_at FROM streaming_window_commits WHERE (committed_at>? OR (committed_at=? AND task_id>?) OR (committed_at=? AND task_id=? AND window_id>?))" + task_filter + " ORDER BY committed_at,task_id,window_id LIMIT ?", (after[0], after[0], after[1], after[0], after[1], after[2], *task_ids, remaining)).fetchall()
            if not commits:
                self.scan_after.pop(source["source_id"], None)
                continue
            for commit in commits:
                self.scan_after[source["source_id"]] = (commit["committed_at"], commit["task_id"], commit["window_id"])
                if adapter.get("task_ids") and commit["task_id"] not in adapter["task_ids"]:
                    results.append({"commit_ref": commit["window_id"], "projection_complete": False, "skipped": True})
                    continue
                try:
                    results.append(self.advance(source["source_id"], commit["task_id"], commit["window_id"]))
                except ServerRiskError as exc:
                    results.append({"commit_ref": commit["window_id"], "projection_complete": False, "error_code": exc.code})
        return results


class CommittedObservationWorker:
    def __init__(self, adapter: CommittedObservationAdapter, *, interval: float = 1.0) -> None:
        self.adapter, self.interval = adapter, max(0.25, interval)
        self.stop = threading.Event()
        self.adapter.cancelled = self.stop.is_set
        self.thread: threading.Thread | None = None
        self.lock = threading.Lock()

    def start(self) -> bool:
        with self.lock:
            if not self.adapter.enabled or self.thread and self.thread.is_alive():
                return False
            self.thread = threading.Thread(target=self._run, name="server-risk-committed-worker", daemon=True)
            self.thread.start()
            return True

    def _run(self) -> None:
        while not self.stop.is_set():
            try:
                self.adapter.tick()
            except ServerRiskError:
                pass
            if self.stop.wait(self.interval):
                return

    def close(self) -> None:
        self.stop.set()
        if self.thread:
            self.thread.join(timeout=2)
            if self.thread.is_alive():
                raise ServerRiskError("source_worker_cleanup_failed", "来源派生资源尚未回收")


def main() -> None:
    """Explicit deployment worker. Never migrate, register sources or start a Broker."""
    import argparse
    import os
    import signal
    from pathlib import Path
    from logrisk.database import create_database, utc_now
    from logrisk.server_risk.assessment import AssessmentService
    from logrisk.server_risk.observations import ObservationService
    from logrisk.server_risk.policies import RiskPolicies
    from logrisk.server_risk.repository import ServerRiskRepository
    parser = argparse.ArgumentParser(description="Bounded registered committed-observation worker")
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=Path("configs/server_risk.yaml"))
    parser.add_argument("--database-provider", choices=("sqlite", "postgres"), default="sqlite")
    args = parser.parse_args()
    policies = RiskPolicies(args.config)
    database = create_database(provider=args.database_provider, sqlite_path=args.database, database_url=os.getenv("LOGRISK_DATABASE_URL") if args.database_provider == "postgres" else None, state_root=args.state_root, migrate=False)
    repository = ServerRiskRepository(database)
    assessment = AssessmentService(repository, policies, utc_now)
    repository.assessment = assessment
    observations = ObservationService(repository, policies, assessment, utc_now)
    worker = CommittedObservationWorker(CommittedObservationAdapter(repository, observations, policies, utc_now))
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_args: stopped.set())
    signal.signal(signal.SIGINT, lambda *_args: stopped.set())
    if not worker.start():
        print("Registered committed source worker disabled")
        return
    try:
        stopped.wait()
    finally:
        worker.close()


if __name__ == "__main__":
    main()
