#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
import tempfile
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from logrisk.database import utc_now
from logrisk.server_risk.contracts import LING_MODEL, json_text
from logrisk.server_risk.replay import import_history
from logrisk.server_risk.committed_adapter import CommittedObservationAdapter, CommittedObservationWorker
from pipeline.dashboard_server import build_server


def seed_commit(database: Any, task: str, key: str, items: list[dict[str, Any]]) -> None:
    from logrisk.streaming_state import _payload_hash
    cursor, summary = {"kind": "file", "value": {"offset": 0}}, {"template_count": len(items)}
    with database.transaction() as conn:
        conn.execute("INSERT INTO streaming_tasks(task_id,source_kind,source_identity_json,config_hash,status,stage,cursor_json,task_json,created_at,updated_at) VALUES (?,'file','{}','synthetic','completed','COMPLETED',?,'{}',?,?) ON CONFLICT(task_id) DO NOTHING", (task, json_text(cursor), utc_now(), utc_now()))
        conn.execute("INSERT INTO streaming_window_commits(task_id,window_id,cursor_json,summary_json,committed_at,payload_hash) VALUES (?,?,?,?,?,?)", (task, key, json_text(cursor), json_text(summary), utc_now(), _payload_hash(cursor, items, summary)))
        for index, item in enumerate(items):
            conn.execute("INSERT INTO streaming_batch_windows(task_id,window_id,item_index,window_json) VALUES (?,?,?,?)", (task, key, index, json_text(item)))


def synthetic_window(node: str, when: str, position: str, *, positive: bool = False) -> dict[str, Any]:
    return {"cluster": "SYNTHETIC", "entity_type": "node", "entity_id": node, "source_namespace": "synthetic-preview", "source_batch_id": "chain", "source_item_id": position, "window_start": when, "window_end": when, "first_seen": when, "last_seen": when, "count": 1, "risk_semantic": {"risk_type": "fixture.healthy" if positive else "service.failed", "severity": "high", "semantic_fields": {"unit": "preview.service", "boot_id": "synthetic-boot", "ready": True} if positive else {"unit": "preview.service", "boot_id": "synthetic-boot"}}}


def register_synthetic(service: Any) -> None:
    policies = service.assessments.policies
    policies.sources["synthetic-history"] = {"source_id": "synthetic-history", "enabled": True, "source_epoch": "synthetic-epoch", "config_revision": "synthetic-config", "environment": "synthetic", "cluster": "SYNTHETIC", "node_ids": ["synthetic-history", "synthetic-live"], "capability": "authoritative_state", "objects": [{"object_kind": "unit", "object_id": "preview.service"}], "signals": {"failed": {"kind": "adverse", "risk_type": "service.failed"}, "unit_expected_state": {"kind": "state_snapshot", "risk_type": "service.failed", "field_types": {"ready": "boolean"}}}, "permitted_actors": ["synthetic-worker"], "freshness_seconds": 100, "max_gap_seconds": 12, "allowed_lateness_seconds": 864000000, "commit_adapter": {"enabled": True, "actor": "synthetic-worker", "source_namespace": "synthetic-preview", "task_ids": ["synthetic-history"], "mode": "historical", "mappings": {"service.failed": {"kind": "adverse", "signal_code": "failed", "object_kind": "unit", "object_field": "unit"}, "fixture.healthy": {"kind": "state_snapshot", "signal_code": "unit_expected_state", "object_kind": "unit", "object_field": "unit"}}}}
    import copy
    live = copy.deepcopy(policies.sources["synthetic-history"])
    live["source_id"] = "synthetic-live"
    live["node_ids"] = ["synthetic-live"]
    live["commit_adapter"]["task_ids"] = ["synthetic-live"]
    live["commit_adapter"]["mode"] = "current"
    policies.sources["synthetic-history"]["node_ids"] = ["synthetic-history"]
    policies.sources["synthetic-live"] = live
    policies.rules["service.failed"] = {"mode": "state", "recovery_signal": "unit_expected_state", "positive_field": "ready", "positive_value": True, "hold_seconds": 10}
    from logrisk.server_risk.contracts import digest
    policies.version = digest({"sources": policies.sources, "rules": policies.rules})


class PreviewFakeModel:
    def __init__(self) -> None:
        self.calls = 0
        self.last_metadata: dict[str, Any] = {}

    def generate_json(self, messages: Any, schema: Any, *, model: str, timeout: float, options: Any) -> dict[str, Any]:
        if model != LING_MODEL:
            raise ValueError("fixed Ling required")
        self.calls += 1
        evidence = json.loads(messages[1]["content"])
        fact = next(iter(evidence["facts"]), None)
        return {"evidence_digest": evidence["evidence_digest"], "blocks": [{"claim_code": "risk_summary" if fact else "coverage_gap", "fact_refs": [fact["fact_ref"]] if fact else [], "risk_refs": []}]}


def build_preview(source: Path, output_root: Path, *, synthetic: bool = False, fake_model: bool = False) -> tuple[Any, dict[str, Any]]:
    root = output_root.absolute()
    # The CLI only permits ignored output/ or temporary directories.
    approved = ((Path.cwd() / "output").resolve(), Path(tempfile.gettempdir()).resolve())
    resolved = root.resolve()
    if not any(path in resolved.parents for path in approved) or any(parent.is_symlink() for parent in [root, *root.parents]):
        from logrisk.server_risk.contracts import ServerRiskError
        raise ServerRiskError("replay_destination_invalid", "预览必须位于受控output或临时目录")
    root.mkdir(parents=True, exist_ok=True)
    manifest = import_history(source, root / "state" / "preview.sqlite3", allowed_root=root)
    server = build_server("127.0.0.1", 0, database_path=root / "state" / "preview.sqlite3", state_root=root / "state", output_root=root / "artifacts", node_risk_reports_enabled=fake_model, ollama_checker=lambda: {"online": False, "models": []}, input_analyzer=lambda rows: {"risk_entities": [], "top_templates": []})
    service = server.server_risk
    service.source_worker.close()
    if fake_model:
        fake = PreviewFakeModel()
        service.reports.model_factory = lambda connection: fake
    stages: deque[dict[str, Any]] = deque(maxlen=1024)
    if synthetic:
        register_synthetic(service)
        history = [synthetic_window("synthetic-history", stamp, str(i), positive=i > 0) for i, stamp in enumerate(("2026-08-31T15:59:39+00:00", "2026-08-31T15:59:49+00:00", "2026-08-31T15:59:59+00:00"))]
        seed_commit(server.database, "synthetic-history", "synthetic-historical-chain", history)
        now = datetime.now(timezone.utc)
        live = [synthetic_window("synthetic-live", (now - timedelta(seconds=30 - i * 10)).isoformat(), "live-" + str(i), positive=i > 0) for i in range(3)]
        seed_commit(server.database, "synthetic-live", "synthetic-live-chain", live)
    service.committed_adapter = CommittedObservationAdapter(service.repository, service.observations, service.assessments.policies, utc_now, stage_hook=lambda stage, details: stages.append({"stage": stage, **details}))
    service.source_worker = CommittedObservationWorker(service.committed_adapter, interval=0.25)
    service.source_worker.start()
    # Wait for independent worker, never call advance from the query/HTTP path.
    if synthetic:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if any(row["stage"] == "commit_frontier_complete" and row.get("commit_ref") == "synthetic-live-chain" for row in list(stages)):
                break
            time.sleep(.025)
        if not any(row["stage"] == "commit_frontier_complete" for row in list(stages)):
            server.server_close()
            raise RuntimeError("Synthetic source worker did not complete")
    manifest.update({"url": "http://127.0.0.1:" + str(server.server_address[1]) + "/node-risks", "database": str(root / "state" / "preview.sqlite3"), "fake_model_explicitly_injected": fake_model, "synthetic_separate": synthetic, "current_clock": "server wall-clock only", "historical_day": "2026-08-31", "synthetic_node_url": "http://127.0.0.1:" + str(server.server_address[1]) + "/node-risks/SYNTHETIC/synthetic-history", "stage_hits": list(stages), "stage_trace_capacity": stages.maxlen})
    (root / "preview-manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
    return server, manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Readonly isolated historical/synthetic server-risk preview")
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--synthetic", action="store_true")
    parser.add_argument("--fake-model", action="store_true")
    args = parser.parse_args()
    server, manifest = build_preview(args.source, args.output_root, synthetic=args.synthetic, fake_model=args.fake_model)
    print(json.dumps(manifest, ensure_ascii=False), flush=True)
    def stop(_signum: int, _frame: Any) -> None:
        threading.Thread(target=server.shutdown, name="preview-stop").start()
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
