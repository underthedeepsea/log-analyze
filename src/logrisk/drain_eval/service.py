from __future__ import annotations

import configparser
import json
import os
import uuid
from pathlib import Path
from typing import Any

from logrisk.drain_eval.annotation_store import AnnotationStore
from logrisk.drain_eval.dataset import DatasetStore, atomic_json
from logrisk.drain_eval.downstream_metrics import evaluate_downstream
from logrisk.drain_eval.labeled_metrics import evaluate_labeled
from logrisk.drain_eval.schema import DrainQualityError, now_iso, require_object
from logrisk.drain_eval.stability import evaluate_stability
from logrisk.drain_eval.template_store import TemplateStore
from logrisk.drain_eval.tuner import grid_candidates, rank_candidates
from logrisk.drain_eval.unlabeled_metrics import evaluate_unlabeled


class DrainQualityService:
    def __init__(self, root: str | Path, profiles_root: str | Path):
        self.root = Path(root)
        self.profiles_root = Path(profiles_root)
        self.datasets = DatasetStore(self.root)
        self.annotations = AnnotationStore(self.root)
        self.templates = TemplateStore(self.root)
        self.profile_events_path = self.root / "profile_events.jsonl"

    def create_eval_run(self, payload: Any) -> dict[str, Any]:
        source = require_object(payload)
        dataset = self.datasets.get(str(source.get("dataset_id") or ""))
        predictions = source.get("predictions")
        if not isinstance(predictions, list):
            raise DrainQualityError("predictions 必须是数组")
        by_id = {str(item.get("record_id")): item for item in predictions if isinstance(item, dict)}
        rows: list[dict[str, Any]] = []
        for gold in dataset["records"]:
            prediction = by_id.get(gold["record_id"])
            if not prediction:
                raise DrainQualityError(f"缺少预测记录: {gold['record_id']}")
            rows.append(dict(gold, **prediction))
        now = now_iso()
        run_id = f"eval_{uuid.uuid4().hex[:16]}"
        metrics = {
            "labeled": evaluate_labeled(rows),
            "unlabeled": evaluate_unlabeled([{
                "template": row.get("predicted_template"),
                "count": 1,
            } for row in rows]),
            "stability": evaluate_stability(source.get("stability_runs") or [predictions]),
            "downstream": evaluate_downstream(source.get("expected_downstream"), source.get("actual_downstream")),
            "performance": dict(source.get("performance") or {}),
        }
        summary = {
            "schema_version": "drain_eval_run_v1",
            "run_id": run_id,
            "dataset_id": dataset["dataset_id"],
            "profile_id": str(source.get("profile_id") or "legacy-default"),
            "status": "completed",
            "progress": 1.0,
            "error": None,
            "metrics": metrics,
            "templates": rows,
            "created_at": now,
            "updated_at": now,
        }
        atomic_json(self.root / "eval_runs" / run_id / "summary.json", summary)
        return summary

    def get_eval_run(self, run_id: str) -> dict[str, Any]:
        path = self.root / "eval_runs" / run_id / "summary.json"
        if not path.exists():
            raise DrainQualityError("评测任务不存在")
        return json.loads(path.read_text(encoding="utf-8"))

    def list_eval_runs(self) -> list[dict[str, Any]]:
        return sorted(
            [json.loads(path.read_text(encoding="utf-8")) for path in self.root.glob("eval_runs/*/summary.json")],
            key=lambda item: str(item.get("created_at")),
            reverse=True,
        )

    def create_tune_run(self, payload: Any) -> dict[str, Any]:
        source = require_object(payload)
        results = source.get("candidates")
        if results is None:
            results = [dict(parameters, profile_id=f"grid-{index + 1}", metrics={}, logs_per_second=0) for index, parameters in enumerate(grid_candidates())]
        if not isinstance(results, list):
            raise DrainQualityError("candidates 必须是数组")
        now = now_iso()
        run_id = f"tune_{uuid.uuid4().hex[:16]}"
        ranked = rank_candidates(results)
        summary = {
            "schema_version": "drain_tune_run_v1",
            "run_id": run_id,
            "status": "completed",
            "progress": 1.0,
            "error": None,
            "dataset_id": source.get("dataset_id"),
            "candidate_count": len(ranked),
            "ranked_candidates": ranked,
            "created_at": now,
            "updated_at": now,
        }
        atomic_json(self.root / "tune_runs" / run_id / "summary.json", summary)
        return summary

    def get_tune_run(self, run_id: str) -> dict[str, Any]:
        path = self.root / "tune_runs" / run_id / "summary.json"
        if not path.exists():
            raise DrainQualityError("调参任务不存在")
        return json.loads(path.read_text(encoding="utf-8"))

    def _profile_events(self) -> list[dict[str, Any]]:
        if not self.profile_events_path.exists():
            return []
        return [json.loads(line) for line in self.profile_events_path.read_text(encoding="utf-8").splitlines() if line.strip()]

    def _append_profile_event(self, event: dict[str, Any]) -> None:
        events = self._profile_events()
        events.append(event)
        self.profile_events_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.profile_events_path.with_name(f".{self.profile_events_path.name}.{uuid.uuid4().hex}.tmp")
        temporary.write_text("".join(json.dumps(item, ensure_ascii=False) + "\n" for item in events), encoding="utf-8")
        os.replace(temporary, self.profile_events_path)

    def list_profiles(self) -> list[dict[str, Any]]:
        latest: dict[str, str] = {}
        for event in self._profile_events():
            latest[event["profile_id"]] = event["status"]
        profiles: list[dict[str, Any]] = []
        for path in sorted(self.profiles_root.glob("*.ini")):
            parser = configparser.ConfigParser(interpolation=None)
            parser.read(path, encoding="utf-8")
            drain = parser["DRAIN"] if parser.has_section("DRAIN") else {}
            profile_id = path.stem
            profiles.append({
                "schema_version": "drain_profile_v1",
                "profile_id": profile_id,
                "name": profile_id.replace("_", " "),
                "path": str(path),
                "status": latest.get(profile_id, "candidate"),
                "parameters": {
                    "sim_th": float(drain.get("sim_th", 0.4)),
                    "depth": int(drain.get("depth", 5)),
                    "max_children": int(drain.get("max_children", 150)),
                    "parametrize_numeric_tokens": str(drain.get("parametrize_numeric_tokens", "true")).lower() == "true",
                    "extra_delimiters": drain.get("extra_delimiters", '["="]'),
                },
            })
        return profiles

    def _change_profile(self, profile_id: str, payload: Any, status: str) -> dict[str, Any]:
        source = require_object(payload)
        if source.get("confirmed") is not True:
            raise DrainQualityError("Profile 变更需要人工确认")
        profile = next((item for item in self.list_profiles() if item["profile_id"] == profile_id), None)
        if not profile:
            raise DrainQualityError("Profile 不存在")
        event = {
            "schema_version": "drain_profile_event_v1",
            "event_id": f"profile_event_{uuid.uuid4().hex}",
            "profile_id": profile_id,
            "status": status,
            "reviewer": str(source.get("reviewer") or "local-operator"),
            "created_at": now_iso(),
        }
        self._append_profile_event(event)
        return dict(profile, status=status, updated_at=event["created_at"])

    def promote_profile(self, profile_id: str, payload: Any) -> dict[str, Any]:
        return self._change_profile(profile_id, payload, "promoted")

    def rollback_profile(self, profile_id: str, payload: Any) -> dict[str, Any]:
        return self._change_profile(profile_id, payload, "rolled_back")
