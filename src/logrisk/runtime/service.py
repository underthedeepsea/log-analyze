from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from logrisk.database import Database
from logrisk.runtime.config import RuntimeConfig
from logrisk.runtime.health import directory_writable, liveness
from logrisk.runtime.repository import RuntimeRepository
from logrisk.runtime.retention import delete_candidates, retention_candidates


class RuntimeQuotaError(RuntimeError):
    """Raised when accepting a new capacity-consuming action is unsafe."""

    code = "runtime_quota_exceeded"


def _object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


class RuntimeService:
    def __init__(
        self,
        database: Database,
        *,
        state_root: str | Path,
        output_root: str | Path,
        config: RuntimeConfig,
        repository: RuntimeRepository | None = None,
    ) -> None:
        self.database = database
        self.state_root = Path(state_root)
        self.output_root = Path(output_root)
        self.state_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.output_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.config = config
        self.repository = repository or RuntimeRepository(database)

    def retention_policy(self) -> dict[str, Any]:
        """Return the effective retention policy without exposing runtime internals."""
        stored = self.repository.get_policy()
        configured = {
            "enabled": self.config.retention.enabled,
            "completed_days": self.config.retention.completed_days,
            "trace_days": self.config.retention.trace_days,
            "cache_days": self.config.retention.cache_days,
        }
        configured.update(stored["policy"])
        validated = RuntimeConfig.from_mapping({"retention": configured}).retention
        return {
            "enabled": validated.enabled,
            "completed_days": validated.completed_days,
            "trace_days": validated.trace_days,
            "cache_days": validated.cache_days,
            "version": stored["version"],
        }

    def save_retention_policy(
        self,
        policy: Mapping[str, Any],
        *,
        expected_version: int,
        actor: str | None,
        roles: tuple[str, ...] | list[str],
        request_id: str,
    ) -> dict[str, Any]:
        if not isinstance(policy, Mapping):
            raise ValueError("retention policy 必须是 JSON object")
        allowed = {"enabled", "completed_days", "trace_days", "cache_days"}
        unknown = set(policy) - allowed
        if unknown:
            raise ValueError(f"retention policy 包含未知字段: {', '.join(sorted(str(key) for key in unknown))}")
        configured = self.retention_policy()
        candidate = {key: configured[key] for key in allowed}
        candidate.update(dict(policy))
        validated = RuntimeConfig.from_mapping({"retention": candidate}).retention
        stored = self.repository.save_policy(
            {
                "enabled": validated.enabled,
                "completed_days": validated.completed_days,
                "trace_days": validated.trace_days,
                "cache_days": validated.cache_days,
            },
            expected_version=expected_version,
            actor=actor,
            roles=roles,
            request_id=request_id,
        )
        return {**stored, "effective": self.retention_policy()}

    def list_tasks(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        kind: str | None = None,
        status: str | None = None,
    ) -> dict[str, Any]:
        page = max(1, int(page))
        page_size = max(1, min(int(page_size), 200))
        items = self._feature_tasks() + self._input_tasks() + self._streaming_tasks() + self._benchmark_tasks() + self._replay_tasks()
        if kind:
            items = [item for item in items if item["kind"] == kind]
        if status:
            items = [item for item in items if item["status"] == status]
        items.sort(key=lambda item: (str(item["updated_at"]), str(item["task_id"])), reverse=True)
        start = (page - 1) * page_size
        return {"items": items[start:start + page_size], "total": len(items), "page": page, "page_size": page_size}

    def _feature_tasks(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute("SELECT job_id, status, job_json, created_at, completed_at, updated_at FROM feature_jobs").fetchall()
        return [
            self._task(
                row["job_id"], "feature_job", row["status"], _object(row["job_json"]).get("stage"),
                _object(row["job_json"]).get("progress"), row["created_at"], row["updated_at"], None,
                ["view"],
            )
            for row in rows
        ]

    def _input_tasks(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute("SELECT input_job_id, status, stage, progress_json, created_at, updated_at FROM input_jobs").fetchall()
        return [
            self._task(
                row["input_job_id"], "input_job", row["status"], row["stage"],
                _object(row["progress_json"]).get("progress"), row["created_at"], row["updated_at"],
                _object(row["progress_json"]).get("error"), ["view"],
            )
            for row in rows
        ]

    def _streaming_tasks(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute("SELECT task_id, status, stage, task_json, created_at, updated_at FROM streaming_tasks").fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            value = _object(row["task_json"])
            actions = ["view"] + (["resume"] if str(row["status"]) in {"failed", "interrupted"} else [])
            items.append(self._task(
                row["task_id"], "streaming", row["status"], row["stage"], value.get("progress"),
                row["created_at"], row["updated_at"], value.get("error"), actions,
            ))
        return items

    def _benchmark_tasks(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute("SELECT run_id, status, progress_completed, progress_total, error, created_at, updated_at FROM benchmark_runs").fetchall()
        return [
            self._task(
                row["run_id"], "benchmark", row["status"], "benchmark",
                self._ratio(row["progress_completed"], row["progress_total"]), row["created_at"], row["updated_at"], row["error"],
                ["view"] + (["cancel"] if str(row["status"]) in {"pending", "running"} else []),
            )
            for row in rows
        ]

    def _replay_tasks(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute("SELECT replay_id, status, error_code, error_message, created_at, updated_at FROM replay_runs").fetchall()
        return [
            self._task(
                row["replay_id"], "replay", row["status"], "replay", None,
                row["created_at"], row["updated_at"], row["error_message"], ["view"], row["error_code"],
            )
            for row in rows
        ]

    @staticmethod
    def _ratio(completed: Any, total: Any) -> float | None:
        total_value = int(total or 0)
        return round(int(completed or 0) / total_value, 4) if total_value else None

    @staticmethod
    def _task(
        task_id: Any,
        kind: str,
        status: Any,
        stage: Any,
        progress: Any,
        created_at: Any,
        updated_at: Any,
        error_message: Any,
        actions: list[str],
        error_code: Any = None,
    ) -> dict[str, Any]:
        return {
            "task_id": str(task_id), "kind": kind, "status": str(status), "stage": str(stage or "unknown"),
            "progress": progress if isinstance(progress, (int, float)) else None,
            "created_at": created_at, "updated_at": updated_at, "error_code": error_code,
            "error_message": str(error_message) if error_message else None, "actions": actions,
        }

    def storage_usage(self) -> dict[str, Any]:
        state_bytes, state_files = self._directory_usage(self.state_root)
        output_bytes, output_files = self._directory_usage(self.output_root)
        total = state_bytes + output_bytes
        quota = self.config.quota
        level = "blocked" if total >= quota.hard_limit_bytes else ("warning" if total >= quota.soft_limit_bytes else "ok")
        return {
            "state_bytes": state_bytes, "state_files": state_files,
            "output_bytes": output_bytes, "output_files": output_files,
            "total_bytes": total, "soft_limit_bytes": quota.soft_limit_bytes,
            "hard_limit_bytes": quota.hard_limit_bytes, "level": level,
        }

    @staticmethod
    def _directory_usage(root: Path) -> tuple[int, int]:
        total = 0
        files = 0
        if not root.exists():
            return total, files
        for path in root.rglob("*"):
            if path.is_file():
                try:
                    total += path.stat().st_size
                    files += 1
                except OSError:
                    continue
        return total, files

    def require_capacity(self, operation: str, *, additional_bytes: int = 0) -> None:
        if isinstance(additional_bytes, bool) or not isinstance(additional_bytes, int) or additional_bytes < 0:
            raise ValueError("additional_bytes 必须是非负整数")
        usage = self.storage_usage()
        self.repository.record_quota_snapshot(usage)
        total = int(usage.get("total_bytes", usage.get("state_bytes", 0)) or 0)
        hard = int(usage.get("hard_limit_bytes", self.config.quota.hard_limit_bytes) or self.config.quota.hard_limit_bytes)
        if total + additional_bytes > hard:
            raise RuntimeQuotaError(f"存储配额已超过硬限制，不能执行 {operation}")

    def preview_retention(self) -> dict[str, Any]:
        policy = self.retention_policy()
        if not policy["enabled"]:
            return {"enabled": False, "candidate_count": 0, "candidate_bytes": 0, "candidate_paths": []}
        candidates = retention_candidates(
            self.database,
            roots=(self.state_root, self.output_root),
            completed_days=policy["completed_days"],
        )
        return {
            "enabled": True,
            "candidate_count": len(candidates),
            "candidate_bytes": sum(int(item["size_bytes"]) for item in candidates),
            "candidate_paths": [item["path"] for item in candidates],
            "candidates": candidates,
        }

    def run_retention(self, *, actor: str | None, request_id: str, execute: bool) -> dict[str, Any]:
        mode = "execute" if execute else "dry_run"
        run = self.repository.start_maintenance(action="retention", mode=mode, actor=actor, request_id=request_id)
        try:
            preview = self.preview_retention()
            summary: dict[str, Any] = {
                "candidate_count": preview["candidate_count"], "candidate_bytes": preview["candidate_bytes"],
            }
            if execute and preview["enabled"]:
                summary.update(delete_candidates(preview["candidates"]))
                summary["deleted_metadata"] = self._delete_retained_artifact_metadata(preview["candidates"])
            completed = self.repository.finish_maintenance(run["run_id"], status="completed", summary=summary)
            self.repository.append_audit(
                "retention.executed" if execute else "retention.previewed",
                "runtime_maintenance", actor, request_id, summary,
                resource_id=run["run_id"],
            )
            return completed
        except Exception as exc:
            self.repository.finish_maintenance(
                run["run_id"], status="failed", error_code="runtime_maintenance_failed", error_message=str(exc)
            )
            raise

    def _delete_retained_artifact_metadata(self, candidates: list[Mapping[str, Any]]) -> int:
        removable = [
            str(item["artifact_id"])
            for item in candidates
            if item.get("artifact_id") and not Path(str(item["path"])).exists()
        ]
        if not removable:
            return 0
        with self.database.transaction() as connection:
            for artifact_id in removable:
                connection.execute("DELETE FROM artifacts WHERE artifact_id=?", (artifact_id,))
        return len(removable)

    def health(self) -> dict[str, Any]:
        result = liveness(self.database)
        result.update({"schema_version": "runtime_health_v1", "storage_usage": self.storage_usage()})
        return result

    def readiness(self) -> dict[str, Any]:
        liveness_status = liveness(self.database)
        directories: dict[str, bool] = {}
        for label, path in (("state", self.state_root), ("output", self.output_root)):
            try:
                directories[label] = directory_writable(path)
            except OSError:
                directories[label] = False
        usage = self.storage_usage()
        migrations_ready = bool(liveness_status.get("migrations"))
        ready = bool(liveness_status["alive"] and migrations_ready and all(directories.values()) and usage["level"] != "blocked")
        return {
            "ready": ready,
            "status": "ready" if ready else "not_ready",
            "checks": {"database": liveness_status["alive"], "migrations": migrations_ready, "directories": directories, "quota": usage["level"]},
            "dependencies": {"model_connections": {"status": "unknown", "required": False}},
            "storage_usage": usage,
        }
