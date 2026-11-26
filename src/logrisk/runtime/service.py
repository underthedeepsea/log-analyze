from __future__ import annotations

import json
from pathlib import Path
from threading import Lock
from time import monotonic
from typing import Any, Mapping

from logrisk.database import Database
from logrisk.runtime.config import RuntimeConfig
from logrisk.runtime.health import directory_writable, liveness
from logrisk.runtime.repository import RuntimeRepository
from logrisk.runtime.retention import delete_candidates, retention_candidates


STORAGE_READ_TTL_SECONDS = 30


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
        self._storage_lock = Lock()
        self._storage_snapshot: tuple[float, dict[str, Any]] | None = None

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
        tables = {
            "feature_job": ("feature_jobs", "job_id", "job_json, NULL AS stage"),
            "input_job": ("input_jobs", "input_job_id", "progress_json, stage"),
            "streaming": ("streaming_tasks", "task_id", "task_json, stage"),
            "benchmark": ("benchmark_runs", "run_id", "progress_completed, progress_total, error"),
            "replay": ("replay_runs", "replay_id", "error_code, error_message"),
        }
        choices = [name for name in tables if not kind or name == kind]
        page_result = {"items": [], "total": 0, "page": page, "page_size": page_size}
        if not choices:
            return page_result
        where = " WHERE status=?" if status else ""
        parameters = [status] if status else []
        start = (page - 1) * page_size
        with self.database.connect() as connection:
            counts = [f"SELECT COUNT(*) AS count FROM {tables[name][0]}{where}" for name in choices]
            total = int(connection.execute(
                "SELECT SUM(count) FROM (" + " UNION ALL ".join(counts) + ") counted",
                parameters * len(choices),
            ).fetchone()[0])
            page_result["total"] = total
            if start >= total:
                return page_result
            heads = [
                f"SELECT {tables[name][1]} AS task_id, updated_at, '{name}' AS kind, {order} AS kind_order "
                f"FROM {tables[name][0]}{where}"
                for order, name in enumerate(choices)
            ]
            rows = connection.execute(
                "SELECT task_id, kind FROM (" + " UNION ALL ".join(heads) + ") tasks "
                "ORDER BY updated_at DESC, task_id DESC, kind_order LIMIT ? OFFSET ?",
                (*parameters * len(choices), page_size, start),
            ).fetchall()
            details_by_key = {}
            for name in choices:
                task_ids = [head["task_id"] for head in rows if head["kind"] == name]
                if not task_ids:
                    continue
                table, key, details = tables[name]
                marks = ",".join("?" for _ in task_ids)
                details_by_key.update(((name, row["task_id"]), row) for row in connection.execute(
                    f"SELECT {key} AS task_id, status, created_at, updated_at, {details} FROM {table} WHERE {key} IN ({marks})",
                    task_ids,
                ))
            for head in rows:
                name, task_id = head["kind"], head["task_id"]
                row = details_by_key.get((name, task_id))
                if row is not None:
                    page_result["items"].append(self._task_row(name, task_id, row))
        return page_result

    def _task_row(self, kind: str, task_id: str, row: Any) -> dict[str, Any]:
        state = str(row["status"])
        stage = progress = error = error_code = None
        actions = ["view"]
        if kind == "feature_job":
            data = _object(row["job_json"])
            stage, progress = data.get("stage"), data.get("progress")
        elif kind in {"input_job", "streaming"}:
            data = _object(row["progress_json"] if kind == "input_job" else row["task_json"])
            stage, progress, error = row["stage"], data.get("progress"), data.get("error")
            if kind == "streaming" and state in {"failed", "interrupted"}:
                actions.append("resume")
        elif kind == "benchmark":
            stage, error = "benchmark", row["error"]
            progress = self._ratio(row["progress_completed"], row["progress_total"])
            if state in {"pending", "running"}:
                actions.append("cancel")
        else:
            stage, error, error_code = "replay", row["error_message"], row["error_code"]
        return self._task(task_id, kind, state, stage, progress, row["created_at"], row["updated_at"], error, actions, error_code)

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
        with self._storage_lock:
            if self._storage_snapshot is None or monotonic() >= self._storage_snapshot[0]:
                usage = self._measure_storage_usage()
                self._storage_snapshot = (monotonic() + STORAGE_READ_TTL_SECONDS, usage)
            return dict(self._storage_snapshot[1])

    def _measure_storage_usage(self) -> dict[str, Any]:
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
        usage = self._measure_storage_usage()
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

    def readiness(self, *, airflow: Mapping[str, Any] | None = None) -> dict[str, Any]:
        liveness_status = liveness(self.database)
        directories: dict[str, bool] = {}
        for label, path in (("state", self.state_root), ("output", self.output_root)):
            try:
                directories[label] = directory_writable(path)
            except OSError:
                directories[label] = False
        usage = self.storage_usage()
        migrations_ready = bool(liveness_status.get("migrations"))
        airflow_ready = True if airflow is None else bool(airflow.get("ready"))
        ready = bool(
            liveness_status["alive"]
            and migrations_ready
            and all(directories.values())
            and usage["level"] != "blocked"
            and airflow_ready
        )
        airflow_dependency = dict(airflow or {
            "status": "not_checked",
            "ready": True,
            "online": None,
            "dag_registered": None,
        })
        return {
            "ready": ready,
            "status": "ready" if ready else "not_ready",
            "checks": {
                "database": liveness_status["alive"],
                "migrations": migrations_ready,
                "directories": directories,
                "quota": usage["level"],
                "airflow": airflow_ready,
            },
            "dependencies": {
                "model_connections": {"status": "unknown", "required": False},
                "airflow": airflow_dependency,
            },
            "storage_usage": usage,
        }
