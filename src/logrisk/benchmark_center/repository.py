from __future__ import annotations

import json
import uuid
from typing import Any, Callable

from logrisk.database import SQLiteDatabase, utc_now


RUN_STATUSES = {"pending", "running", "completed", "failed", "cancelled"}
RUN_MODES = {"fake", "history", "real"}
FORBIDDEN_RAW_EVIDENCE_KEYS = {
    "samples",
    "raw_sample",
    "raw_samples",
    "raw_log",
    "raw_logs",
    "raw_text",
    "log_lines",
    "original_log",
    "original_logs",
}


class BenchmarkError(RuntimeError):
    def __init__(self, message: str, *, code: str = "invalid_request", status_code: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _object(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError) as exc:
        raise BenchmarkError("Benchmark 数据 JSON 损坏", code="corrupt_state", status_code=500) from exc


def _contains_raw_evidence(value: Any) -> bool:
    if isinstance(value, dict):
        return any(str(key).lower() in FORBIDDEN_RAW_EVIDENCE_KEYS or _contains_raw_evidence(item) for key, item in value.items())
    if isinstance(value, list):
        return any(_contains_raw_evidence(item) for item in value)
    return False


class BenchmarkRepository:
    def __init__(self, database: SQLiteDatabase, clock: Callable[[], str] = utc_now) -> None:
        self.database = database
        self.clock = clock

    @staticmethod
    def _page(page: int, page_size: int) -> tuple[int, int]:
        if page < 1 or page_size < 1 or page_size > 100:
            raise BenchmarkError("分页参数无效", code="invalid_pagination")
        return page_size, (page - 1) * page_size

    @staticmethod
    def _suite(row: Any) -> dict[str, Any]:
        payload = _object(row["suite_json"], {})
        return {
            **payload,
            "suite_id": row["suite_id"],
            "name": row["name"],
            "source_type": row["source_type"],
            "case_count": int(row["case_count"]),
            "version": int(row["version"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "schema_version": row["schema_version"],
        }

    @staticmethod
    def _run(row: Any) -> dict[str, Any]:
        return {
            "run_id": row["run_id"],
            "suite_id": row["suite_id"],
            "mode": row["mode"],
            "status": row["status"],
            "idempotency_key": row["idempotency_key"],
            "snapshot": _object(row["snapshot_json"], {}),
            "metrics": _object(row["metrics_json"], {}),
            "progress": {
                "completed": int(row["progress_completed"]),
                "total": int(row["progress_total"]),
            },
            "error": row["error"],
            "version": int(row["version"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "schema_version": row["schema_version"],
        }

    def _audit(
        self,
        connection: Any,
        event_type: str,
        payload: dict[str, Any],
        *,
        run_id: str | None = None,
        operator: str = "system",
        created_at: str | None = None,
    ) -> None:
        connection.execute(
            "INSERT INTO benchmark_audit_events(event_id, run_id, event_type, event_json, operator, created_at, schema_version) "
            "VALUES (?, ?, ?, ?, ?, ?, 'benchmark_audit_event_v1')",
            (f"benchmark-event-{uuid.uuid4().hex}", run_id, event_type, _json(payload), operator, created_at or self.clock()),
        )

    def create_suite(self, payload: dict[str, Any]) -> dict[str, Any]:
        suite_id = str(payload.get("suite_id") or f"suite-{uuid.uuid4().hex}").strip()
        name = str(payload.get("name") or "").strip()
        source_type = str(payload.get("source_type") or "custom")
        cases = payload.get("cases") or []
        if not suite_id or not name or source_type not in {"canonical", "trace", "custom"} or not isinstance(cases, list):
            raise BenchmarkError("Suite 参数无效")
        if _contains_raw_evidence(cases):
            raise BenchmarkError(
                "Benchmark Suite 只能保存聚合、脱敏后的 Evidence",
                code="raw_evidence_forbidden",
                status_code=422,
            )
        now = self.clock()
        snapshot = dict(payload, suite_id=suite_id, name=name, source_type=source_type, cases=cases)
        with self.database.transaction() as connection:
            existing = connection.execute("SELECT * FROM benchmark_suites WHERE suite_id=?", (suite_id,)).fetchone()
            if existing:
                return self._suite(existing)
            connection.execute(
                "INSERT INTO benchmark_suites(suite_id, name, source_type, case_count, suite_json, version, created_at, updated_at, schema_version) "
                "VALUES (?, ?, ?, ?, ?, 1, ?, ?, 'benchmark_suite_v1')",
                (suite_id, name, source_type, len(cases), _json(snapshot), now, now),
            )
            self._audit(connection, "suite_created", {"suite_id": suite_id, "case_count": len(cases)}, operator=str(payload.get("operator") or "system"), created_at=now)
        return self.get_suite(suite_id)

    def get_suite(self, suite_id: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM benchmark_suites WHERE suite_id=?", (suite_id,)).fetchone()
        if row is None:
            raise BenchmarkError("评测 Suite 不存在", code="suite_not_found", status_code=404)
        return self._suite(row)

    def list_suites(self, *, page: int = 1, page_size: int = 50) -> dict[str, Any]:
        limit, offset = self._page(page, page_size)
        with self.database.connect() as connection:
            total = int(connection.execute("SELECT COUNT(*) FROM benchmark_suites").fetchone()[0])
            rows = connection.execute(
                "SELECT * FROM benchmark_suites ORDER BY updated_at DESC, suite_id LIMIT ? OFFSET ?", (limit, offset)
            ).fetchall()
        return {"schema_version": "benchmark_suite_list_v1", "items": [self._suite(row) for row in rows], "pagination": {"page": page, "page_size": page_size, "total": total}}

    def create_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        run_id = str(payload.get("run_id") or f"run-{uuid.uuid4().hex}").strip()
        suite_id = str(payload.get("suite_id") or "").strip()
        mode = str(payload.get("mode") or "fake")
        idempotency_key = str(payload.get("idempotency_key") or run_id).strip()
        if mode not in RUN_MODES or not suite_id or not idempotency_key:
            raise BenchmarkError("Run 参数无效")
        now = self.clock()
        with self.database.transaction() as connection:
            duplicate = connection.execute("SELECT * FROM benchmark_runs WHERE idempotency_key=?", (idempotency_key,)).fetchone()
            if duplicate:
                return self._run(duplicate)
            suite = connection.execute("SELECT case_count FROM benchmark_suites WHERE suite_id=?", (suite_id,)).fetchone()
            if suite is None:
                raise BenchmarkError("评测 Suite 不存在", code="suite_not_found", status_code=404)
            connection.execute(
                "INSERT INTO benchmark_runs(run_id, suite_id, mode, status, idempotency_key, snapshot_json, metrics_json, "
                "progress_completed, progress_total, error, version, created_at, updated_at, schema_version) "
                "VALUES (?, ?, ?, 'pending', ?, ?, '{}', 0, ?, NULL, 1, ?, ?, 'benchmark_run_v1')",
                (run_id, suite_id, mode, idempotency_key, _json(payload.get("snapshot") or {}), int(suite["case_count"]), now, now),
            )
            self._audit(connection, "run_created", {"suite_id": suite_id, "mode": mode}, run_id=run_id, operator=str(payload.get("operator") or "system"), created_at=now)
        return self.get_run(run_id)

    def get_run(self, run_id: str) -> dict[str, Any]:
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM benchmark_runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            raise BenchmarkError("Benchmark Run 不存在", code="run_not_found", status_code=404)
        return self._run(row)

    def list_runs(self, *, page: int = 1, page_size: int = 50, status: str | None = None) -> dict[str, Any]:
        limit, offset = self._page(page, page_size)
        if status and status not in RUN_STATUSES:
            raise BenchmarkError("Run 状态无效")
        where = " WHERE status=?" if status else ""
        params: list[Any] = [status] if status else []
        with self.database.connect() as connection:
            total = int(connection.execute("SELECT COUNT(*) FROM benchmark_runs" + where, params).fetchone()[0])
            rows = connection.execute(
                "SELECT * FROM benchmark_runs" + where + " ORDER BY created_at DESC, run_id LIMIT ? OFFSET ?",
                [*params, limit, offset],
            ).fetchall()
        return {"schema_version": "benchmark_run_list_v1", "items": [self._run(row) for row in rows], "pagination": {"page": page, "page_size": page_size, "total": total}}

    def update_run(
        self,
        run_id: str,
        *,
        status: str | None = None,
        progress_completed: int | None = None,
        metrics: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        if status is not None and status not in RUN_STATUSES:
            raise BenchmarkError("Run 状态无效")
        now = self.clock()
        with self.database.transaction() as connection:
            row = connection.execute("SELECT * FROM benchmark_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                raise BenchmarkError("Benchmark Run 不存在", code="run_not_found", status_code=404)
            next_status = status or row["status"]
            completed = int(row["progress_completed"] if progress_completed is None else progress_completed)
            if completed < 0 or completed > int(row["progress_total"]):
                raise BenchmarkError("Run 进度无效")
            connection.execute(
                "UPDATE benchmark_runs SET status=?, progress_completed=?, metrics_json=?, error=?, version=version+1, updated_at=? WHERE run_id=?",
                (next_status, completed, _json(metrics if metrics is not None else _object(row["metrics_json"], {})), error, now, run_id),
            )
            self._audit(connection, "run_status_changed", {"from": row["status"], "to": next_status, "progress_completed": completed}, run_id=run_id, created_at=now)
        return self.get_run(run_id)

    def add_case_result(self, run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        case_id = str(payload.get("case_id") or "").strip()
        if not case_id:
            raise BenchmarkError("Case ID 不能为空")
        self.get_run(run_id)
        now = self.clock()
        result_id = str(payload.get("result_id") or f"case-result-{uuid.uuid4().hex}")
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO benchmark_case_results(result_id, run_id, case_id, passed, json_valid, schema_valid, "
                "template_reference_ok, duration_ms, error_type, result_json, created_at, schema_version) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'benchmark_case_result_v1') "
                "ON CONFLICT(run_id, case_id) DO UPDATE SET passed=excluded.passed, json_valid=excluded.json_valid, "
                "schema_valid=excluded.schema_valid, template_reference_ok=excluded.template_reference_ok, "
                "duration_ms=excluded.duration_ms, error_type=excluded.error_type, result_json=excluded.result_json, created_at=excluded.created_at",
                (
                    result_id, run_id, case_id, int(bool(payload.get("passed"))), int(bool(payload.get("json_valid"))),
                    int(bool(payload.get("schema_valid"))), int(bool(payload.get("template_reference_ok"))),
                    float(payload.get("duration_ms") or 0), payload.get("error_type"), _json(payload.get("result") or {}), now,
                ),
            )
            self._audit(connection, "case_result_recorded", {"case_id": case_id, "passed": bool(payload.get("passed"))}, run_id=run_id, created_at=now)
        return self.list_case_results(run_id, page=1, page_size=100)["items"][-1]

    def list_case_results(self, run_id: str, *, page: int = 1, page_size: int = 50, passed: bool | None = None) -> dict[str, Any]:
        self.get_run(run_id)
        limit, offset = self._page(page, page_size)
        where = "run_id=?" + (" AND passed=?" if passed is not None else "")
        params: list[Any] = [run_id] + ([int(passed)] if passed is not None else [])
        with self.database.connect() as connection:
            total = int(connection.execute("SELECT COUNT(*) FROM benchmark_case_results WHERE " + where, params).fetchone()[0])
            rows = connection.execute(
                "SELECT * FROM benchmark_case_results WHERE " + where + " ORDER BY case_id LIMIT ? OFFSET ?",
                [*params, limit, offset],
            ).fetchall()
        items = [{
            "result_id": row["result_id"], "run_id": row["run_id"], "case_id": row["case_id"],
            "passed": bool(row["passed"]), "json_valid": bool(row["json_valid"]),
            "schema_valid": bool(row["schema_valid"]), "template_reference_ok": bool(row["template_reference_ok"]),
            "duration_ms": float(row["duration_ms"]), "error_type": row["error_type"],
            "result": _object(row["result_json"], {}), "created_at": row["created_at"], "schema_version": row["schema_version"],
        } for row in rows]
        return {"schema_version": "benchmark_case_result_list_v1", "items": items, "pagination": {"page": page, "page_size": page_size, "total": total}}

    def create_gate(self, payload: dict[str, Any]) -> dict[str, Any]:
        gate_id = str(payload.get("gate_id") or f"gate-{uuid.uuid4().hex}")
        decision = str(payload.get("decision") or "")
        if decision not in {"passed", "blocked", "manual_review"}:
            raise BenchmarkError("Gate 决策无效")
        baseline = self.get_run(str(payload.get("baseline_run_id") or ""))
        candidate = self.get_run(str(payload.get("candidate_run_id") or ""))
        now = self.clock()
        result = {
            "gate_id": gate_id,
            "baseline_run_id": baseline["run_id"],
            "candidate_run_id": candidate["run_id"],
            "decision": decision,
            "thresholds": payload.get("thresholds") or {},
            "deltas": payload.get("deltas") or {},
            "reasons": payload.get("reasons") or [],
            "operator": str(payload.get("operator") or "local-reviewer"),
            "created_at": now,
            "schema_version": "benchmark_gate_v1",
        }
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO benchmark_gates(gate_id, baseline_run_id, candidate_run_id, decision, thresholds_json, "
                "deltas_json, reasons_json, operator, created_at, schema_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'benchmark_gate_v1')",
                (gate_id, result["baseline_run_id"], result["candidate_run_id"], decision, _json(result["thresholds"]), _json(result["deltas"]), _json(result["reasons"]), result["operator"], now),
            )
            self._audit(connection, "gate_evaluated", {"gate_id": gate_id, "decision": decision}, run_id=candidate["run_id"], operator=result["operator"], created_at=now)
        return result

    def list_gates(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute("SELECT * FROM benchmark_gates ORDER BY created_at DESC, gate_id DESC").fetchall()
        return [{
            "gate_id": row["gate_id"], "baseline_run_id": row["baseline_run_id"], "candidate_run_id": row["candidate_run_id"],
            "decision": row["decision"], "thresholds": _object(row["thresholds_json"], {}), "deltas": _object(row["deltas_json"], {}),
            "reasons": _object(row["reasons_json"], []), "operator": row["operator"], "created_at": row["created_at"], "schema_version": row["schema_version"],
        } for row in rows]

    def list_artifacts(self, run_id: str) -> dict[str, Any]:
        self.get_run(run_id)
        with self.database.connect() as connection:
            rows = connection.execute("SELECT * FROM benchmark_artifacts WHERE run_id=? ORDER BY created_at, artifact_id", (run_id,)).fetchall()
        return {"schema_version": "benchmark_artifact_list_v1", "items": [dict(row) for row in rows]}

    def add_artifact(self, run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.get_run(run_id)
        artifact_id = str(payload.get("artifact_id") or f"artifact-{uuid.uuid4().hex}").strip()
        artifact_type = str(payload.get("artifact_type") or "").strip()
        path = str(payload.get("path") or "").strip()
        sha256 = str(payload.get("sha256") or "").strip().lower()
        try:
            size_bytes = int(payload.get("size_bytes") or 0)
        except (TypeError, ValueError) as exc:
            raise BenchmarkError("Artifact 大小无效") from exc
        if not artifact_id or not artifact_type or not path or size_bytes < 0:
            raise BenchmarkError("Artifact 参数无效")
        if len(sha256) != 64 or any(character not in "0123456789abcdef" for character in sha256):
            raise BenchmarkError("Artifact SHA256 无效")
        now = self.clock()
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO benchmark_artifacts(artifact_id, run_id, artifact_type, path, size_bytes, sha256, created_at, schema_version) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 'benchmark_artifact_v1')",
                (artifact_id, run_id, artifact_type, path, size_bytes, sha256, now),
            )
            self._audit(
                connection,
                "artifact_registered",
                {"artifact_id": artifact_id, "artifact_type": artifact_type, "path": path},
                run_id=run_id,
                created_at=now,
            )
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM benchmark_artifacts WHERE artifact_id=?", (artifact_id,)).fetchone()
        return dict(row)

    def list_audit_events(self, run_id: str | None = None) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            if run_id:
                rows = connection.execute("SELECT * FROM benchmark_audit_events WHERE run_id=? ORDER BY created_at, rowid", (run_id,)).fetchall()
            else:
                rows = connection.execute("SELECT * FROM benchmark_audit_events ORDER BY created_at, rowid").fetchall()
        return [dict(row) | {"event": _object(row["event_json"], {})} for row in rows]
