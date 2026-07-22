from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any, Callable

from logrisk.ai_eval.runner import mock_extractor
from logrisk.benchmark_center.gates import evaluate_regression_gate
from logrisk.benchmark_center.repository import BenchmarkError, BenchmarkRepository
from logrisk.benchmark_center.runner import execute_cases


class BenchmarkService:
    def __init__(
        self,
        repository: BenchmarkRepository,
        *,
        canonical_cases: list[dict[str, Any]] | None = None,
        real_extractor: Callable[..., list[dict[str, Any]]] | None = None,
        asset_inventory: Callable[[], dict[str, int]] | None = None,
        profile_resolver: Callable[[str], dict[str, Any]] | None = None,
        connection_resolver: Callable[[str], dict[str, Any]] | None = None,
        prompt_resolver: Callable[[str], dict[str, Any]] | None = None,
    ) -> None:
        self.repository = repository
        self.real_extractor = real_extractor
        self.asset_inventory = asset_inventory or (lambda: {})
        self.profile_resolver = profile_resolver
        self.connection_resolver = connection_resolver
        self.prompt_resolver = prompt_resolver
        if canonical_cases is not None:
            self._ensure_canonical_suite(canonical_cases)

    def _ensure_canonical_suite(self, cases: list[dict[str, Any]]) -> dict[str, Any]:
        digest = hashlib.sha256(json.dumps(cases, ensure_ascii=False, sort_keys=True).encode()).hexdigest()[:12]
        return self.repository.create_suite({
            "suite_id": f"suite-canonical-{digest}",
            "name": "Canonical Eval Cases",
            "source_type": "canonical",
            "cases": cases,
            "source_hash": digest,
            "operator": "system-seed",
        })

    def list_suites(self, *, page: int = 1, page_size: int = 50) -> dict[str, Any]:
        return self.repository.list_suites(page=page, page_size=page_size)

    def create_suite(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = self.repository.create_suite(payload)
        return {**result, "request_id": f"request-{uuid.uuid4().hex}", "resource_id": result["suite_id"]}

    def list_runs(self, *, page: int = 1, page_size: int = 50, status: str | None = None) -> dict[str, Any]:
        return self.repository.list_runs(page=page, page_size=page_size, status=status)

    def create_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        suite = self.repository.get_suite(str(payload.get("suite_id") or ""))
        mode = str(payload.get("mode") or "fake")
        case_limit = int(payload.get("case_limit") or suite["case_count"])
        if case_limit < 1 or case_limit > suite["case_count"]:
            raise BenchmarkError("Case 数量超出 Suite 范围", status_code=422)
        retry_count = int(payload.get("retry_count") or 0)
        budget_units = int(payload.get("budget_units") or 0)
        if retry_count < 0:
            raise BenchmarkError("重试次数不能小于 0", status_code=422)
        if mode == "real":
            required = ("prompt_id", "model_profile_id", "timeout_seconds", "retry_count", "budget_units")
            if payload.get("confirmed") is not True:
                raise BenchmarkError("真实模型评测必须人工确认预算", code="confirmation_required", status_code=422)
            if any(payload.get(field) in (None, "") for field in required):
                raise BenchmarkError("真实模型评测缺少锁定配置", code="snapshot_incomplete", status_code=422)
            if budget_units < case_limit:
                raise BenchmarkError("真实模型预算不足以覆盖所选 Case", code="budget_too_small", status_code=422)
        profile_snapshot: dict[str, Any] | None = None
        connection_snapshot: dict[str, Any] | None = None
        prompt_snapshot: dict[str, Any] | None = None
        if mode == "real" and self.profile_resolver:
            try:
                profile_snapshot = self.profile_resolver(str(payload["model_profile_id"]))
                connection_id = str(profile_snapshot.get("connection_id") or "")
                if not connection_id or self.connection_resolver is None:
                    raise KeyError("connection_id")
                connection = self.connection_resolver(connection_id)
                connection_snapshot = {
                    key: connection.get(key)
                    for key in (
                        "connection_id", "display_name", "provider", "base_url", "api_key_env",
                        "timeout_seconds", "enabled", "is_default", "api_key_configured",
                    )
                    if key in connection
                }
                if not connection_snapshot.get("enabled"):
                    raise BenchmarkError(
                        "真实模型评测连接已停用",
                        code="connection_unavailable",
                        status_code=422,
                    )
                if connection_snapshot.get("provider") == "openai_compatible" and not connection_snapshot.get("api_key_configured"):
                    raise BenchmarkError(
                        "真实模型评测连接未配置 API Key 环境变量",
                        code="connection_unavailable",
                        status_code=422,
                    )
                if self.prompt_resolver is None:
                    raise KeyError("prompt_resolver")
                prompt_snapshot = self.prompt_resolver(str(payload["prompt_id"]))
            except BenchmarkError:
                raise
            except (KeyError, ValueError, FileNotFoundError) as exc:
                raise BenchmarkError("真实模型评测配置不存在或不可用", code="snapshot_unavailable", status_code=422) from exc
        snapshot = {
            "suite_version": suite["version"],
            "suite_source_hash": suite.get("source_hash"),
            "prompt_id": payload.get("prompt_id"),
            "prompt_hash": payload.get("prompt_hash"),
            "model_profile_id": payload.get("model_profile_id"),
            "connection_id": payload.get("connection_id"),
            "provider": payload.get("provider"),
            "model": payload.get("model"),
            "case_limit": case_limit,
            "timeout_seconds": float(payload.get("timeout_seconds") or 120),
            "retry_count": retry_count,
            "budget_units": budget_units,
            "mode": mode,
            "profile_snapshot": profile_snapshot,
            "connection_snapshot": connection_snapshot,
            "prompt_snapshot": prompt_snapshot,
        }
        result = self.repository.create_run({
            "run_id": payload.get("run_id") or f"run-{uuid.uuid4().hex}",
            "suite_id": suite["suite_id"],
            "mode": mode,
            "idempotency_key": payload.get("idempotency_key") or f"request-{uuid.uuid4().hex}",
            "snapshot": snapshot,
            "operator": str(payload.get("operator") or "local-operator"),
        })
        return {**result, "request_id": f"request-{uuid.uuid4().hex}", "resource_id": result["run_id"]}

    def _extractor(self, run: dict[str, Any]) -> tuple[Callable[..., list[dict[str, Any]]], dict[str, Any]]:
        if run["mode"] == "fake":
            return mock_extractor, {}
        if run["mode"] == "history":
            return (lambda _entity, *, case, **_kwargs: list(case.get("historical_features") or [])), {}
        if self.real_extractor is None:
            raise BenchmarkError("当前服务未配置真实模型评测执行器", code="real_executor_unavailable", status_code=422)
        snapshot = run["snapshot"]
        return self.real_extractor, {
            "prompt_id": snapshot.get("prompt_id"),
            "model_profile_id": snapshot.get("model_profile_id"),
            "profile_snapshot": snapshot.get("profile_snapshot"),
            "connection_snapshot": snapshot.get("connection_snapshot"),
            "prompt_snapshot": snapshot.get("prompt_snapshot"),
            "timeout": snapshot.get("timeout_seconds"),
            "cache_enabled": False,
            "job_id": "benchmark-" + run["run_id"],
        }

    def execute_run(self, run_id: str) -> dict[str, Any]:
        run = self.repository.get_run(run_id)
        if run["status"] in {"completed", "failed", "cancelled"}:
            return run

        def cancelled() -> bool:
            return self.repository.get_run(run_id)["status"] == "cancelled"

        def save_case(result: dict[str, Any], completed: int) -> None:
            self.repository.add_case_result(run_id, {
                "case_id": result["case_id"],
                "passed": result["passed"],
                "json_valid": result["json_valid"],
                "schema_valid": result["schema_valid"],
                "template_reference_ok": result["template_reference_ok"],
                "duration_ms": result["duration_ms"],
                "error_type": result.get("error_type"),
                "result": result,
            })
            if self.repository.get_run(run_id)["status"] != "cancelled":
                self.repository.update_run(run_id, status="running", progress_completed=completed)

        try:
            suite = self.repository.get_suite(run["suite_id"])
            cases = list(suite.get("cases") or [])[: int(run["snapshot"].get("case_limit") or suite["case_count"])]
            extractor, options = self._extractor(run)
            self.repository.update_run(run_id, status="running", progress_completed=0)
            execution = execute_cases(
                cases,
                extractor=extractor,
                extractor_options=options,
                on_result=save_case,
                should_cancel=cancelled,
                retry_count=int(run["snapshot"].get("retry_count") or 0) if run["mode"] == "real" else 0,
                max_calls=int(run["snapshot"].get("budget_units") or 0) if run["mode"] == "real" else None,
                count_model_calls=run["mode"] == "real",
            )
            if execution["cancelled"] or self.repository.get_run(run_id)["status"] == "cancelled":
                return self.repository.get_run(run_id)
            return self.repository.update_run(run_id, status="completed", progress_completed=len(cases), metrics=execution["metrics"])
        except Exception as exc:
            current = self.repository.get_run(run_id)
            if current["status"] == "cancelled":
                return current
            return self.repository.update_run(
                run_id,
                status="failed",
                progress_completed=current["progress"]["completed"],
                error=str(exc),
            )

    def cancel_run(self, run_id: str, *, operator: str = "local-operator") -> dict[str, Any]:
        run = self.repository.get_run(run_id)
        if run["status"] in {"completed", "failed", "cancelled"}:
            raise BenchmarkError("当前 Run 状态不可取消", code="invalid_transition", status_code=409)
        result = self.repository.update_run(run_id, status="cancelled", progress_completed=run["progress"]["completed"])
        return {**result, "request_id": f"request-{uuid.uuid4().hex}", "resource_id": run_id, "operator": operator}

    def get_run(self, run_id: str) -> dict[str, Any]:
        return {
            "schema_version": "benchmark_run_detail_v1",
            "run": self.repository.get_run(run_id),
            "cases": self.repository.list_case_results(run_id, page=1, page_size=100),
            "artifacts": self.repository.list_artifacts(run_id),
            "audit_events": self.repository.list_audit_events(run_id),
        }

    def compare(self, payload: dict[str, Any]) -> dict[str, Any]:
        baseline = self.repository.get_run(str(payload.get("baseline_run_id") or ""))
        candidate = self.repository.get_run(str(payload.get("candidate_run_id") or ""))
        evaluation = evaluate_regression_gate(baseline["metrics"], candidate["metrics"], payload.get("thresholds") or {})
        return {
            "schema_version": "benchmark_comparison_v1",
            "baseline": baseline,
            "candidate": candidate,
            **evaluation,
        }

    def evaluate_gate(self, payload: dict[str, Any]) -> dict[str, Any]:
        comparison = self.compare(payload)
        return self.repository.create_gate({
            "baseline_run_id": comparison["baseline"]["run_id"],
            "candidate_run_id": comparison["candidate"]["run_id"],
            "decision": comparison["decision"],
            "thresholds": comparison["thresholds"],
            "deltas": comparison["deltas"],
            "reasons": comparison["reasons"],
            "operator": str(payload.get("operator") or "local-reviewer"),
        })

    def overview(self) -> dict[str, Any]:
        runs = self.repository.list_runs(page=1, page_size=100)["items"]
        suites = self.repository.list_suites(page=1, page_size=100)["items"]
        gates = self.repository.list_gates()
        completed = [run for run in runs if run["status"] == "completed"]
        latest = completed[0] if completed else None
        gate_counts = {decision: sum(gate["decision"] == decision for gate in gates) for decision in ("passed", "blocked", "manual_review")}
        return {
            "schema_version": "benchmark_overview_v1",
            "suite_count": len(suites),
            "run_count": len(runs),
            "completed_run_count": len(completed),
            "latest_metrics": latest["metrics"] if latest else {},
            "latest_run_id": latest["run_id"] if latest else None,
            "gate_counts": gate_counts,
            "failure_count": sum(run["status"] == "failed" for run in runs),
            "source_assets": self.asset_inventory(),
        }

    def trends(self) -> dict[str, Any]:
        runs = self.repository.list_runs(page=1, page_size=100)["items"]
        items = [{"run_id": run["run_id"], "created_at": run["created_at"], "metrics": run["metrics"], "snapshot": run["snapshot"]} for run in reversed(runs) if run["status"] == "completed"]
        return {"schema_version": "benchmark_trend_v1", "items": items}

    def leaderboard(self) -> dict[str, Any]:
        runs = [run for run in self.repository.list_runs(page=1, page_size=100)["items"] if run["status"] == "completed"]
        items = sorted(({
            "run_id": run["run_id"],
            "model_profile_id": run["snapshot"].get("model_profile_id") or ("fake-model" if run["mode"] == "fake" else "history"),
            "prompt_id": run["snapshot"].get("prompt_id"),
            "pass_rate": float(run["metrics"].get("pass_rate") or 0),
            "schema_valid_rate": float(run["metrics"].get("schema_valid_rate") or 0),
            "latency_p95_ms": float(run["metrics"].get("latency_p95_ms") or 0),
        } for run in runs), key=lambda item: (-item["pass_rate"], item["latency_p95_ms"], item["run_id"]))
        return {"schema_version": "benchmark_leaderboard_v1", "items": items}
