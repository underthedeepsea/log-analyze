from __future__ import annotations

import argparse
import json
import mimetypes
import os
import re
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.error import URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen

from logrisk.ai_harness.prompt_registry import PromptRegistry, PromptTemplate
from logrisk.ai_harness.trace_logger import AITraceLogger
from logrisk.approved_rules import ApprovedRuleError, ApprovedRuleStore
from logrisk.feature_extractor_ollama import DEFAULT_OLLAMA_URL, FEATURE_PROMPT_ID
from logrisk.feature_jobs import FeatureJobError, FeatureJobManager
from logrisk.input_parser import parse_log_content
from logrisk.processing_metrics import ProcessingMetricsError, ProcessingMetricsStore
from pipeline.manual_import_pipeline import analyze_records


MAX_UPLOAD_BYTES = 10 * 1024 * 1024
DEFAULT_MODEL = "qwen3:1.7b"


class DashboardHTTPServer(ThreadingHTTPServer):
    daemon_threads = True


def check_ollama(base_url: str, timeout: float = 2) -> dict[str, Any]:
    try:
        with urlopen(f"{base_url.rstrip('/')}/api/tags", timeout=timeout) as response:
            payload = json.load(response)
        models = [item.get("name") for item in payload.get("models", []) if item.get("name")]
        return {"online": True, "models": models}
    except (OSError, URLError, ValueError, json.JSONDecodeError) as exc:
        return {"online": False, "models": [], "error": str(exc)}


def build_server(
    host: str,
    port: int,
    manager: FeatureJobManager | None = None,
    frontend_path: str | Path | None = None,
    ollama_checker: Callable[[], dict[str, Any]] | None = None,
    default_model: str = DEFAULT_MODEL,
    default_ollama_url: str = DEFAULT_OLLAMA_URL,
    default_timeout: float = 120,
    input_analyzer: Callable[[list[dict[str, Any]]], dict[str, Any]] | None = None,
) -> DashboardHTTPServer:
    root = Path(__file__).resolve().parents[2]
    server = DashboardHTTPServer((host, port), DashboardHandler)
    state_root = root / "state"
    server.manager = manager or FeatureJobManager(  # type: ignore[attr-defined]
        rule_store=ApprovedRuleStore(state_root / "approved_rules.json"),
        metrics_store=ProcessingMetricsStore(state_root / "processing_metrics.json"),
    )
    server.frontend_path = Path(frontend_path or root / "frontend" / "dist" / "index.html")  # type: ignore[attr-defined]
    server.default_model = default_model  # type: ignore[attr-defined]
    server.default_ollama_url = default_ollama_url  # type: ignore[attr-defined]
    server.default_timeout = default_timeout  # type: ignore[attr-defined]
    server.prompt_registry = PromptRegistry(root / "prompts", root / "configs" / "ai_harness.yaml", state_root / "prompt_versions.json")  # type: ignore[attr-defined]
    server.trace_logger = AITraceLogger(state_root / "ai_traces.jsonl")  # type: ignore[attr-defined]
    server.ollama_checker = ollama_checker or (  # type: ignore[attr-defined]
        lambda: check_ollama(default_ollama_url)
    )
    analysis_lock = threading.Lock()

    def default_input_analyzer(records: list[dict[str, Any]]) -> dict[str, Any]:
        with analysis_lock:
            return analyze_records(
                records,
                config_path=str(root / "configs" / "drain3_recommended.ini"),
                rules_path=str(root / "configs" / "risk_rules.yaml"),
                state_dir=str(state_root / "dashboard_drain3"),
            )

    server.input_analyzer = input_analyzer or default_input_analyzer  # type: ignore[attr-defined]
    return server


class DashboardHandler(BaseHTTPRequestHandler):
    server: DashboardHTTPServer

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _json(self, status: int, payload: Any, headers: dict[str, str] | None = None) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> Any:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise FeatureJobError("Content-Length 无效") from exc
        if length > MAX_UPLOAD_BYTES:
            raise FeatureJobError("上传内容超过 10 MB")
        try:
            return json.loads(self.rfile.read(length) or b"{}")
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise FeatureJobError("请求体不是有效 JSON") from exc

    def _route_parts(self) -> tuple[str, dict[str, list[str]]]:
        parsed = urlparse(self.path)
        return parsed.path, parse_qs(parsed.query)

    def do_GET(self) -> None:
        path, query = self._route_parts()
        try:
            if path in {"/", "/prompts", "/ai-traces", "/ai-observability"}:
                self._serve_frontend()
                return
            if path.startswith("/assets/"):
                self._serve_asset(path)
                return
            if path == "/api/config":
                self._json(HTTPStatus.OK, {
                    "default_model": self.server.default_model,  # type: ignore[attr-defined]
                    "default_ollama_url": self.server.default_ollama_url,  # type: ignore[attr-defined]
                    "default_timeout": self.server.default_timeout,  # type: ignore[attr-defined]
                    "max_upload_bytes": MAX_UPLOAD_BYTES,
                })
                return
            if path == "/api/ollama/status":
                self._json(HTTPStatus.OK, self.server.ollama_checker())  # type: ignore[attr-defined]
                return
            if path == "/api/rules":
                self._json(HTTPStatus.OK, {"rules": self.server.manager.list_rules()})  # type: ignore[attr-defined]
                return
            if path == "/api/metrics":
                self._json(HTTPStatus.OK, self.server.manager.get_system_metrics())  # type: ignore[attr-defined]
                return
            if path == "/api/ai-harness/status":
                self._json(HTTPStatus.OK, self._ai_harness_status())
                return
            if path == "/api/ai-harness/observability/summary":
                self._json(HTTPStatus.OK, self._observability_summary())
                return
            if path == "/api/ai-harness/events/recent":
                self._json(HTTPStatus.OK, self._observability_recent_events(query))
                return
            match = re.fullmatch(r"/api/ai-harness/jobs/([a-f0-9]+)/progress", path)
            if match:
                self._json(HTTPStatus.OK, self._observability_progress(match.group(1)))
                return
            match = re.fullmatch(r"/api/ai-harness/jobs/([a-f0-9]+)/events", path)
            if match:
                self._json(HTTPStatus.OK, self._observability_events(match.group(1), query))
                return
            if path == "/api/ai-harness/prompts":
                self._json(HTTPStatus.OK, self._prompt_list())
                return
            match = re.fullmatch(r"/api/ai-harness/prompts/([A-Za-z0-9_-]+)", path)
            if match:
                self._json(HTTPStatus.OK, self._prompt_detail(match.group(1)))
                return
            if path == "/api/ai-harness/traces":
                self._json(HTTPStatus.OK, self._trace_list(query))
                return
            match = re.fullmatch(r"/api/ai-harness/traces/([A-Za-z0-9_-]+)", path)
            if match:
                trace = self.server.trace_logger.get_trace(match.group(1))  # type: ignore[attr-defined]
                if not trace:
                    self._json(HTTPStatus.NOT_FOUND, {"error": "Trace 不存在"})
                    return
                self._json(HTTPStatus.OK, self._with_prompt_content(trace))
                return
            match = re.fullmatch(r"/api/jobs/([a-f0-9]+)", path)
            if match:
                self._json(HTTPStatus.OK, self.server.manager.get_job(match.group(1)))  # type: ignore[attr-defined]
                return
            match = re.fullmatch(r"/api/jobs/([a-f0-9]+)/events", path)
            if match:
                cursor = int(query.get("cursor", ["0"])[0])
                self._serve_events(match.group(1), max(0, cursor))
                return
            self._json(HTTPStatus.NOT_FOUND, {"error": "资源不存在"})
        except (FeatureJobError, ApprovedRuleError, ProcessingMetricsError) as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except (TypeError, ValueError) as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    def do_POST(self) -> None:
        path, _ = self._route_parts()
        try:
            payload = self._read_json()
            if path == "/api/inputs/analyze":
                if not isinstance(payload, dict):
                    raise FeatureJobError("请求体必须是 JSON object")
                filename = payload.get("filename")
                content = payload.get("content")
                if not isinstance(filename, str) or not filename.strip():
                    raise FeatureJobError("上传文件名无效")
                if not isinstance(content, str):
                    raise FeatureJobError("上传内容必须是 UTF-8 文本")
                records = parse_log_content(filename.strip(), content)
                result = self.server.input_analyzer(records)  # type: ignore[attr-defined]
                self._json(HTTPStatus.OK, {"result": result})
                return
            if path == "/api/jobs":
                if not isinstance(payload, dict):
                    raise FeatureJobError("请求体必须是 JSON object")
                job_id = self.server.manager.create_job(  # type: ignore[attr-defined]
                    payload.get("result"),
                    model=payload.get("model") or self.server.default_model,  # type: ignore[attr-defined]
                    min_score=float(payload.get("min_score", 40)),
                    base_url=payload.get("ollama_url") or self.server.default_ollama_url,  # type: ignore[attr-defined]
                    timeout=float(payload.get("timeout", self.server.default_timeout)),  # type: ignore[attr-defined]
                    prompt_id=payload.get("prompt_id") or FEATURE_PROMPT_ID,
                )
                self._json(HTTPStatus.ACCEPTED, {"job_id": job_id})
                return
            match = re.fullmatch(r"/api/jobs/([a-f0-9]+)/entities/([^/]+)/retry", path)
            if match:
                self.server.manager.retry_entity(match.group(1), match.group(2))  # type: ignore[attr-defined]
                self._json(HTTPStatus.ACCEPTED, {"status": "queued"})
                return
            match = re.fullmatch(r"/api/jobs/([a-f0-9]+)/export", path)
            if match:
                package = self.server.manager.export_approved(match.group(1))  # type: ignore[attr-defined]
                self._json(
                    HTTPStatus.OK,
                    package,
                    {"Content-Disposition": 'attachment; filename="logrisk-feature-package.json"'},
                )
                return
            self._json(HTTPStatus.NOT_FOUND, {"error": "资源不存在"})
        except (FeatureJobError, ApprovedRuleError, ProcessingMetricsError) as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except (TypeError, ValueError) as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    def do_PATCH(self) -> None:
        path, _ = self._route_parts()
        try:
            payload = self._read_json()
            match = re.fullmatch(r"/api/ai-harness/prompts/([A-Za-z0-9_-]+)", path)
            if match:
                if not isinstance(payload, dict):
                    raise FeatureJobError("请求体必须是 JSON object")
                content = payload.get("content")
                if not isinstance(content, str) or not content.strip():
                    raise FeatureJobError("Prompt 内容不能为空")
                self.server.prompt_registry.update(match.group(1), content, str(payload.get("note") or ""))  # type: ignore[attr-defined]
                self._json(HTTPStatus.OK, self._prompt_detail(match.group(1)))
                return
            match = re.fullmatch(r"/api/jobs/([a-f0-9]+)/features/([A-Za-z0-9_-]+)", path)
            if not match:
                self._json(HTTPStatus.NOT_FOUND, {"error": "资源不存在"})
                return
            feature = self.server.manager.update_feature(match.group(1), match.group(2), payload)  # type: ignore[attr-defined]
            self._json(HTTPStatus.OK, feature)
        except (FeatureJobError, ApprovedRuleError, ProcessingMetricsError) as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    def _serve_frontend(self) -> None:
        path = self.server.frontend_path  # type: ignore[attr-defined]
        if not path.is_file():
            self._json(HTTPStatus.NOT_FOUND, {"error": "前端文件不存在"})
            return
        body = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _prompt_meta(self, prompt: PromptTemplate) -> dict[str, Any]:
        traces = self.server.trace_logger.list_traces(prompt_id=prompt.prompt_id, limit=200)  # type: ignore[attr-defined]
        return {
            "prompt_id": prompt.prompt_id,
            "display_name": prompt.display_name or prompt.prompt_id,
            "description": prompt.description or "",
            "analysis_type": prompt.analysis_type,
            "status": prompt.status,
            "is_default": prompt.is_default,
            "prompt_hash": prompt.sha256,
            "path": prompt.path,
            "version": prompt.version,
            "used_by_models": sorted({str(item.get("model")) for item in traces if item.get("model")}),
            "last_used_at": traces[0].get("created_at") if traces else None,
            "created_at": None,
            "updated_at": None,
        }

    def _prompt_list(self) -> dict[str, Any]:
        prompts = self.server.prompt_registry.list_prompts()  # type: ignore[attr-defined]
        return {"current_prompt_id": FEATURE_PROMPT_ID, "items": [self._prompt_meta(prompt) for prompt in prompts]}

    def _prompt_detail(self, prompt_id: str) -> dict[str, Any]:
        prompt = self.server.prompt_registry.load(prompt_id)  # type: ignore[attr-defined]
        detail = self._prompt_meta(prompt)
        detail["content"] = prompt.content
        detail["history"] = self.server.prompt_registry.history(prompt_id)  # type: ignore[attr-defined]
        detail["recent_traces"] = self._compact_traces(self.server.trace_logger.list_traces(prompt_id=prompt_id, limit=10))  # type: ignore[attr-defined]
        return detail

    def _ai_harness_status(self) -> dict[str, Any]:
        summary = self.server.trace_logger.summary_today()  # type: ignore[attr-defined]
        return {
            "trace_enabled": self.server.trace_logger.enabled,  # type: ignore[attr-defined]
            "trace_path": str(self.server.trace_logger.path),  # type: ignore[attr-defined]
            "current_prompt_id": FEATURE_PROMPT_ID,
            **summary,
        }

    def _compact_traces(self, traces: list[dict[str, Any]]) -> list[dict[str, Any]]:
        fields = ("trace_id", "job_id", "entity_type", "entity_id", "prompt_id", "prompt_hash", "provider", "model", "status", "latency_ms", "created_at", "evaluator_result")
        return [{key: item.get(key) for key in fields} for item in traces]

    def _trace_list(self, query: dict[str, list[str]]) -> dict[str, Any]:
        limit = int(query.get("limit", ["50"])[0])
        traces = self.server.trace_logger.list_traces(  # type: ignore[attr-defined]
            job_id=query.get("job_id", [None])[0],
            trace_id=query.get("trace_id", [None])[0],
            status=query.get("status", [None])[0],
            prompt_id=query.get("prompt_id", [None])[0],
            limit=limit,
        )
        return {"items": self._compact_traces(traces)}

    def _with_prompt_content(self, trace: dict[str, Any]) -> dict[str, Any]:
        result = dict(trace)
        prompt_id = str(trace.get("prompt_id") or FEATURE_PROMPT_ID)
        try:
            prompt = self.server.prompt_registry.load(prompt_id)  # type: ignore[attr-defined]
            result.setdefault("prompt_path", prompt.path)
            result["prompt_content"] = prompt.content
        except FileNotFoundError:
            result["prompt_content"] = ""
        return result

    def _feature_status_for_entity(self, snapshot: dict[str, Any], entity: dict[str, Any], trace: dict[str, Any] | None = None) -> dict[str, Any]:
        features = [
            feature for feature in snapshot.get("features", [])
            if feature.get("entity", {}).get("id") == entity.get("entity_id")
        ]
        trace_id = next((feature.get("trace_id") for feature in features if feature.get("trace_id")), None)
        trace_id = trace_id or (trace or {}).get("trace_id")
        evaluator_result = next((feature.get("evaluator_result") for feature in features if feature.get("evaluator_result")), None) or (trace or {}).get("evaluator_result")
        evaluator_status = "skipped"
        if evaluator_result:
            evaluator_status = "failed" if not evaluator_result.get("passed") else ("warning" if evaluator_result.get("warnings") else "passed")
        result = {
            "status": "pending",
            "candidate_count": len(features),
            "failure_reason": None,
            "trace_id": trace_id,
            "model_status": "pending",
            "parse_status": "skipped",
            "schema_status": "skipped",
            "evaluator_status": evaluator_status,
            "evaluator_result": evaluator_result,
        }
        if entity.get("status") == "rule_matched":
            result.update({"status": "reused_rule", "failure_reason": "命中历史规则，跳过 LLM", "model_status": "skipped"})
            return result
        if entity.get("status") == "running":
            result.update({"status": "model_running", "model_status": "running"})
            return result
        if entity.get("status") == "failed":
            reason = str(entity.get("error") or "模型服务返回错误，AI 分析失败。")
            lowered = reason.lower()
            status = "evaluator_failed" if evaluator_status == "failed" or reason.startswith("Evaluator") else ("model_timeout" if "timeout" in lowered or "超时" in reason else ("parse_failed" if "json" in lowered or "解析" in reason else "model_failed"))
            result.update({
                "status": status,
                "failure_reason": self._evaluator_reason(evaluator_result) or reason,
                "model_status": "success" if status == "evaluator_failed" else ("timeout" if status == "model_timeout" else "failed"),
                "parse_status": "passed" if status == "evaluator_failed" else ("failed" if status == "parse_failed" else "skipped"),
                "schema_status": "passed" if status == "evaluator_failed" else "skipped",
            })
            return result
        if entity.get("status") in {"queued", "skipped"}:
            result.update({"status": "pending" if entity.get("status") == "queued" else "skipped", "model_status": "pending" if entity.get("status") == "queued" else "skipped"})
            return result
        if not features:
            result.update({"status": "no_feature", "candidate_count": 0, "failure_reason": "AI 正常完成，但未识别到关键特征", "model_status": "success", "parse_status": "passed", "schema_status": "passed"})
            return result
        if any(feature.get("status") == "approved" for feature in features):
            result.update({"status": "approved"})
        if all(feature.get("status") == "rejected" for feature in features):
            result.update({"status": "rejected", "failure_reason": "候选特征已被人工驳回"})
        if result["status"] == "pending":
            result["status"] = "waiting_review"
        result.update({"model_status": "success", "parse_status": "passed", "schema_status": "passed"})
        return result

    def _evaluator_reason(self, evaluator_result: dict[str, Any] | None) -> str | None:
        errors = evaluator_result.get("errors") if isinstance(evaluator_result, dict) else None
        if not errors:
            return None
        suffix = f"；另有 {len(errors) - 1} 个错误" if len(errors) > 1 else ""
        return str(errors[0]) + suffix

    def _evaluator_stats(self, traces: list[dict[str, Any]], features: list[dict[str, Any]]) -> dict[str, Any]:
        source = traces if traces else features
        results = [
            item.get("evaluator_result")
            for item in source
            if isinstance(item.get("evaluator_result"), dict)
            and (not traces or item.get("status") in {"success", "evaluator_failed"})
        ]
        total = len(results)
        failed_results = [result for result in results if not result.get("passed")]
        errors = [str(error) for result in failed_results for error in result.get("errors", [])]
        return {
            "total": total,
            "passed": sum(bool(result.get("passed")) for result in results),
            "failed": len(failed_results),
            "warnings": sum(1 for result in results if result.get("warnings")),
            "pass_rate": round(sum(bool(result.get("passed")) for result in results) / total, 4) if total else 0,
            "evidence_reference_errors": sum("template_hash" in error or "component" in error or "entity" in error for error in errors),
            "forbidden_claim_errors": sum("禁止表达" in error or "RCA" in error or "建议" in error for error in errors),
        }

    def _observability_progress(self, job_id: str) -> dict[str, Any]:
        snapshot = self.server.manager.get_job(job_id)  # type: ignore[attr-defined]
        traces = self.server.trace_logger.list_traces(job_id=job_id, limit=200)  # type: ignore[attr-defined]
        trace_by_entity = {
            str(trace.get("entity_id")): trace
            for trace in traces
            if trace.get("entity_id")
        }
        evaluator = self._evaluator_stats(traces, snapshot.get("features", []))
        entities = []
        counts = {
            "risk_entities_total": len(snapshot.get("entities", [])),
            "rule_reused": 0,
            "ai_required": 0,
            "model_success": 0,
            "parse_success": 0,
            "schema_passed": 0,
            "evaluator_passed": 0,
            "candidate_features": len(snapshot.get("features", [])),
            "approved_rules": sum(feature.get("status") == "approved" for feature in snapshot.get("features", [])),
            "failed": 0,
            "no_feature": 0,
            "evaluator_total": evaluator["total"],
            "evaluator_passed": evaluator["passed"],
            "evaluator_failed": evaluator["failed"],
            "evidence_reference_error_count": evaluator["evidence_reference_errors"],
            "forbidden_claim_count": evaluator["forbidden_claim_errors"],
        }
        for entity in snapshot.get("entities", []):
            state = self._feature_status_for_entity(snapshot, entity, trace_by_entity.get(str(entity.get("entity_id"))))
            status = state["status"]
            counts["rule_reused"] += status == "reused_rule"
            counts["ai_required"] += status != "reused_rule" and status != "skipped"
            counts["model_success"] += status in {"no_feature", "waiting_review", "candidate_generated", "approved", "rejected"}
            counts["parse_success"] += status not in {"model_timeout", "model_failed", "parse_failed", "pending", "skipped"}
            counts["schema_passed"] += status not in {"model_timeout", "model_failed", "parse_failed", "schema_failed", "pending", "skipped"}
            counts["failed"] += status in {"model_timeout", "model_failed", "parse_failed", "schema_failed", "evaluator_failed"}
            counts["no_feature"] += status == "no_feature"
            entities.append({
                "entity_type": entity.get("entity_type"),
                "entity_id": entity.get("entity_id"),
                "risk_score": entity.get("risk_score"),
                "reused_rule": status == "reused_rule",
                "status": "candidate_generated" if status == "waiting_review" and state["candidate_count"] else status,
                "trace_id": state["trace_id"],
                "candidate_count": state["candidate_count"],
                "failure_reason": state["failure_reason"],
                "model_status": state["model_status"],
                "parse_status": state["parse_status"],
                "schema_status": state["schema_status"],
                "evaluator_status": state["evaluator_status"],
                "evaluator_result": state["evaluator_result"],
            })
        current = next((item for item in entities if item["status"] in {"model_running", "pending"}), None)
        processed = sum(item["status"] not in {"pending", "skipped"} for item in entities)
        total = max(1, counts["ai_required"])
        return {
            "job_id": snapshot["job_id"],
            "status": "partial_failed" if snapshot["status"] == "completed_with_errors" else snapshot["status"],
            "created_at": snapshot["created_at"],
            "updated_at": snapshot.get("completed_at"),
            "model": snapshot["model"],
            "prompt_id": snapshot["prompt_id"],
            "source_file": snapshot.get("source_summary", {}).get("source_file"),
            "current_stage": "model_call" if current else "candidate_features",
            "current_message": f"正在分析第 {min(processed + 1, total)} / {total} 个风险实体" if current else "AI 分析已结束，等待人工审批或规则沉淀",
            "summary": counts,
            "entities": entities,
        }

    def _observability_summary(self) -> dict[str, Any]:
        jobs = self.server.manager.list_jobs()  # type: ignore[attr-defined]
        current = next((job for job in jobs if job["status"] in {"queued", "running"}), jobs[0] if jobs else None)
        progress = self._observability_progress(current["job_id"]) if current else {"summary": {}, "entities": []}
        summary = progress.get("summary", {})
        failed = sum(item.get("status") in {"model_timeout", "model_failed", "parse_failed", "schema_failed", "evaluator_failed"} for item in progress.get("entities", []))
        success = int(summary.get("model_success") or 0)
        total_ai = int(summary.get("ai_required") or 0)
        return {
            "running_jobs": sum(job["status"] in {"queued", "running"} for job in jobs),
            "current_job_id": current["job_id"] if current else None,
            "today_ai_calls": self.server.trace_logger.summary_today().get("today_calls", 0),  # type: ignore[attr-defined]
            "ai_required": total_ai,
            "model_success_rate": round(success / total_ai, 4) if total_ai else 0,
            "candidate_feature_count": int(summary.get("candidate_features") or 0),
            "schema_failed_count": failed,
            "evaluator_failed_count": int(summary.get("evaluator_failed") or 0),
            "no_feature_count": int(summary.get("no_feature") or 0),
            "evaluator": {
                "total": int(summary.get("evaluator_total") or 0),
                "passed": int(summary.get("evaluator_passed") or 0),
                "failed": int(summary.get("evaluator_failed") or 0),
                "warnings": sum(1 for item in progress.get("entities", []) if item.get("evaluator_status") == "warning"),
                "pass_rate": round((int(summary.get("evaluator_passed") or 0) / int(summary.get("evaluator_total") or 1)), 4) if int(summary.get("evaluator_total") or 0) else 0,
                "evidence_reference_errors": int(summary.get("evidence_reference_error_count") or 0),
                "forbidden_claim_errors": int(summary.get("forbidden_claim_count") or 0),
            },
        }

    def _observability_event(self, event: dict[str, Any]) -> dict[str, Any]:
        event_type = str(event.get("type") or "")
        stage_map = {
            "job_created": ("job_created", "success", "任务已创建"),
            "job_started": ("entity_filter", "running", "任务开始"),
            "entity_rule_matched": ("rule_reuse", "success", "命中历史规则，跳过 LLM"),
            "entity_started": ("model_call", "running", "开始调用模型"),
            "entity_completed": ("feature_generation", "success", f"生成 {event.get('feature_count', 0)} 个候选特征"),
            "entity_failed": ("evaluator" if str(event.get("error") or "").startswith("Evaluator") else "model_call", "failed", str(event.get("error") or "AI 分析失败")),
            "feature_updated": ("manual_review", "success", f"人工审批更新为 {event.get('status')}"),
            "job_completed": ("rule_persist", "success", "任务完成"),
            "entity_queued": ("retry", "running", "实体已重新排队"),
        }
        stage, status, message = stage_map.get(event_type, ("unknown", "success", event_type))
        return {
            "event_id": str(event.get("sequence", "")),
            "job_id": event.get("job_id"),
            "entity_id": event.get("entity_id"),
            "entity_type": event.get("entity_type"),
            "stage": stage,
            "status": status,
            "message": message,
            "trace_id": event.get("trace_id"),
            "created_at": event.get("timestamp"),
            "extra": {key: value for key, value in event.items() if key not in {"sequence", "type", "timestamp", "job_id", "entity_id", "entity_type", "trace_id"}},
        }

    def _observability_events(self, job_id: str, query: dict[str, list[str]]) -> dict[str, Any]:
        limit = int(query.get("limit", ["100"])[0])
        events = self.server.manager.list_events(job_id, limit=limit)  # type: ignore[attr-defined]
        return {"items": [self._observability_event(event) for event in reversed(events)]}

    def _observability_recent_events(self, query: dict[str, list[str]]) -> dict[str, Any]:
        limit = int(query.get("limit", ["100"])[0])
        items = []
        for job in self.server.manager.list_jobs():  # type: ignore[attr-defined]
            items.extend(self.server.manager.list_events(job["job_id"], limit=limit))  # type: ignore[attr-defined]
        items.sort(key=lambda event: str(event.get("timestamp") or ""), reverse=True)
        return {"items": [self._observability_event(event) for event in items[: max(1, min(limit, 200))]]}

    def _serve_asset(self, request_path: str) -> None:
        asset_root = (self.server.frontend_path.parent / "assets").resolve()  # type: ignore[attr-defined]
        relative = request_path.removeprefix("/assets/")
        target = (asset_root / relative).resolve()
        try:
            target.relative_to(asset_root)
        except ValueError:
            self._json(HTTPStatus.NOT_FOUND, {"error": "资源不存在"})
            return
        if not target.is_file():
            self._json(HTTPStatus.NOT_FOUND, {"error": "资源不存在"})
            return
        body = target.read_bytes()
        content_type = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
        if content_type.startswith("text/") or content_type in {"application/javascript", "application/json"}:
            content_type += "; charset=utf-8"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _serve_events(self, job_id: str, cursor: int) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()
        while True:
            events, cursor = self.server.manager.wait_for_events(job_id, cursor, timeout=15)  # type: ignore[attr-defined]
            try:
                if not events:
                    self.wfile.write(b": heartbeat\n\n")
                for event in events:
                    data = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
                    self.wfile.write(f"id: {event['sequence']}\nevent: {event['type']}\ndata: {data}\n\n".encode("utf-8"))
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                return
            if self.server.manager.get_job(job_id)["status"] in {"completed", "completed_with_errors"}:  # type: ignore[attr-defined]
                return


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="启动日志特征人工审批 Dashboard")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--model", default=os.getenv("OLLAMA_MODEL", DEFAULT_MODEL))
    parser.add_argument("--ollama-url", default=os.getenv("OLLAMA_HOST", DEFAULT_OLLAMA_URL))
    parser.add_argument("--ollama-timeout", type=float, default=float(os.getenv("OLLAMA_TIMEOUT", "120")))
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    server = build_server(
        args.host,
        args.port,
        default_model=args.model,
        default_ollama_url=args.ollama_url,
        default_timeout=args.ollama_timeout,
    )
    print(f"Feature review dashboard: http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
