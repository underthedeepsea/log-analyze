from __future__ import annotations

import argparse
import json
import mimetypes
import os
import re
import threading
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.error import URLError
from urllib.parse import parse_qs, unquote, urlparse
from urllib.request import Request, urlopen

from logrisk.ai_harness.connections import ConnectionStore
from logrisk.ai_harness.model_profile import ModelProfileRegistry
from logrisk.ai_harness.prompt_registry import PromptRegistry, PromptTemplate, SQLitePromptRegistry
from logrisk.ai_harness.providers import create_model_client
from logrisk.ai_harness.trace_logger import AITraceLogger
from logrisk.approved_rules import ApprovedRuleError, ApprovedRuleStore
from logrisk.drain_eval.schema import DrainQualityError
from logrisk.drain_eval.service import DrainQualityService
from logrisk.feature_extractor_ollama import DEFAULT_OLLAMA_URL, FEATURE_PROMPT_ID
from logrisk.feature_jobs import FeatureJobError, FeatureJobFileStore, FeatureJobManager, _cache_enabled_default
from logrisk.input_jobs import InputJobConfig, InputJobStore
from logrisk.input_parser import parse_log_content
from logrisk.large_file_pipeline import run_large_file_pipeline
from logrisk.processing_metrics import ProcessingMetricsError, ProcessingMetricsStore
from logrisk.rule_governance import RuleGovernanceError, RuleGovernanceRepository, RuleGovernanceService
from logrisk.node_risk import NodeRiskError, NodeRiskService
from logrisk.risk_semantics import RiskSemanticError, RiskSemanticService, validate_rule
from logrisk.semantic.schema import SemanticValidationError
from logrisk.semantic.store import SemanticDictionaryStore
from logrisk.upload_sessions import UploadConfig, UploadSessionStore
from logrisk.database import SQLiteDatabase
from logrisk.legacy_import import LegacyStateImporter
from logrisk.sqlite_stores import (
    SQLiteAICache,
    SQLiteAITraceLogger,
    SQLiteApprovedRuleStore,
    SQLiteDrainQualityService,
    SQLiteFeatureJobStore,
    SQLiteInputJobStore,
    SQLiteProcessingMetricsStore,
    SQLiteSemanticDictionaryStore,
    SQLiteUploadSessionStore,
)
from pipeline.manual_import_pipeline import analyze_records


MAX_UPLOAD_BYTES = 10 * 1024 * 1024
MAX_LARGE_UPLOAD_BYTES = 500 * 1024 * 1024
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


def check_model_connection(connection: dict[str, Any]) -> dict[str, Any]:
    if not connection.get("enabled"):
        return {"online": False, "models": [], "error": "连接已停用"}
    if connection.get("provider") == "ollama":
        return check_ollama(str(connection["base_url"]), float(connection.get("timeout_seconds") or 2))
    api_key_env = str(connection.get("api_key_env") or "")
    api_key = os.environ.get(api_key_env)
    if not api_key:
        return {"online": False, "models": [], "error": f"未配置环境变量: {api_key_env}"}
    request = Request(
        f"{str(connection['base_url']).rstrip('/')}/models",
        headers={"Accept": "application/json", "Authorization": f"Bearer {api_key}"},
    )
    try:
        with urlopen(request, timeout=float(connection.get("timeout_seconds") or 10)) as response:
            payload = json.load(response)
        models = [item.get("id") for item in payload.get("data", []) if isinstance(item, dict) and item.get("id")]
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
    drain_quality_root: str | Path | None = None,
    semantic_root: str | Path | None = None,
    cors_origins: list[str] | tuple[str, ...] | set[str] | None = None,
    database_path: str | Path | None = None,
) -> DashboardHTTPServer:
    root = Path(__file__).resolve().parents[2]
    state_root = Path(database_path).parent if database_path else root / "state"
    database = SQLiteDatabase(database_path or os.getenv("LOGRISK_DB_PATH") or state_root / "logrisk.sqlite3")
    connections = ConnectionStore(database)
    connections.seed_defaults(default_ollama_url)
    profiles = ModelProfileRegistry(root / "configs" / "model_profiles.yaml", database=database)
    prompts = SQLitePromptRegistry(database, root / "prompts", root / "configs" / "ai_harness.yaml")
    traces = SQLiteAITraceLogger(database)
    ai_cache = SQLiteAICache(database)
    drain_quality = (
        DrainQualityService(drain_quality_root, root / "configs" / "drain3_profiles", root / "configs" / "drain3_recommended.ini")
        if drain_quality_root else SQLiteDrainQualityService(database, root / "configs" / "drain3_profiles", root / "configs" / "drain3_recommended.ini")
    )
    semantic_dictionaries = (
        SemanticDictionaryStore(semantic_root, root / "configs" / "semantic_dictionary")
        if semantic_root else SQLiteSemanticDictionaryStore(database, root / "configs" / "semantic_dictionary")
    )
    risk_semantics = RiskSemanticService(database, root / "configs" / "risk_semantics" / "builtin.yaml")
    node_risks = NodeRiskService(database, root / "configs" / "node_risk.yaml")
    LegacyStateImporter(database, state_root, root / "output" / "uploads").run()

    def configured_extractor(entity: dict[str, Any], **kwargs: Any) -> list[dict[str, Any]]:
        import logrisk.feature_extractor_ollama as feature_extractor

        profile_snapshot = kwargs.get("profile_snapshot")
        profile = profiles.from_snapshot(profile_snapshot) if isinstance(profile_snapshot, dict) else profiles.get(kwargs.get("model_profile_id"))
        connection_snapshot = kwargs.get("connection_snapshot")
        connection = dict(connection_snapshot) if isinstance(connection_snapshot, dict) else connections.get(profile.connection_id)
        if not connection["enabled"]:
            raise FeatureJobError(f"模型连接已停用: {profile.connection_id}")
        feature_extractor.PROMPT_REGISTRY = prompts
        feature_extractor.TRACE_LOGGER = traces
        feature_extractor.AI_CACHE = ai_cache
        return feature_extractor.extract_features_for_entity(
            entity,
            model=profile.model,
            base_url=connection["base_url"],
            timeout=float(connection["timeout_seconds"]),
            model_client=create_model_client(connection),
            prompt_id=kwargs.get("prompt_id") or profile.default_prompt_id,
            job_id=kwargs.get("job_id"),
            cache_enabled=bool(kwargs.get("cache_enabled", True)),
            model_profile_id=profile.profile_id,
            provider=connection["provider"],
            model_profile=profile,
        )

    server = DashboardHTTPServer((host, port), DashboardHandler)
    server.database = database  # type: ignore[attr-defined]
    server.connections = connections  # type: ignore[attr-defined]
    rule_store = SQLiteApprovedRuleStore(database)
    if manager is not None and manager.rule_store is None:
        manager.rule_store = rule_store
    server.manager = manager or FeatureJobManager(  # type: ignore[attr-defined]
        extractor=configured_extractor,
        rule_store=rule_store,
        metrics_store=SQLiteProcessingMetricsStore(database),
        persistence=SQLiteFeatureJobStore(database),
    )
    server.rule_governance = RuleGovernanceService(RuleGovernanceRepository(database))  # type: ignore[attr-defined]
    server.frontend_path = Path(frontend_path or root / "frontend" / "dist" / "index.html")  # type: ignore[attr-defined]
    server.default_model = default_model  # type: ignore[attr-defined]
    server.default_ollama_url = default_ollama_url  # type: ignore[attr-defined]
    server.default_timeout = default_timeout  # type: ignore[attr-defined]
    server.prompt_registry = prompts  # type: ignore[attr-defined]
    server.model_profiles = profiles  # type: ignore[attr-defined]
    server.trace_logger = traces  # type: ignore[attr-defined]
    server.upload_store = SQLiteUploadSessionStore(UploadConfig(upload_dir=state_root / "uploads"), database)  # type: ignore[attr-defined]
    server.input_jobs = SQLiteInputJobStore(InputJobConfig(output_dir=root / "output" / "uploads"), database)  # type: ignore[attr-defined]
    server.drain_quality = drain_quality  # type: ignore[attr-defined]
    server.semantic_dictionaries = semantic_dictionaries  # type: ignore[attr-defined]
    server.risk_semantics = risk_semantics  # type: ignore[attr-defined]
    server.node_risks = node_risks  # type: ignore[attr-defined]
    configured_origins = cors_origins if cors_origins is not None else os.getenv("DASHBOARD_CORS_ORIGINS", "").split(",")
    server.cors_origins = {str(origin).strip().rstrip("/") for origin in configured_origins if str(origin).strip()}  # type: ignore[attr-defined]
    server.ollama_checker = ollama_checker or (  # type: ignore[attr-defined]
        lambda: check_ollama(default_ollama_url)
    )

    def govern_drain_result(result: dict[str, Any]) -> dict[str, Any]:
        def active_templates(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
            governed = [server.drain_quality.templates.apply_override(item) for item in items]  # type: ignore[attr-defined]
            return [item for item in governed if item.get("template_governance_status") not in {"ignored", "deleted"}]

        risk_levels: dict[str, set[str]] = {}
        for entity in result.get("risk_entities") or []:
            for template in entity.get("top_templates") or []:
                template_hash = str(template.get("template_hash") or "")
                if template_hash:
                    risk_levels.setdefault(template_hash, set()).add(str(entity.get("risk_level") or "unknown"))
        catalog_rows = []
        for template in result.get("top_templates") or []:
            if template.get("template_hash") and template.get("template"):
                catalog_rows.append(dict(template, risk_levels=sorted(risk_levels.get(str(template["template_hash"]), set()))))
        if catalog_rows:
            server.drain_quality.templates.import_templates(catalog_rows)  # type: ignore[attr-defined]
        governed = dict(result)
        governed["top_templates"] = active_templates(result.get("top_templates") or [])
        governed["risk_entities"] = [dict(entity, top_templates=active_templates(entity.get("top_templates") or [])) for entity in result.get("risk_entities") or []]
        return governed

    server.govern_drain_result = govern_drain_result  # type: ignore[attr-defined]
    analysis_lock = threading.Lock()

    def default_input_analyzer(
        records: list[dict[str, Any]],
        config_path: str | Path | None = None,
        semantic_snapshot: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with analysis_lock:
            return analyze_records(
                records,
                config_path=str(config_path or root / "configs" / "drain3_recommended.ini"),
                rules_path=str(root / "configs" / "risk_rules.yaml"),
                state_dir=str(state_root / "dashboard_drain3"),
                semantic_snapshot=semantic_snapshot,
                risk_semantics=risk_semantics,
                node_risks=node_risks,
            )

    server.input_analyzer = input_analyzer or default_input_analyzer  # type: ignore[attr-defined]
    server.input_analyzer_accepts_config = input_analyzer is None  # type: ignore[attr-defined]

    def run_input_job(input_job_id: str) -> None:
        store = server.input_jobs  # type: ignore[attr-defined]
        job = store.get_job(input_job_id)
        job.update({"status": "running", "stage": "reading", "started_at": _now()})
        store.write_job(input_job_id, job)
        try:
            result = run_large_file_pipeline(
                input_job_id=input_job_id,
                input_path=job["source_path"],
                filename=job["filename"],
                config_path=job.get("drain_config_path") or root / "configs" / "drain3_recommended.ini",
                rules_path=root / "configs" / "risk_rules.yaml",
                state_dir=state_root / "dashboard_drain3_large" / input_job_id,
                progress_callback=lambda progress: store.write_progress(input_job_id, progress),
                semantic_snapshot=job.get("semantic_dictionary_snapshot"),
                risk_semantics=risk_semantics,
                node_risks=node_risks,
            )
            result = server.govern_drain_result(result)  # type: ignore[attr-defined]
            store.write_result(input_job_id, result)
            job.update({"status": "completed", "stage": "completed", "completed_at": _now(), "error": None})
            store.write_job(input_job_id, job)
            store.write_progress(input_job_id, {"input_job_id": input_job_id, "status": "completed", "stage": "completed", "progress": 1.0, "risk_entities": len(result.get("risk_entities") or [])})
        except Exception as exc:
            job.update({"status": "failed", "stage": "failed", "completed_at": _now(), "error": str(exc)})
            store.write_job(input_job_id, job)
            store.write_progress(input_job_id, {"input_job_id": input_job_id, "status": "failed", "stage": "failed", "progress": 1.0, "error": str(exc)})

    server.run_input_job = run_input_job  # type: ignore[attr-defined]
    return server


def _now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


class DashboardHandler(BaseHTTPRequestHandler):
    server: DashboardHTTPServer

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _cors_headers(self) -> None:
        origin = self.headers.get("Origin", "").rstrip("/")
        if origin and origin in self.server.cors_origins:  # type: ignore[attr-defined]
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Chunk-SHA256")
            self.send_header("Vary", "Origin")

    def _json(self, status: int, payload: Any, headers: dict[str, str] | None = None) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self._cors_headers()
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

    def _read_bytes(self, max_bytes: int) -> bytes:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise FeatureJobError("Content-Length 无效") from exc
        if length > max_bytes:
            raise FeatureJobError("上传分片超过限制")
        return self.rfile.read(length)

    def _route_parts(self) -> tuple[str, dict[str, list[str]]]:
        parsed = urlparse(self.path)
        return parsed.path, parse_qs(parsed.query)

    def do_OPTIONS(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self._cors_headers()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:
        path, query = self._route_parts()
        try:
            if path in {"/", "/prompts", "/ai-traces", "/ai-observability", "/model-profiles", "/drain-quality", "/rules", "/settings", "/node-risks", "/semantic-library"} or path.startswith("/node-risks/"):
                self._serve_frontend()
                return
            if path == "/config.js":
                self._serve_frontend_file("config.js", "application/javascript; charset=utf-8")
                return
            if path.startswith("/assets/"):
                self._serve_asset(path)
                return
            if path == "/api/config":
                self._json(HTTPStatus.OK, {
                    "default_model": self.server.default_model,  # type: ignore[attr-defined]
                    "default_ollama_url": self.server.default_ollama_url,  # type: ignore[attr-defined]
                    "default_timeout": self.server.default_timeout,  # type: ignore[attr-defined]
                    "ai_cache_enabled": _cache_enabled_default(),
                    "max_upload_bytes": MAX_UPLOAD_BYTES,
                })
                return
            if path == "/api/node-risks":
                self._json(HTTPStatus.OK, self.server.node_risks.list_nodes(  # type: ignore[attr-defined]
                    cluster=query.get("cluster", [None])[0], level=query.get("level", [None])[0], domain=query.get("domain", [None])[0],
                    trend=query.get("trend", [None])[0], search=query.get("search", [None])[0],
                    active_only=query.get("status", [None])[0] == "active",
                    page=int(query.get("page", ["1"])[0]), page_size=int(query.get("page_size", ["50"])[0]),
                ))
                return
            if path == "/api/semantics":
                self._json(HTTPStatus.OK, {"schema_version": "risk_semantic_list_v1", "items": self.server.risk_semantics.list_rules()})  # type: ignore[attr-defined]
                return
            if path == "/api/semantics/effective":
                self._json(HTTPStatus.OK, {"schema_version": "risk_semantic_registry_v1", "items": self.server.risk_semantics.effective_rules()})  # type: ignore[attr-defined]
                return
            if path == "/api/semantics/export":
                self._json(HTTPStatus.OK, self.server.risk_semantics.export_bundle())  # type: ignore[attr-defined]
                return
            if path == "/api/semantics/unclassified":
                self._json(HTTPStatus.OK, {"items": self.server.risk_semantics.list_unclassified()})  # type: ignore[attr-defined]
                return
            match = re.fullmatch(r"/api/node-risks/([^/]+)/([^/]+)(?:/(events|timeline|daily|score-explanation))?", path)
            if match:
                cluster, node_id, subresource = (unquote(value) if value else value for value in match.groups())
                if subresource == "events":
                    result = self.server.node_risks.list_events(cluster, node_id, risk_type=query.get("risk_type", [None])[0], severity=query.get("severity", [None])[0], status=query.get("status", [None])[0], page=int(query.get("page", ["1"])[0]), page_size=int(query.get("page_size", ["50"])[0]))  # type: ignore[attr-defined]
                elif subresource == "timeline":
                    result = {"items": self.server.node_risks.timeline(cluster, node_id)}  # type: ignore[attr-defined]
                elif subresource == "daily":
                    result = {"items": self.server.node_risks.daily(cluster, node_id)}  # type: ignore[attr-defined]
                elif subresource == "score-explanation":
                    result = self.server.node_risks.get_node(cluster, node_id)["snapshot"]["score_breakdown"]  # type: ignore[attr-defined]
                else:
                    result = self.server.node_risks.get_node(cluster, node_id)  # type: ignore[attr-defined]
                self._json(HTTPStatus.OK, result)
                return
            match = re.fullmatch(r"/api/semantics/([^/]+)(?:/versions)?", path)
            if match:
                rule_id = unquote(match.group(1))
                result = self.server.risk_semantics.versions(rule_id) if path.endswith("/versions") else self.server.risk_semantics.get_rule(rule_id)  # type: ignore[attr-defined]
                self._json(HTTPStatus.OK, {"items": result} if path.endswith("/versions") else result)
                return
            if path == "/api/health":
                self._json(HTTPStatus.OK, {"schema_version": "logrisk_health_v1", "service": "logrisk-dashboard", "status": "ok", "version": "1.23.0", "storage": "sqlite"})
                return
            if path == "/api/drain-quality/datasets":
                self._json(HTTPStatus.OK, {"schema_version": "drain_dataset_list_v1", "items": self.server.drain_quality.datasets.list()})  # type: ignore[attr-defined]
                return
            match = re.fullmatch(r"/api/drain-quality/datasets/([A-Za-z0-9_-]+)", path)
            if match:
                self._json(HTTPStatus.OK, self.server.drain_quality.datasets.get(match.group(1)))  # type: ignore[attr-defined]
                return
            if path == "/api/drain-quality/annotations":
                self._json(HTTPStatus.OK, {"schema_version": "drain_annotation_list_v1", "items": self.server.drain_quality.annotations.events(), "state": self.server.drain_quality.annotations.replay()})  # type: ignore[attr-defined]
                return
            if path == "/api/drain-quality/annotation-queue/next":
                annotated = set(self.server.drain_quality.annotations.replay())  # type: ignore[attr-defined]
                item = next((template for template in self.server.drain_quality.templates.list_templates() if template["template_hash"] not in annotated), None)  # type: ignore[attr-defined]
                self._json(HTTPStatus.OK, {"item": item})
                return
            if path == "/api/drain-quality/eval-runs":
                self._json(HTTPStatus.OK, {"schema_version": "drain_eval_run_list_v1", "items": self.server.drain_quality.list_eval_runs()})  # type: ignore[attr-defined]
                return
            match = re.fullmatch(r"/api/drain-quality/eval-runs/([A-Za-z0-9_-]+)(/templates)?", path)
            if match:
                run = self.server.drain_quality.get_eval_run(match.group(1))  # type: ignore[attr-defined]
                self._json(HTTPStatus.OK, {"items": run.get("templates", [])} if match.group(2) else run)
                return
            match = re.fullmatch(r"/api/drain-quality/tune-runs/([A-Za-z0-9_-]+)", path)
            if match:
                self._json(HTTPStatus.OK, self.server.drain_quality.get_tune_run(match.group(1)))  # type: ignore[attr-defined]
                return
            if path == "/api/drain-quality/profiles":
                self._json(HTTPStatus.OK, {"schema_version": "drain_profile_list_v1", "items": self.server.drain_quality.list_profiles()})  # type: ignore[attr-defined]
                return
            if path == "/api/drain-quality/configs":
                self._json(HTTPStatus.OK, {
                    "schema_version": "drain_config_list_v1",
                    "items": self.server.drain_quality.configs.list_configs(),  # type: ignore[attr-defined]
                    "active": self.server.drain_quality.configs.active_snapshot(),  # type: ignore[attr-defined]
                })
                return
            if path == "/api/semantic/dictionaries":
                self._json(HTTPStatus.OK, {
                    "schema_version": "semantic_dictionary_list_v1",
                    "items": self.server.semantic_dictionaries.list_dictionaries(),  # type: ignore[attr-defined]
                    "active": self.server.semantic_dictionaries.active_snapshot()["versions"],  # type: ignore[attr-defined]
                })
                return
            match = re.fullmatch(r"/api/semantic/dictionaries/([A-Za-z0-9_-]+)", path)
            if match:
                items = self.server.semantic_dictionaries.list_dictionaries()  # type: ignore[attr-defined]
                item = next((entry for entry in items if entry["dictionary_id"] == match.group(1)), None)
                if item is None:
                    raise SemanticValidationError("语义词典不存在")
                self._json(HTTPStatus.OK, item)
                return
            match = re.fullmatch(r"/api/semantic/dictionaries/([A-Za-z0-9_-]+)/versions/(\d+)", path)
            if match:
                self._json(HTTPStatus.OK, self.server.semantic_dictionaries.get_version(match.group(1), int(match.group(2))))  # type: ignore[attr-defined]
                return
            match = re.fullmatch(r"/api/templates/([^/]+)/semantic-summary", path)
            if match:
                self._json(HTTPStatus.OK, self.server.drain_quality.templates.semantic_summary(unquote(match.group(1))))  # type: ignore[attr-defined]
                return
            match = re.fullmatch(r"/api/drain-quality/configs/([A-Za-z0-9_-]+)/versions/(\d+)", path)
            if match:
                self._json(HTTPStatus.OK, self.server.drain_quality.configs.get_version(match.group(1), int(match.group(2))))  # type: ignore[attr-defined]
                return
            if path == "/api/drain-quality/templates":
                self._json(HTTPStatus.OK, {"schema_version": "drain_template_list_v1", "items": self.server.drain_quality.templates.list_templates(  # type: ignore[attr-defined]
                    status=query.get("status", [None])[0],
                    component=query.get("component", [None])[0],
                    query=query.get("query", [None])[0],
                )})
                return
            match = re.fullmatch(r"/api/drain-quality/templates/([^/]+)(/history)?", path)
            if match:
                template_hash = unquote(match.group(1))
                payload = {"items": self.server.drain_quality.templates.history(template_hash)} if match.group(2) else self.server.drain_quality.templates.get_template(template_hash)  # type: ignore[attr-defined]
                self._json(HTTPStatus.OK, payload)
                return
            if path == "/api/ollama/status":
                self._json(HTTPStatus.OK, self.server.ollama_checker())  # type: ignore[attr-defined]
                return
            if path == "/api/rule-governance/rules":
                self._json(HTTPStatus.OK, self.server.rule_governance.list_rules(  # type: ignore[attr-defined]
                    status=query.get("status", [None])[0],
                    page=int(query.get("page", ["1"])[0]),
                    page_size=int(query.get("page_size", ["50"])[0]),
                ))
                return
            if path == "/api/rule-governance/review-queue":
                self._json(HTTPStatus.OK, self.server.rule_governance.review_queue())  # type: ignore[attr-defined]
                return
            match = re.fullmatch(r"/api/rule-governance/rules/([A-Za-z0-9_-]+)", path)
            if match:
                self._json(HTTPStatus.OK, self.server.rule_governance.get_rule(match.group(1)))  # type: ignore[attr-defined]
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
            if path == "/api/ai-harness/model-profiles":
                self._json(HTTPStatus.OK, self._model_profiles())
                return
            if path == "/api/ai-harness/connections":
                self._json(HTTPStatus.OK, {"items": self.server.connections.list()})  # type: ignore[attr-defined]
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
            match = re.fullmatch(r"/api/input-jobs/([A-Za-z0-9_-]+)", path)
            if match:
                self._json(HTTPStatus.OK, self.server.input_jobs.get_progress(match.group(1)))  # type: ignore[attr-defined]
                return
            match = re.fullmatch(r"/api/input-jobs/([A-Za-z0-9_-]+)/result", path)
            if match:
                result = self.server.input_jobs.get_result(match.group(1))  # type: ignore[attr-defined]
                self._json(HTTPStatus.OK, {"result_path": str(self.server.input_jobs.result_path(match.group(1))), "result": result})  # type: ignore[attr-defined]
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
        except (RiskSemanticError, NodeRiskError) as exc:
            self._json(exc.status_code, {"error": str(exc), "code": exc.code})
        except RuleGovernanceError as exc:
            self._json(exc.status_code, {"error": str(exc), "code": exc.code, "request_id": f"request-{uuid.uuid4().hex}"})
        except (FeatureJobError, ApprovedRuleError, ProcessingMetricsError, DrainQualityError, SemanticValidationError) as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except (TypeError, ValueError) as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    def do_POST(self) -> None:
        path, _ = self._route_parts()
        try:
            payload = self._read_json()
            if path == "/api/semantics":
                if not isinstance(payload, dict):
                    raise RiskSemanticError("请求体必须是 object")
                rule_payload = {key: value for key, value in payload.items() if key not in {"operator", "reason"}}
                self._json(HTTPStatus.CREATED, self.server.risk_semantics.create_rule(rule_payload, operator=str(payload.get("operator") or "local-operator"), reason=str(payload.get("reason") or "创建风险语义")))  # type: ignore[attr-defined]
                return
            if path == "/api/semantics/validate":
                self._json(HTTPStatus.OK, self.server.risk_semantics.validate_payload(payload))  # type: ignore[attr-defined]
                return
            if path == "/api/semantics/test":
                self._json(HTTPStatus.OK, self.server.risk_semantics.test_payload(payload))  # type: ignore[attr-defined]
                return
            if path == "/api/semantics/import":
                self._json(HTTPStatus.CREATED, self.server.risk_semantics.import_bundle(payload, operator=str(payload.get("operator") or "local-operator"), reason=str(payload.get("reason") or "导入风险语义")))  # type: ignore[attr-defined]
                return
            match = re.fullmatch(r"/api/semantics/unclassified/([^/]+)/create-candidate", path)
            if match:
                result = self.server.risk_semantics.create_from_unclassified(unquote(match.group(1)), payload, operator=str(payload.get("operator") or "local-operator"), reason=str(payload.get("reason") or "从待补充语义创建草稿"))  # type: ignore[attr-defined]
                self._json(HTTPStatus.CREATED, result)
                return
            match = re.fullmatch(r"/api/semantics/([^/]+)/(publish|disable|restore-default)", path)
            if match:
                rule_id, action = unquote(match.group(1)), match.group(2)
                common = {"expected_version": int(payload.get("expected_version") or 0), "confirmed": payload.get("confirmed") is True, "operator": str(payload.get("operator") or ""), "reason": str(payload.get("reason") or "")}
                if action == "publish":
                    result = self.server.risk_semantics.publish(rule_id, **common)  # type: ignore[attr-defined]
                elif action == "disable":
                    result = self.server.risk_semantics.disable(rule_id, **common)  # type: ignore[attr-defined]
                else:
                    result = self.server.risk_semantics.restore_default(rule_id, **common)  # type: ignore[attr-defined]
                self._json(HTTPStatus.OK, result)
                return
            match = re.fullmatch(r"/api/semantics/([^/]+)/rollback/(\d+)", path)
            if match:
                result = self.server.risk_semantics.rollback(unquote(match.group(1)), target_version=int(match.group(2)), expected_version=int(payload.get("expected_version") or 0), confirmed=payload.get("confirmed") is True, operator=str(payload.get("operator") or ""), reason=str(payload.get("reason") or ""))  # type: ignore[attr-defined]
                self._json(HTTPStatus.OK, result)
                return
            match = re.fullmatch(r"/api/node-risks/([^/]+)/([^/]+)/recalculate", path)
            if match:
                self._json(HTTPStatus.OK, self.server.node_risks.recalculate(unquote(match.group(1)), unquote(match.group(2))))  # type: ignore[attr-defined]
                return
            match = re.fullmatch(r"/api/node-risks/events/([^/]+)/(acknowledge|recover)", path)
            if match:
                method = self.server.node_risks.acknowledge_event if match.group(2) == "acknowledge" else self.server.node_risks.recover_event  # type: ignore[attr-defined]
                self._json(HTTPStatus.OK, method(unquote(match.group(1)), operator=str(payload.get("operator") or ""), reason=str(payload.get("reason") or "")))
                return
            governance_match = re.fullmatch(
                r"/api/rule-governance/rules/([A-Za-z0-9_-]+)/(status|feedback|rollback)",
                path,
            )
            if governance_match:
                if not isinstance(payload, dict):
                    raise RuleGovernanceError("请求体必须是 JSON object", status_code=400)
                rule_id, action = governance_match.groups()
                if action == "status":
                    result = self.server.rule_governance.change_status(  # type: ignore[attr-defined]
                        rule_id,
                        status=str(payload.get("status") or ""),
                        expected_version=int(payload.get("expected_version") or 0),
                        operator=str(payload.get("operator") or ""),
                        reason=str(payload.get("reason") or ""),
                    )
                    response_status = HTTPStatus.OK
                elif action == "feedback":
                    result = self.server.rule_governance.record_feedback(  # type: ignore[attr-defined]
                        rule_id,
                        outcome=str(payload.get("outcome") or ""),
                        operator=str(payload.get("operator") or ""),
                        note=str(payload.get("note") or ""),
                        cluster=payload.get("cluster"),
                        job_id=payload.get("job_id"),
                        entity_id=payload.get("entity_id"),
                    )
                    response_status = HTTPStatus.CREATED
                else:
                    result = self.server.rule_governance.rollback(  # type: ignore[attr-defined]
                        rule_id,
                        target_version=int(payload.get("target_version") or 0),
                        expected_version=int(payload.get("expected_version") or 0),
                        confirmed=payload.get("confirmed") is True,
                        operator=str(payload.get("operator") or ""),
                        reason=str(payload.get("reason") or ""),
                    )
                    response_status = HTTPStatus.OK
                self._json(response_status, result)
                return
            if path == "/api/drain-quality/datasets":
                self._json(HTTPStatus.CREATED, self.server.drain_quality.datasets.create(payload))  # type: ignore[attr-defined]
                return
            if path == "/api/drain-quality/annotations":
                self._json(HTTPStatus.CREATED, self.server.drain_quality.annotations.append(payload))  # type: ignore[attr-defined]
                return
            match = re.fullmatch(r"/api/drain-quality/annotations/([A-Za-z0-9_-]+)/review", path)
            if match:
                self._json(HTTPStatus.CREATED, self.server.drain_quality.annotations.review(match.group(1), payload))  # type: ignore[attr-defined]
                return
            if path == "/api/drain-quality/clusters/merge":
                source = dict(payload) if isinstance(payload, dict) else payload
                if isinstance(source, dict):
                    source["action"] = "merge"
                self._json(HTTPStatus.CREATED, self.server.drain_quality.annotations.append(source))  # type: ignore[attr-defined]
                return
            if path == "/api/drain-quality/clusters/split":
                source = dict(payload) if isinstance(payload, dict) else payload
                if isinstance(source, dict):
                    source["action"] = "split"
                self._json(HTTPStatus.CREATED, self.server.drain_quality.annotations.append(source))  # type: ignore[attr-defined]
                return
            if path == "/api/drain-quality/eval-runs":
                self._json(HTTPStatus.CREATED, self.server.drain_quality.create_eval_run(payload))  # type: ignore[attr-defined]
                return
            if path == "/api/drain-quality/configs":
                self._json(HTTPStatus.CREATED, self.server.drain_quality.configs.create_candidate(payload))  # type: ignore[attr-defined]
                return
            match = re.fullmatch(r"/api/semantic/dictionaries/([A-Za-z0-9_-]+)/candidates", path)
            if match:
                self._json(HTTPStatus.CREATED, self.server.semantic_dictionaries.create_candidate(match.group(1), payload))  # type: ignore[attr-defined]
                return
            match = re.fullmatch(r"/api/semantic/dictionaries/([A-Za-z0-9_-]+)/candidates/(\d+)/(validate|publish)", path)
            if match:
                version = int(match.group(2))
                result = (
                    self.server.semantic_dictionaries.validate_version(match.group(1), version)  # type: ignore[attr-defined]
                    if match.group(3) == "validate"
                    else self.server.semantic_dictionaries.publish(match.group(1), version, payload)  # type: ignore[attr-defined]
                )
                self._json(HTTPStatus.OK, result)
                return
            match = re.fullmatch(r"/api/semantic/dictionaries/([A-Za-z0-9_-]+)/rollback", path)
            if match:
                if not isinstance(payload, dict):
                    raise SemanticValidationError("请求体必须是 object")
                self._json(HTTPStatus.OK, self.server.semantic_dictionaries.rollback(  # type: ignore[attr-defined]
                    match.group(1), int(payload.get("version") or 0), payload,
                ))
                return
            if path == "/api/semantic/test":
                self._json(HTTPStatus.OK, self.server.semantic_dictionaries.test_snapshot(payload))  # type: ignore[attr-defined]
                return
            match = re.fullmatch(r"/api/drain-quality/configs/([A-Za-z0-9_-]+)/versions", path)
            if match:
                self._json(HTTPStatus.CREATED, self.server.drain_quality.configs.save_version(match.group(1), payload))  # type: ignore[attr-defined]
                return
            match = re.fullmatch(r"/api/drain-quality/configs/([A-Za-z0-9_-]+)/(validate|publish|rollback)", path)
            if match:
                if not isinstance(payload, dict):
                    raise DrainQualityError("请求体必须是 JSON object")
                version = int(payload.get("version") or 0)
                if match.group(2) == "validate":
                    result = self.server.drain_quality.configs.validate_version(match.group(1), version)  # type: ignore[attr-defined]
                elif match.group(2) == "publish":
                    result = self.server.drain_quality.publish_config(match.group(1), version, payload)  # type: ignore[attr-defined]
                else:
                    result = self.server.drain_quality.configs.rollback(match.group(1), version, payload)  # type: ignore[attr-defined]
                self._json(HTTPStatus.OK, result)
                return
            if path == "/api/drain-quality/tune-runs":
                self._json(HTTPStatus.CREATED, self.server.drain_quality.create_tune_run(payload))  # type: ignore[attr-defined]
                return
            if path == "/api/drain-quality/templates/import":
                templates = payload.get("templates") if isinstance(payload, dict) else None
                self._json(HTTPStatus.CREATED, {"items": self.server.drain_quality.templates.import_templates(templates)})  # type: ignore[attr-defined]
                return
            match = re.fullmatch(r"/api/drain-quality/templates/([^/]+)/changes", path)
            if match:
                self._json(HTTPStatus.OK, self.server.drain_quality.templates.change_template(unquote(match.group(1)), payload))  # type: ignore[attr-defined]
                return
            match = re.fullmatch(r"/api/drain-quality/templates/([^/]+)/rollback", path)
            if match:
                if not isinstance(payload, dict):
                    raise DrainQualityError("请求体必须是 JSON object")
                self._json(HTTPStatus.OK, self.server.drain_quality.templates.rollback(  # type: ignore[attr-defined]
                    unquote(match.group(1)), int(payload.get("target_version") or 0),
                    expected_version=int(payload.get("expected_version") or 0),
                    confirmed=payload.get("confirmed") is True,
                    operator=str(payload.get("operator") or "local-operator"),
                ))
                return
            match = re.fullmatch(r"/api/drain-quality/profiles/([A-Za-z0-9_-]+)/(promote|rollback)", path)
            if match:
                method = self.server.drain_quality.promote_profile if match.group(2) == "promote" else self.server.drain_quality.rollback_profile  # type: ignore[attr-defined]
                self._json(HTTPStatus.OK, method(match.group(1), payload))
                return
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
                drain_config = self.server.drain_quality.configs.active_snapshot()  # type: ignore[attr-defined]
                semantic_snapshot = self.server.semantic_dictionaries.active_snapshot()  # type: ignore[attr-defined]
                analyzed = self.server.input_analyzer(records, drain_config["path"], semantic_snapshot) if self.server.input_analyzer_accepts_config else self.server.input_analyzer(records)  # type: ignore[attr-defined]
                result = self.server.govern_drain_result(analyzed)  # type: ignore[attr-defined]
                self._json(HTTPStatus.OK, {"result": result, "drain_config": {
                    "config_id": drain_config["config_id"],
                    "version": drain_config["version"],
                    "content_hash": drain_config["content_hash"],
                }, "semantic_dictionaries": semantic_snapshot["versions"]})
                return
            if path == "/api/uploads":
                if not isinstance(payload, dict):
                    raise FeatureJobError("请求体必须是 JSON object")
                manifest = self.server.upload_store.create(  # type: ignore[attr-defined]
                    filename=str(payload.get("filename") or "upload.log"),
                    size_bytes=int(payload.get("size_bytes") or 0),
                    chunk_size_bytes=int(payload.get("chunk_size_bytes") or 0) or None,
                )
                self._json(HTTPStatus.OK, {
                    "upload_id": manifest["upload_id"],
                    "chunk_size_bytes": manifest["chunk_size_bytes"],
                    "total_chunks": manifest["total_chunks"],
                    "max_upload_bytes": MAX_LARGE_UPLOAD_BYTES,
                    "status": manifest["status"],
                })
                return
            match = re.fullmatch(r"/api/uploads/([A-Za-z0-9_-]+)/complete", path)
            if match:
                manifest = self.server.upload_store.complete(upload_id=match.group(1), final_sha256=payload.get("sha256") if isinstance(payload, dict) else None)  # type: ignore[attr-defined]
                self._json(HTTPStatus.OK, {"upload_id": manifest["upload_id"], "status": manifest["status"], "path": str(self.server.upload_store.source_path(match.group(1)))})  # type: ignore[attr-defined]
                return
            if path == "/api/inputs/analyze-upload":
                if not isinstance(payload, dict):
                    raise FeatureJobError("请求体必须是 JSON object")
                upload_id = str(payload.get("upload_id") or "")
                manifest = self.server.upload_store.get(upload_id)  # type: ignore[attr-defined]
                if manifest.get("status") != "completed":
                    raise FeatureJobError("上传尚未完成")
                job = self.server.input_jobs.create(  # type: ignore[attr-defined]
                    upload_id=upload_id,
                    filename=str(payload.get("filename") or manifest.get("safe_filename") or "upload.log"),
                    source_path=str(self.server.upload_store.source_path(upload_id)),  # type: ignore[attr-defined]
                    drain_config=self.server.drain_quality.configs.active_snapshot(),  # type: ignore[attr-defined]
                    semantic_snapshot=self.server.semantic_dictionaries.active_snapshot(),  # type: ignore[attr-defined]
                )
                threading.Thread(target=self.server.run_input_job, args=(job["input_job_id"],), daemon=True).start()  # type: ignore[attr-defined]
                self._json(HTTPStatus.ACCEPTED, job)
                return
            if path == "/api/ai-harness/model-profiles":
                if not isinstance(payload, dict):
                    raise FeatureJobError("请求体必须是 JSON object")
                connection_id = str(payload.get("connection_id") or "ollama-local")
                connection = self.server.connections.get(connection_id)  # type: ignore[attr-defined]
                payload = {**payload, "connection_id": connection_id, "provider": connection["provider"]}
                profile = self.server.model_profiles.save(payload)  # type: ignore[attr-defined]
                self._json(HTTPStatus.OK, profile.public_dict())
                return
            if path == "/api/ai-harness/connections":
                if not isinstance(payload, dict):
                    raise FeatureJobError("请求体必须是 JSON object")
                self._json(HTTPStatus.OK, self.server.connections.save(payload))  # type: ignore[attr-defined]
                return
            match = re.fullmatch(r"/api/ai-harness/connections/([A-Za-z0-9_-]+)/test", path)
            if match:
                connection = self.server.connections.get(match.group(1))  # type: ignore[attr-defined]
                self._json(HTTPStatus.OK, {"connection_id": match.group(1), **check_model_connection(connection)})
                return
            if path == "/api/jobs":
                if not isinstance(payload, dict):
                    raise FeatureJobError("请求体必须是 JSON object")
                profile = self.server.model_profiles.get(payload.get("model_profile_id"))  # type: ignore[attr-defined]
                connection = self.server.connections.get(profile.connection_id)  # type: ignore[attr-defined]
                if not connection["enabled"]:
                    raise FeatureJobError(f"模型连接已停用: {profile.connection_id}")
                if connection["provider"] == "openai_compatible" and not connection["api_key_configured"]:
                    raise FeatureJobError(f"未配置 API Key 环境变量: {connection.get('api_key_env')}")
                job_id = self.server.manager.create_job(  # type: ignore[attr-defined]
                    payload.get("result"),
                    model=profile.model,
                    min_score=float(payload.get("min_score", 40)),
                    base_url=connection["base_url"],
                    timeout=float(connection["timeout_seconds"]),
                    prompt_id=payload.get("prompt_id") or profile.default_prompt_id,
                    cache_enabled=payload.get("cache_enabled", None),
                    model_profile_id=profile.profile_id,
                    retry_count=int(payload.get("retry_count", 0)),
                    provider=connection["provider"],
                    connection_snapshot=connection,
                    profile_snapshot=profile.public_dict(),
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
        except (RiskSemanticError, NodeRiskError) as exc:
            self._json(exc.status_code, {"error": str(exc), "code": exc.code})
        except RuleGovernanceError as exc:
            self._json(exc.status_code, {"error": str(exc), "code": exc.code, "request_id": f"request-{uuid.uuid4().hex}"})
        except (FeatureJobError, ApprovedRuleError, ProcessingMetricsError, DrainQualityError, SemanticValidationError) as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except (TypeError, ValueError) as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    def do_PUT(self) -> None:
        path, _ = self._route_parts()
        try:
            match = re.fullmatch(r"/api/semantics/([^/]+)", path)
            if match:
                payload = self._read_json()
                if not isinstance(payload, dict):
                    raise RiskSemanticError("请求体必须是 object")
                result = self.server.risk_semantics.update_rule(unquote(match.group(1)), payload.get("changes") or payload, expected_version=int(payload.get("expected_version") or 0), operator=str(payload.get("operator") or ""), reason=str(payload.get("reason") or ""))  # type: ignore[attr-defined]
                self._json(HTTPStatus.OK, result)
                return
            semantic_match = re.fullmatch(r"/api/semantic/dictionaries/([A-Za-z0-9_-]+)/candidates/(\d+)", path)
            if semantic_match:
                payload = self._read_json()
                if not isinstance(payload, dict):
                    raise SemanticValidationError("请求体必须是 object")
                payload = dict(payload, expected_version=int(semantic_match.group(2)))
                self._json(HTTPStatus.CREATED, self.server.semantic_dictionaries.save_version(semantic_match.group(1), payload))  # type: ignore[attr-defined]
                return
            match = re.fullmatch(r"/api/uploads/([A-Za-z0-9_-]+)/chunks/([0-9]+)", path)
            if not match:
                self._json(HTTPStatus.NOT_FOUND, {"error": "资源不存在"})
                return
            data = self._read_bytes(MAX_UPLOAD_BYTES)
            manifest = self.server.upload_store.append_chunk(  # type: ignore[attr-defined]
                upload_id=match.group(1),
                index=int(match.group(2)),
                data=data,
                chunk_sha256=self.headers.get("X-Chunk-SHA256"),
            )
            received = len(manifest.get("received_chunks") or [])
            total = int(manifest["total_chunks"])
            self._json(HTTPStatus.OK, {
                "upload_id": manifest["upload_id"],
                "chunk_index": int(match.group(2)),
                "received": True,
                "received_chunks": received,
                "total_chunks": total,
                "progress": received / total if total else 1.0,
            })
        except (FeatureJobError, DrainQualityError, SemanticValidationError, RiskSemanticError, NodeRiskError, KeyError, ValueError, OSError) as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    def do_DELETE(self) -> None:
        path, _ = self._route_parts()
        try:
            match = re.fullmatch(r"/api/semantics/([^/]+)", path)
            if not match:
                self._json(HTTPStatus.NOT_FOUND, {"error": "资源不存在"})
                return
            payload = self._read_json()
            self.server.risk_semantics.delete(unquote(match.group(1)), confirmed=payload.get("confirmed") is True, operator=str(payload.get("operator") or ""), reason=str(payload.get("reason") or ""))  # type: ignore[attr-defined]
            self._json(HTTPStatus.OK, {"deleted": True})
        except RiskSemanticError as exc:
            self._json(exc.status_code, {"error": str(exc), "code": exc.code})

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
            match = re.fullmatch(r"/api/ai-harness/connections/([A-Za-z0-9_-]+)", path)
            if match:
                if not isinstance(payload, dict):
                    raise FeatureJobError("请求体必须是 JSON object")
                current = self.server.connections.get(match.group(1))  # type: ignore[attr-defined]
                updated = self.server.connections.save({**current, **payload, "connection_id": match.group(1)})  # type: ignore[attr-defined]
                self._json(HTTPStatus.OK, updated)
                return
            match = re.fullmatch(r"/api/jobs/([a-f0-9]+)/features/([A-Za-z0-9_-]+)", path)
            if not match:
                self._json(HTTPStatus.NOT_FOUND, {"error": "资源不存在"})
                return
            feature = self.server.manager.update_feature(match.group(1), match.group(2), payload)  # type: ignore[attr-defined]
            self._json(HTTPStatus.OK, feature)
        except (FeatureJobError, ApprovedRuleError, ProcessingMetricsError, DrainQualityError, KeyError, ValueError) as exc:
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
        self._cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def _serve_frontend_file(self, filename: str, content_type: str) -> None:
        target = self.server.frontend_path.parent / filename  # type: ignore[attr-defined]
        if not target.is_file():
            self._json(HTTPStatus.NOT_FOUND, {"error": "资源不存在"})
            return
        body = target.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self._cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def _prompt_meta(self, prompt: PromptTemplate) -> dict[str, Any]:
        traces = self.server.trace_logger.list_traces(prompt_id=prompt.prompt_id, prompt_hash=prompt.sha256, limit=200)  # type: ignore[attr-defined]
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
        detail["recent_traces"] = self._compact_traces(self.server.trace_logger.list_traces(prompt_id=prompt_id, prompt_hash=prompt.sha256, limit=10))  # type: ignore[attr-defined]
        return detail

    def _ai_harness_status(self) -> dict[str, Any]:
        summary = self.server.trace_logger.summary_today()  # type: ignore[attr-defined]
        return {
            "trace_enabled": self.server.trace_logger.enabled,  # type: ignore[attr-defined]
            "trace_path": str(self.server.trace_logger.path),  # type: ignore[attr-defined]
            "current_prompt_id": FEATURE_PROMPT_ID,
            **summary,
        }

    def _model_profiles(self) -> dict[str, Any]:
        registry = self.server.model_profiles  # type: ignore[attr-defined]
        return {
            "default_profile_id": registry.default_profile_id,
            "profiles": [profile.public_dict() for profile in registry.list_enabled()],
        }

    def _compact_traces(self, traces: list[dict[str, Any]]) -> list[dict[str, Any]]:
        fields = ("trace_id", "job_id", "entity_type", "entity_id", "prompt_id", "prompt_hash", "provider", "model", "model_profile_id", "parameter_size", "thinking_enabled", "context_budget", "evidence_build_meta", "status", "latency_ms", "created_at", "evaluator_result")
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
            "model_profile_id": (trace or {}).get("model_profile_id"),
            "parameter_size": (trace or {}).get("parameter_size"),
            "thinking_enabled": (trace or {}).get("thinking_enabled"),
            "context_budget": (trace or {}).get("context_budget") or {},
            "evidence_build_meta": (trace or {}).get("evidence_build_meta") or {},
        }
        if entity.get("status") == "rule_matched":
            result.update({"status": "reused_rule", "failure_reason": "命中历史规则，跳过 LLM", "model_status": "skipped"})
            return result
        if entity.get("status") == "running":
            result.update({"status": "model_running", "model_status": "running"})
            return result
        if entity.get("cache_hit"):
            result.update({"status": "cache_hit", "failure_reason": "命中 AI Cache，跳过模型调用", "model_status": "cached", "parse_status": "passed", "schema_status": "passed"})
            if features:
                result["status"] = "waiting_review"
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
            "cache_hit": 0,
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
            counts["cache_hit"] += bool(entity.get("cache_hit"))
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
                "model_profile_id": state["model_profile_id"],
                "parameter_size": state["parameter_size"],
                "thinking_enabled": state["thinking_enabled"],
                "context_budget": state["context_budget"],
                "evidence_build_meta": state["evidence_build_meta"],
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
            "model_profile_id": snapshot.get("model_profile_id"),
            "model_profile": self._profile_summary(snapshot.get("model_profile_id")),
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
            "cache_hit_count": int(summary.get("cache_hit") or 0),
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
            "model_profile": progress.get("model_profile") or self._profile_summary(None),
        }

    def _profile_summary(self, profile_id: str | None) -> dict[str, Any]:
        try:
            profile = self.server.model_profiles.get(profile_id)  # type: ignore[attr-defined]
        except Exception:
            return {}
        return {
            "profile_id": profile.profile_id,
            "model": profile.model,
            "parameter_size": profile.parameter_size,
            "thinking_enabled": profile.thinking.enabled,
            "max_templates": profile.evidence_budget.max_templates,
            "max_evidence_chars": profile.evidence_budget.max_evidence_chars,
            "default_prompt_id": profile.default_prompt_id,
        }

    def _observability_event(self, event: dict[str, Any]) -> dict[str, Any]:
        event_type = str(event.get("type") or "")
        stage_map = {
            "job_created": ("job_created", "success", "任务已创建"),
            "job_started": ("entity_filter", "running", "任务开始"),
            "entity_rule_matched": ("rule_reuse", "success", "命中历史规则，跳过 LLM"),
            "entity_cache_hit": ("ai_cache", "success", "命中 AI Cache，跳过模型调用"),
            "entity_started": ("model_call", "running", "开始调用模型"),
            "entity_retrying": ("model_call", "running", f"模型输出异常，自动重试第 {event.get('attempt', 0)} / {event.get('retry_count', 0)} 次"),
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
        self._cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def _serve_events(self, job_id: str, cursor: int) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self._cors_headers()
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
    parser.add_argument("--database", default=os.getenv("LOGRISK_DB_PATH", "state/logrisk.sqlite3"), help="SQLite 数据库路径")
    parser.add_argument("--cors-origins", default=os.getenv("DASHBOARD_CORS_ORIGINS", ""), help="逗号分隔的允许跨域来源")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    server = build_server(
        args.host,
        args.port,
        default_model=args.model,
        default_ollama_url=args.ollama_url,
        default_timeout=args.ollama_timeout,
        cors_origins=args.cors_origins.split(","),
        database_path=args.database,
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
