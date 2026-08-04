from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping

from logrisk.ai_harness.connection_check import check_model_connection
from logrisk.application.container import ApplicationContainer
from logrisk.database import DatabaseError, PostgresDatabase
from logrisk.database_config import database_url_from_candidate
from logrisk.feature_extractor_ollama import FEATURE_PROMPT_ID
from logrisk.feature_jobs import FeatureJobError
from logrisk.runtime.identity import RequestIdentity, require_write_access


@dataclass(frozen=True)
class ApiResult:
    status: int
    body: Mapping[str, Any]
    headers: Mapping[str, str] = field(default_factory=dict)


class ApiFacade:
    """Framework-free read API shared by the local server and Django views."""

    def __init__(
        self,
        container: ApplicationContainer,
        *,
        version: str,
        service_resolver: Callable[[str, Any], Any] | None = None,
    ) -> None:
        self.container = container
        self.version = str(version)
        self.service_resolver = service_resolver

    def dispatch_read(self, path: str, query: Mapping[str, Any] | None = None) -> ApiResult | None:
        if path == "/api/health":
            return self.health()
        if path == "/api/runtime/readiness":
            return self.runtime_readiness()
        if path == "/api/runtime/health":
            return ApiResult(200, self._service("runtime_service", self.container.runtime_service).health())
        if path == "/api/runtime/airflow":
            airflow = self._service("airflow_readiness", None)
            if airflow is None:
                return ApiResult(200, {
                    "configured": False,
                    "status": "not_configured",
                    "ready": True,
                    "online": False,
                    "dag_registered": False,
                    "dags": [],
                })
            return ApiResult(200, {"configured": True, **dict(airflow)})
        if path == "/api/runtime/storage":
            return ApiResult(200, self._service("runtime_service", self.container.runtime_service).storage_usage())
        if path == "/api/runtime/tasks":
            return ApiResult(200, self._service("runtime_service", self.container.runtime_service).list_tasks(
                page=self._integer(query or {}, "page", 1),
                page_size=self._integer(query or {}, "page_size", 50),
                kind=self._query(query or {}, "kind"),
                status=self._query(query or {}, "status"),
            ))
        if path == "/api/runtime/audits":
            return ApiResult(200, self._service("runtime_repository", self.container.runtime_repository).list_audits(
                limit=self._integer(query or {}, "limit", 100),
                before=self._query(query or {}, "before"),
            ))
        if path == "/api/runtime/retention":
            runtime = self._service("runtime_service", self.container.runtime_service)
            return ApiResult(200, {
                "policy": runtime.repository.get_policy(),
                "effective": runtime.retention_policy(),
                "configured": {
                    "enabled": runtime.config.retention.enabled,
                    "completed_days": runtime.config.retention.completed_days,
                },
            })
        if path == "/api/system/database":
            candidate = self.container.database_settings.load()
            return ApiResult(200, {
                "runtime": self.container.database_runtime.public_dict(),
                "candidate": self.container.database_settings.public_dict(candidate) if candidate else None,
                "restart_required": False,
            })
        if path == "/api/ai-harness/model-profiles":
            return self.model_profiles()
        if path == "/api/ai-harness/connections":
            return self.model_connections()
        if path == "/api/ai-harness/prompts":
            return self.prompts()
        if path.startswith("/api/ai-harness/prompts/"):
            prompt_id = path.rsplit("/", 1)[-1]
            if prompt_id:
                return self.prompt_detail(prompt_id)
        if path.startswith("/api/orchestration/runs/"):
            run_id = path.rsplit("/", 1)[-1]
            if run_id:
                return self.orchestration_detail(run_id)
        if path.startswith("/api/input-orchestration/runs/"):
            run_id = path.rsplit("/", 1)[-1]
            if run_id:
                return self.input_orchestration_detail(run_id)
        if path == "/api/rule-governance/rules":
            return self.rule_governance_rules(query or {})
        if path == "/api/rule-governance/review-queue":
            return self.rule_review_queue()
        if path.startswith("/api/rule-governance/rules/"):
            rule_id = path.rsplit("/", 1)[-1]
            if rule_id:
                return self.rule_governance_detail(rule_id)
        if path == "/api/semantics":
            return ApiResult(200, {"schema_version": "risk_semantic_list_v1", "items": self.container.risk_semantics.list_rules()})
        if path == "/api/semantics/effective":
            return ApiResult(200, {"schema_version": "risk_semantic_registry_v1", "items": self.container.risk_semantics.effective_rules()})
        if path == "/api/semantics/export":
            return ApiResult(200, self.container.risk_semantics.export_bundle())
        if path == "/api/semantics/unclassified":
            return ApiResult(200, {"items": self.container.risk_semantics.list_unclassified()})
        if path.startswith("/api/semantics/"):
            if path.endswith("/versions"):
                semantic_id = path.rsplit("/", 2)[-2]
                return self.semantic_versions(semantic_id)
            semantic_id = path.rsplit("/", 1)[-1]
            if semantic_id and semantic_id not in {"effective", "export", "unclassified"}:
                return self.semantic_detail(semantic_id)
        if path == "/api/benchmark-center/overview":
            return ApiResult(200, self.container.benchmark_center.overview())
        if path == "/api/benchmark-center/trends":
            return ApiResult(200, self.container.benchmark_center.trends())
        if path == "/api/benchmark-center/leaderboard":
            return ApiResult(200, self.container.benchmark_center.leaderboard())
        if path == "/api/benchmark-center/suites":
            return ApiResult(200, self.container.benchmark_center.list_suites(
                page=self._integer(query or {}, "page", 1), page_size=self._integer(query or {}, "page_size", 50),
            ))
        if path == "/api/benchmark-center/runs":
            return ApiResult(200, self.container.benchmark_center.list_runs(
                page=self._integer(query or {}, "page", 1), page_size=self._integer(query or {}, "page_size", 50),
                status=self._query(query or {}, "status"),
            ))
        if path.startswith("/api/benchmark-center/runs/"):
            parts = path.split("/")
            run_id = parts[4]
            if len(parts) > 5 and parts[5] == "cases":
                return ApiResult(200, self.container.benchmark_center.repository.list_case_results(
                    run_id, page=self._integer(query or {}, "page", 1), page_size=self._integer(query or {}, "page_size", 50),
                ))
            if len(parts) > 5 and parts[5] == "artifacts":
                return ApiResult(200, self.container.benchmark_center.repository.list_artifacts(run_id))
            return ApiResult(200, self.container.benchmark_center.get_run(run_id))
        if path == "/api/drain-quality/datasets":
            return ApiResult(200, {"schema_version": "drain_dataset_list_v1", "items": self.container.drain_quality.datasets.list()})
        if path == "/api/drain-quality/annotations":
            return ApiResult(200, {"schema_version": "drain_annotation_list_v1", "items": self.container.drain_quality.annotations.events(), "state": self.container.drain_quality.annotations.replay()})
        if path == "/api/drain-quality/eval-runs":
            return ApiResult(200, {"schema_version": "drain_eval_run_list_v1", "items": self.container.drain_quality.list_eval_runs()})
        if path == "/api/drain-quality/profiles":
            return ApiResult(200, {"schema_version": "drain_profile_list_v1", "items": self.container.drain_quality.list_profiles()})
        if path == "/api/drain-quality/configs":
            return ApiResult(200, {"schema_version": "drain_config_list_v1", "items": self.container.drain_quality.configs.list_configs(), "active": self.container.drain_quality.configs.active_snapshot()})
        if path == "/api/semantic/dictionaries":
            return ApiResult(200, {
                "schema_version": "semantic_dictionary_list_v1",
                "items": self.container.semantic_dictionaries.list_dictionaries(),
                "active": self.container.semantic_dictionaries.active_snapshot()["versions"],
            })
        if path.startswith("/api/semantic/dictionaries/"):
            parts = path.split("/")
            dictionary_id = parts[4]
            if len(parts) >= 7 and parts[5] == "versions":
                return ApiResult(200, self.container.semantic_dictionaries.get_version(dictionary_id, int(parts[6])))
            return ApiResult(200, next(
                item for item in self.container.semantic_dictionaries.list_dictionaries()
                if item["dictionary_id"] == dictionary_id
            ))
        if path == "/api/release-readiness":
            return self.release_readiness()
        return None

    def health(self) -> ApiResult:
        return ApiResult(200, {
            "schema_version": "logrisk_health_v1",
            "service": "logrisk-dashboard",
            "status": "ok",
            "version": self.version,
            "storage": self.container.database_runtime.provider,
        })

    def runtime_readiness(self) -> ApiResult:
        body = self._service("runtime_service", self.container.runtime_service).readiness(
            airflow=self._service("airflow_readiness", None)
        )
        return ApiResult(200 if body["ready"] else 503, body)

    def model_profiles(self) -> ApiResult:
        registry = self._service("model_profiles", self.container.model_profiles)
        profiles: list[dict[str, Any]] = []
        for profile in registry.list_enabled():
            item = profile.public_dict()
            try:
                connection = self._service("connections", self.container.connections).get(profile.connection_id)
                item["connection_enabled"] = bool(connection.get("enabled"))
                extension_ready = all(dict(connection.get("credential_envs_configured") or {}).values())
                item["connection_ready"] = bool(
                    connection.get("enabled")
                    and (
                        connection.get("provider") == "ollama"
                        or connection.get("api_key_configured")
                        or (connection.get("provider") == "extension" and extension_ready)
                    )
                )
            except KeyError:
                item["connection_enabled"] = False
                item["connection_ready"] = False
            profiles.append(item)
        return ApiResult(200, {"default_profile_id": registry.default_profile_id, "profiles": profiles})

    def prompts(self) -> ApiResult:
        items: list[dict[str, Any]] = []
        prompts = self._service("prompt_registry", self.container.prompt_registry)
        traces_logger = self._service("trace_logger", self.container.trace_logger)
        for prompt in prompts.list_prompts():
            traces = traces_logger.list_traces(
                prompt_id=prompt.prompt_id, prompt_hash=prompt.sha256, limit=200
            )
            items.append({
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
            })
        return ApiResult(200, {"current_prompt_id": FEATURE_PROMPT_ID, "items": items})

    def prompt_detail(self, prompt_id: str) -> ApiResult:
        prompts = self._service("prompt_registry", self.container.prompt_registry)
        prompt = prompts.load(str(prompt_id))
        traces_logger = self._service("trace_logger", self.container.trace_logger)
        traces = traces_logger.list_traces(prompt_id=prompt.prompt_id, prompt_hash=prompt.sha256, limit=10)
        return ApiResult(200, {
            "prompt_id": prompt.prompt_id,
            "display_name": prompt.display_name or prompt.prompt_id,
            "description": prompt.description or "",
            "analysis_type": prompt.analysis_type,
            "status": prompt.status,
            "is_default": prompt.is_default,
            "prompt_hash": prompt.sha256,
            "path": prompt.path,
            "version": prompt.version,
            "content": prompt.content,
            "history": prompts.history(prompt.prompt_id),
            "recent_traces": [
                {key: item.get(key) for key in (
                    "trace_id", "job_id", "entity_type", "entity_id", "prompt_id", "prompt_hash",
                    "provider", "model", "model_profile_id", "status", "latency_ms", "created_at",
                )}
                for item in traces
            ],
        })

    def model_connections(self) -> ApiResult:
        return ApiResult(200, {
            "items": self._service("connections", self.container.connections).list(),
        })

    def save_connection(self, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        connection = self._service("connections", self.container.connections).save(dict(payload))
        self._audit(
            "ai_connection.saved",
            "provider_connection",
            identity,
            resource_id=str(connection.get("connection_id") or ""),
            attributes={
                "provider": str(connection.get("provider") or ""),
                "enabled": bool(connection.get("enabled")),
            },
        )
        return ApiResult(200, connection)

    def update_connection(
        self,
        connection_id: str,
        payload: Mapping[str, Any],
        identity: RequestIdentity,
    ) -> ApiResult:
        self._require_write(identity)
        connections = self._service("connections", self.container.connections)
        current = connections.get(str(connection_id))
        updated = connections.save({**current, **dict(payload), "connection_id": str(connection_id)})
        self._audit(
            "ai_connection.saved",
            "provider_connection",
            identity,
            resource_id=str(connection_id),
            attributes={"provider": str(updated.get("provider") or ""), "enabled": bool(updated.get("enabled"))},
        )
        return ApiResult(200, updated)

    def test_connection(self, connection_id: str, identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        connection = self._service("connections", self.container.connections).get(str(connection_id))
        result = {"connection_id": str(connection_id), **check_model_connection(connection)}
        self._audit(
            "ai_connection.tested",
            "provider_connection",
            identity,
            resource_id=str(connection_id),
            attributes={"provider": str(connection.get("provider") or ""), "online": bool(result.get("online"))},
        )
        return ApiResult(200, result)

    def save_model_profile(self, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        profiles = self._service("model_profiles", self.container.model_profiles)
        connections = self._service("connections", self.container.connections)
        profile_payload = dict(payload)
        connection_id = str(profile_payload.get("connection_id") or "")
        if not connection_id:
            available = connections.list()
            if not available:
                raise ValueError("没有可用的模型连接")
            connection_id = str(available[0]["connection_id"])
        connection = connections.get(connection_id)
        profile_payload["connection_id"] = connection_id
        profile_payload["provider"] = connection["provider"]
        profile = profiles.save(profile_payload)
        self._audit(
            "model_profile.saved",
            "model_profile",
            identity,
            resource_id=profile.profile_id,
            attributes={
                "connection_id": profile.connection_id,
                "provider": profile.provider,
                "enabled": bool(profile.enabled),
            },
        )
        return ApiResult(200, profile.public_dict())

    def update_prompt(
        self,
        prompt_id: str,
        payload: Mapping[str, Any],
        identity: RequestIdentity,
    ) -> ApiResult:
        self._require_write(identity)
        content = payload.get("content")
        if not isinstance(content, str) or not content.strip():
            raise ValueError("Prompt 内容不能为空")
        prompts = self._service("prompt_registry", self.container.prompt_registry)
        prompt = prompts.update(str(prompt_id), content, str(payload.get("note") or ""))
        self._audit(
            "prompt.updated",
            "prompt_template",
            identity,
            resource_id=prompt.prompt_id,
            attributes={"version": prompt.version, "analysis_type": prompt.analysis_type},
        )
        return self.prompt_detail(prompt.prompt_id)

    def save_retention_policy(self, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        runtime = self._service("runtime_service", self.container.runtime_service)
        result = runtime.save_retention_policy(
            payload.get("policy", {}),
            expected_version=int(payload.get("expected_version", 0)),
            actor=identity.actor,
            roles=identity.roles,
            request_id=identity.request_id,
        )
        return ApiResult(200, {
            "policy": result,
            "request_id": identity.request_id,
            "resource_id": result["policy_id"],
            "version": result["version"],
        })

    def run_retention(self, *, execute: bool, identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        result = self._service("runtime_service", self.container.runtime_service).run_retention(
            actor=identity.actor,
            request_id=identity.request_id,
            execute=execute,
        )
        return ApiResult(200, {
            "maintenance": result,
            "request_id": identity.request_id,
            "resource_id": result["run_id"],
            "version": 1,
        })

    def save_database_candidate(self, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        candidate = self.container.database_settings.save(payload)
        self._audit(
            "runtime.database_candidate.saved",
            "database_connection_candidate",
            identity,
            resource_id=None,
            attributes={
                "provider": str(candidate.get("provider") or ""),
                "restart_required": True,
            },
        )
        return ApiResult(200, {
            "candidate": candidate,
            "restart_required": True,
            "message": "连接候选配置已保存，重启 Django/Airflow 后生效",
        })

    def test_database_candidate(self, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        candidate = self.container.database_settings.validate(payload)
        if candidate["provider"] == "sqlite":
            return ApiResult(200, {"online": True, "provider": "sqlite", "message": "SQLite 本地路径配置有效"})
        try:
            url = database_url_from_candidate(candidate, os.environ)
            PostgresDatabase(url, state_root=self.container.database.state_root, migrate=False).test_connection()
        except (DatabaseError, ValueError):
            raise ValueError("无法连接 PostgreSQL；请检查地址、网络、SSL 和密码环境变量")
        return ApiResult(200, {"online": True, "provider": "postgres", "message": "PostgreSQL 连接成功"})

    def rule_governance_rules(self, query: Mapping[str, Any]) -> ApiResult:
        return ApiResult(200, self._service("rule_governance", self.container.rule_governance).list_rules(
            status=self._query(query, "status"),
            page=self._integer(query, "page", 1),
            page_size=self._integer(query, "page_size", 50),
        ))

    def rule_review_queue(self) -> ApiResult:
        return ApiResult(200, self._service("rule_governance", self.container.rule_governance).review_queue())

    def semantic_detail(self, rule_id: str) -> ApiResult:
        rule = self.container.risk_semantics.get_rule(str(rule_id))
        return ApiResult(200, {"rule": rule, "versions": self.container.risk_semantics.versions(str(rule_id))})

    def semantic_versions(self, rule_id: str) -> ApiResult:
        return ApiResult(200, {"items": self.container.risk_semantics.versions(str(rule_id))})

    def create_semantic_rule(self, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        data = {key: value for key, value in dict(payload).items() if key not in {"operator", "reason"}}
        reason = str(payload.get("reason") or "创建风险语义")
        result = self.container.risk_semantics.create_rule(
            data, operator=str(identity.actor or "unknown"), reason=reason,
        )
        self._audit(
            "semantic.created", "risk_semantic_rule", identity,
            resource_id=str(result.get("id") or ""),
            attributes={"version": int(result.get("version") or 0), "source": str(result.get("source") or "")},
        )
        return ApiResult(201, result)

    def create_benchmark_suite(self, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        data = dict(payload, operator=str(identity.actor or "unknown"))
        result = self.container.benchmark_center.create_suite(data)
        self._audit(
            "benchmark.suite_created", "benchmark_suite", identity,
            resource_id=str(result.get("suite_id") or ""), attributes={"case_count": int(result.get("case_count") or 0)},
        )
        return ApiResult(201, result)

    def create_drain_dataset(self, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        result = self.container.drain_quality.datasets.create(dict(payload))
        self._audit(
            "drain.dataset_created", "drain_dataset", identity,
            resource_id=str(result.get("dataset_id") or ""), attributes={"record_count": int(result.get("record_count") or 0)},
        )
        return ApiResult(201, result)

    def append_drain_annotation(self, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        result = self.container.drain_quality.annotations.append(dict(payload, reviewer=str(identity.actor or "unknown")))
        self._audit(
            "drain.annotation_created", "drain_annotation", identity,
            resource_id=str(result.get("annotation_id") or ""), attributes={"action": str(result.get("action") or "")},
        )
        return ApiResult(201, result)

    def review_drain_annotation(self, annotation_id: str, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        result = self.container.drain_quality.annotations.review(
            str(annotation_id), dict(payload, reviewer=str(identity.actor or "unknown")),
        )
        self._audit(
            "drain.annotation_reviewed", "drain_annotation", identity,
            resource_id=str(annotation_id), attributes={"decision": str(result.get("decision") or "")},
        )
        return ApiResult(201, result)

    def create_drain_eval_run(self, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        result = self.container.drain_quality.create_eval_run(dict(payload))
        self._audit(
            "drain.evaluation_created", "drain_eval_run", identity,
            resource_id=str(result.get("run_id") or ""), attributes={"status": str(result.get("status") or "")},
        )
        return ApiResult(201, result)

    def create_drain_config(self, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        result = self.container.drain_quality.configs.create_candidate(dict(payload))
        self._audit(
            "drain.config_candidate_created", "drain_config", identity,
            resource_id=str(result.get("config_id") or ""), attributes={"version": int(result.get("version") or 0)},
        )
        return ApiResult(201, result)

    def create_drain_tune_run(self, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        result = self.container.drain_quality.create_tune_run(dict(payload))
        self._audit(
            "drain.tune_run_created", "drain_tune_run", identity,
            resource_id=str(result.get("run_id") or ""), attributes={"candidate_count": int(result.get("candidate_count") or 0)},
        )
        return ApiResult(201, result)

    def test_semantic_dictionary(self, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        result = self.container.semantic_dictionaries.test_snapshot(dict(payload))
        self._audit(
            "semantic_dictionary.tested", "semantic_dictionary", identity,
            resource_id=None, attributes={"matched": bool(result.get("semantic_fields"))},
        )
        return ApiResult(200, result)

    def create_semantic_dictionary_candidate(self, dictionary_id: str, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        result = self.container.semantic_dictionaries.create_candidate(
            str(dictionary_id), dict(payload, operator=str(identity.actor or "unknown")),
        )
        self._audit(
            "semantic_dictionary.candidate_created", "semantic_dictionary", identity,
            resource_id=str(dictionary_id), attributes={"version": int(result.get("version") or 0)},
        )
        return ApiResult(201, result)

    def semantic_dictionary_action(self, dictionary_id: str, action: str, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        version = int(payload.get("version") or 0)
        data = dict(payload, operator=str(identity.actor or "unknown"))
        if action == "validate":
            result = self.container.semantic_dictionaries.validate_version(str(dictionary_id), version)
        elif action == "publish":
            result = self.container.semantic_dictionaries.publish(str(dictionary_id), version, data)
        elif action == "rollback":
            result = self.container.semantic_dictionaries.rollback(str(dictionary_id), version, data)
        else:
            raise ValueError("不支持的语义词典操作")
        self._audit(
            "semantic_dictionary." + action, "semantic_dictionary", identity,
            resource_id=str(dictionary_id), attributes={"version": version, "valid": bool(result.get("valid", True))},
        )
        return ApiResult(200, result)

    def create_benchmark_run(self, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        result = self.container.benchmark_center.create_run(dict(payload, operator=str(identity.actor or "unknown")))
        self._audit(
            "benchmark.run_created", "benchmark_run", identity,
            resource_id=str(result.get("run_id") or ""), attributes={"mode": str(result.get("mode") or "")},
        )
        return ApiResult(202, result)

    def cancel_benchmark_run(self, run_id: str, identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        result = self.container.benchmark_center.cancel_run(str(run_id), operator=str(identity.actor or "unknown"))
        self._audit(
            "benchmark.run_cancelled", "benchmark_run", identity,
            resource_id=str(run_id), attributes={"status": str(result.get("status") or "")},
        )
        return ApiResult(200, result)

    def compare_benchmark(self, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        return ApiResult(200, self.container.benchmark_center.compare(dict(payload)))

    def evaluate_benchmark_gate(self, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        result = self.container.benchmark_center.evaluate_gate(dict(payload, operator=str(identity.actor or "unknown")))
        self._audit(
            "benchmark.gate_evaluated", "benchmark_gate", identity,
            resource_id=str(result.get("gate_id") or ""), attributes={"decision": str(result.get("decision") or "")},
        )
        return ApiResult(201, result)

    def create_semantic_override(self, rule_id: str, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        changes = {key: value for key, value in dict(payload).items() if key not in {"operator", "reason"}}
        result = self.container.risk_semantics.create_override(
            str(rule_id), changes, operator=str(identity.actor or "unknown"),
            reason=str(payload.get("reason") or "创建内置语义覆盖"),
        )
        self._audit(
            "semantic.override_created", "risk_semantic_rule", identity,
            resource_id=str(result.get("id") or ""),
            attributes={"override_of": str(rule_id), "version": int(result.get("version") or 0)},
        )
        return ApiResult(201, result)

    def update_semantic_rule(self, rule_id: str, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        changes = {key: value for key, value in dict(payload).items() if key not in {"operator", "reason", "expected_version"}}
        result = self.container.risk_semantics.update_rule(
            str(rule_id), changes, expected_version=int(payload.get("expected_version") or 0),
            operator=str(identity.actor or "unknown"), reason=str(payload.get("reason") or "更新风险语义"),
        )
        self._audit(
            "semantic.updated", "risk_semantic_rule", identity,
            resource_id=str(rule_id), attributes={"version": int(result.get("version") or 0)},
        )
        return ApiResult(200, result)

    def validate_semantic(self, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        result = self.container.risk_semantics.validate_payload(dict(payload))
        self._audit(
            "semantic.validated", "risk_semantic_rule", identity,
            resource_id=str(payload.get("id") or ""), attributes={"valid": bool(result.get("valid"))},
        )
        return ApiResult(200, result)

    def test_semantic(self, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        result = self.container.risk_semantics.test_payload(dict(payload))
        self._audit(
            "semantic.tested", "risk_semantic_rule", identity,
            resource_id=str((payload.get("rule") or payload).get("id") if isinstance(payload.get("rule") or payload, Mapping) else ""),
            attributes={"valid": bool(result.get("valid"))},
        )
        return ApiResult(200, result)

    def import_semantic_bundle(self, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        result = self.container.risk_semantics.import_bundle(
            dict(payload), operator=str(identity.actor or "unknown"),
            reason=str(payload.get("reason") or "导入风险语义"),
        )
        self._audit(
            "semantic.imported", "risk_semantic_bundle", identity,
            resource_id=None, attributes={"created": len(result.get("created") or [])},
        )
        return ApiResult(201, result)

    def semantic_action(self, rule_id: str, action: str, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        common = {
            "expected_version": int(payload.get("expected_version") or 0),
            "confirmed": payload.get("confirmed") is True,
            "operator": str(identity.actor or "unknown"),
            "reason": str(payload.get("reason") or "风险语义操作"),
        }
        if action == "publish":
            result = self.container.risk_semantics.publish(str(rule_id), **common)
        elif action == "disable":
            result = self.container.risk_semantics.disable(str(rule_id), **common)
        elif action == "restore-default":
            result = self.container.risk_semantics.restore_default(str(rule_id), **common)
        else:
            raise ValueError("不支持的语义操作")
        self._audit(
            "semantic." + ("published" if action == "publish" else "disabled" if action == "disable" else "restored_default"),
            "risk_semantic_rule", identity,
            resource_id=str(rule_id), attributes={"status": str(result.get("status") or ""), "version": int(result.get("version") or 0)},
        )
        return ApiResult(200, result)

    def rollback_semantic(self, rule_id: str, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        result = self.container.risk_semantics.rollback(
            str(rule_id), target_version=int(payload.get("target_version") or payload.get("version") or 0),
            expected_version=int(payload.get("expected_version") or 0), confirmed=payload.get("confirmed") is True,
            operator=str(identity.actor or "unknown"), reason=str(payload.get("reason") or "回滚风险语义"),
        )
        self._audit(
            "semantic.rolled_back", "risk_semantic_rule", identity,
            resource_id=str(rule_id), attributes={"version": int(result.get("version") or 0)},
        )
        return ApiResult(200, result)

    def rule_governance_detail(self, rule_id: str) -> ApiResult:
        return ApiResult(200, self._service("rule_governance", self.container.rule_governance).get_rule(str(rule_id)))

    def change_rule_status(
        self,
        rule_id: str,
        payload: Mapping[str, Any],
        identity: RequestIdentity,
    ) -> ApiResult:
        self._require_write(identity)
        result = self._service("rule_governance", self.container.rule_governance).change_status(
            str(rule_id),
            status=str(payload.get("status") or ""),
            expected_version=int(payload.get("expected_version") or 0),
            operator=str(identity.actor or "unknown"),
            reason=str(payload.get("reason") or ""),
        )
        self._audit(
            "rule.status_changed",
            "approved_rule",
            identity,
            resource_id=str(rule_id),
            attributes={"status": str(payload.get("status") or ""), "version": int(result.get("version") or 0)},
        )
        return ApiResult(200, result)

    def record_rule_feedback(
        self,
        rule_id: str,
        payload: Mapping[str, Any],
        identity: RequestIdentity,
    ) -> ApiResult:
        self._require_write(identity)
        result = self._service("rule_governance", self.container.rule_governance).record_feedback(
            str(rule_id),
            outcome=str(payload.get("outcome") or ""),
            operator=str(identity.actor or "unknown"),
            note=str(payload.get("note") or ""),
            cluster=payload.get("cluster"),
            job_id=payload.get("job_id"),
            entity_id=payload.get("entity_id"),
        )
        self._audit(
            "rule.feedback_recorded",
            "approved_rule",
            identity,
            resource_id=str(rule_id),
            attributes={"outcome": str(payload.get("outcome") or ""), "version": int(result.get("version") or 0)},
        )
        return ApiResult(201, result)

    def rollback_rule(
        self,
        rule_id: str,
        payload: Mapping[str, Any],
        identity: RequestIdentity,
    ) -> ApiResult:
        self._require_write(identity)
        result = self._service("rule_governance", self.container.rule_governance).rollback(
            str(rule_id),
            target_version=int(payload.get("target_version") or 0),
            expected_version=int(payload.get("expected_version") or 0),
            confirmed=payload.get("confirmed") is True,
            operator=str(identity.actor or "unknown"),
            reason=str(payload.get("reason") or ""),
        )
        self._audit(
            "rule.rolled_back",
            "approved_rule",
            identity,
            resource_id=str(rule_id),
            attributes={
                "target_version": int(payload.get("target_version") or 0),
                "version": int(result.get("version") or 0),
            },
        )
        return ApiResult(200, result)

    def release_readiness(self) -> ApiResult:
        return ApiResult(200, self._service("release_readiness", self.container.release_readiness).overview())

    def create_feature_job(self, payload: Mapping[str, Any]) -> str:
        document = payload.get("result")
        if not isinstance(document, Mapping):
            raise FeatureJobError("result 必须是 JSON object")
        self._service("runtime_service", self.container.runtime_service).require_capacity("特征识别")
        profile = self._service("model_profiles", self.container.model_profiles).get(payload.get("model_profile_id"))
        connection = self._service("connections", self.container.connections).get(profile.connection_id)
        if not connection.get("enabled"):
            raise FeatureJobError("模型连接已停用")
        provider = str(connection.get("provider") or "")
        if provider == "openai_compatible" and not connection.get("api_key_configured"):
            raise FeatureJobError("远端模型连接缺少 API Key 环境变量")
        if provider == "extension" and not all(dict(connection.get("credential_envs_configured") or {}).values()):
            raise FeatureJobError("扩展模型连接缺少所需凭据环境变量")
        return self._service("feature_jobs", self.container.feature_jobs).create_job(
            dict(document),
            model=profile.model,
            min_score=float(payload.get("min_score", 40)),
            base_url=str(connection["base_url"]),
            timeout=float(connection["timeout_seconds"]),
            prompt_id=str(payload.get("prompt_id") or profile.default_prompt_id),
            cache_enabled=payload.get("cache_enabled"),
            model_profile_id=profile.profile_id,
            retry_count=int(payload.get("retry_count", 0)),
            provider=provider,
            connection_snapshot=connection,
            profile_snapshot=profile.public_dict(),
        )

    def feature_job(self, job_id: str) -> ApiResult:
        manager = self._service("feature_jobs", self.container.feature_jobs)
        manager.refresh_from_persistence(str(job_id))
        return ApiResult(200, manager.get_job(str(job_id)))

    def feature_job_events(self, job_id: str, cursor: int) -> tuple[list[dict[str, Any]], int]:
        manager = self._service("feature_jobs", self.container.feature_jobs)
        manager.refresh_from_persistence(str(job_id))
        return manager.wait_for_events(str(job_id), max(0, int(cursor)), timeout=0)

    def orchestration_detail(self, orchestration_run_id: str) -> ApiResult:
        return ApiResult(200, self.container.orchestration.get(str(orchestration_run_id)))

    def input_orchestration_detail(self, input_orchestration_run_id: str) -> ApiResult:
        return ApiResult(200, self.container.input_orchestration.get(str(input_orchestration_run_id)))

    def sync_orchestration(
        self,
        orchestration_run_id: str,
        payload: Mapping[str, Any],
        identity: RequestIdentity,
    ) -> ApiResult:
        self._require_write(identity)
        service = self.container.orchestration
        current = service.get(str(orchestration_run_id))
        external_run_id = str(current.get("external_run_id") or "")
        if not external_run_id:
            raise ValueError("编排运行尚未绑定 Airflow DAG Run")
        airflow = self._service("airflow_orchestrator", None)
        if airflow is None:
            raise ValueError("Django 未配置 Airflow 编排器")
        external = airflow.get_run(external_run_id)
        if (
            external.job_id != current["job_id"]
            or external.orchestration_run_id != current["orchestration_run_id"]
            or external.external_run_id != external_run_id
        ):
            raise ValueError("Airflow DAG Run 与 LOGRISK 编排标识不匹配")
        expected_version = int(payload.get("expected_version") or current["state_version"])
        result = service.reconcile_external(
            str(orchestration_run_id), external.state, expected_version=expected_version,
        )
        self._audit(
            "orchestration.reconciled",
            "orchestration_run",
            identity,
            resource_id=str(orchestration_run_id),
            attributes={"airflow_state": external.state, "status": result["status"]},
        )
        return ApiResult(200, {**result, "airflow_state": external.state})

    def sync_input_orchestration(
        self,
        input_orchestration_run_id: str,
        payload: Mapping[str, Any],
        identity: RequestIdentity,
    ) -> ApiResult:
        self._require_write(identity)
        service = self.container.input_orchestration
        current = service.get(str(input_orchestration_run_id))
        external_run_id = str(current.get("external_run_id") or "")
        if not external_run_id:
            raise ValueError("输入编排运行尚未绑定 Airflow DAG Run")
        airflow = self._service("input_airflow_orchestrator", None)
        if airflow is None:
            raise ValueError("Django 未配置输入 Airflow 编排器")
        external = airflow.get_run(external_run_id)
        if (
            external.input_job_id != current["input_job_id"]
            or external.input_orchestration_run_id != current["input_orchestration_run_id"]
            or external.external_run_id != external_run_id
        ):
            raise ValueError("Airflow 输入 DAG Run 与 LOGRISK 编排标识不匹配")
        expected_version = int(payload.get("expected_version") or current["state_version"])
        result = service.reconcile_external(
            str(input_orchestration_run_id), external.state, expected_version=expected_version,
        )
        self._audit(
            "input_orchestration.reconciled",
            "input_orchestration_run",
            identity,
            resource_id=str(input_orchestration_run_id),
            attributes={"airflow_state": external.state, "status": result["status"]},
        )
        return ApiResult(200, {**result, "airflow_state": external.state})

    def cancel_orchestration(self, orchestration_run_id: str, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        service = self.container.orchestration
        current = service.get(str(orchestration_run_id))
        expected_version = int(payload.get("expected_version") or current["state_version"])
        result = service.request_cancel(str(orchestration_run_id), expected_version=expected_version)
        self._audit(
            "orchestration.cancel_requested", "orchestration_run", identity,
            resource_id=str(orchestration_run_id), attributes={"version": int(result["state_version"])},
        )
        return ApiResult(200, result)

    def retry_orchestration(self, orchestration_run_id: str, payload: Mapping[str, Any], identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        service = self.container.orchestration
        current = service.get(str(orchestration_run_id))
        expected_version = int(payload.get("expected_version") or current["state_version"])
        result = service.retry_dispatch(str(orchestration_run_id), expected_version=expected_version)
        self._audit(
            "orchestration.retry_requested", "orchestration_run", identity,
            resource_id=str(orchestration_run_id), attributes={"version": int(result["state_version"])},
        )
        return ApiResult(200, result)

    def update_feature(
        self,
        job_id: str,
        candidate_id: str,
        payload: Mapping[str, Any],
        identity: RequestIdentity,
    ) -> ApiResult:
        self._require_write(identity)
        feature = self._service("feature_jobs", self.container.feature_jobs).update_feature(
            str(job_id), str(candidate_id), dict(payload)
        )
        status = str(feature.get("status") or "pending")
        self._audit(
            "feature.approved" if status == "approved" else "feature.reviewed",
            "feature_candidate",
            identity,
            resource_id=str(candidate_id),
            attributes={"job_id": str(job_id), "status": status},
        )
        return ApiResult(200, feature)

    def export_approved(self, job_id: str, identity: RequestIdentity) -> ApiResult:
        self._require_write(identity)
        package = self._service("feature_jobs", self.container.feature_jobs).export_approved(str(job_id))
        self._audit(
            "feature.exported",
            "feature_job",
            identity,
            resource_id=str(job_id),
            attributes={
                "approved_count": int(dict(package.get("review_statistics") or {}).get("approved") or 0),
            },
        )
        return ApiResult(
            200,
            package,
            {"Content-Disposition": 'attachment; filename="logrisk-feature-package.json"'},
        )

    def validate_release(
        self,
        payload: Mapping[str, Any],
        identity: RequestIdentity,
        *,
        idempotency_key: str,
    ) -> ApiResult:
        self._require_write(identity)
        key = str(idempotency_key).strip()
        if not key:
            raise ValueError("发布校验需要 Idempotency-Key 或 idempotency_key")
        result = self._service("release_readiness", self.container.release_readiness).validate(
            target_version=str(payload.get("target_version") or self.version),
            idempotency_key=key,
        )
        self._audit(
            "release_readiness.validated",
            "release_validation",
            identity,
            resource_id=str(result["validation_id"]),
            attributes={"target_version": result["target_version"], "status": result["status"]},
        )
        return ApiResult(200, {**result, "request_id": identity.request_id, "resource_id": result["validation_id"]})

    def _service(self, name: str, default: Any) -> Any:
        return self.service_resolver(name, default) if self.service_resolver else default

    def _require_write(self, identity: RequestIdentity) -> None:
        require_write_access(identity, self.container.runtime_config)

    def _audit(
        self,
        action: str,
        resource_type: str,
        identity: RequestIdentity,
        *,
        resource_id: str | None,
        attributes: Mapping[str, Any],
    ) -> None:
        self._service("runtime_repository", self.container.runtime_repository).append_audit(
            action,
            resource_type,
            identity.actor,
            identity.request_id,
            attributes,
            resource_id=resource_id,
            roles=identity.roles,
        )

    @staticmethod
    def _query(query: Mapping[str, Any], name: str) -> str | None:
        value = query.get(name)
        if isinstance(value, (list, tuple)):
            value = value[0] if value else None
        return str(value) if value is not None and str(value) else None

    @classmethod
    def _integer(cls, query: Mapping[str, Any], name: str, default: int) -> int:
        value = cls._query(query, name)
        try:
            return int(value) if value is not None else default
        except ValueError:
            return default
