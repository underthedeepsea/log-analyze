from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping

from logrisk.application.container import ApplicationContainer
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
        if path == "/api/ai-harness/model-profiles":
            return self.model_profiles()
        if path == "/api/ai-harness/prompts":
            return self.prompts()
        if path == "/api/rule-governance/rules":
            return self.rule_governance_rules(query or {})
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
        body = self._service("runtime_service", self.container.runtime_service).readiness()
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

    def rule_governance_rules(self, query: Mapping[str, Any]) -> ApiResult:
        return ApiResult(200, self._service("rule_governance", self.container.rule_governance).list_rules(
            status=self._query(query, "status"),
            page=self._integer(query, "page", 1),
            page_size=self._integer(query, "page_size", 50),
        ))

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
