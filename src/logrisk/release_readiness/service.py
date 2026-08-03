from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from logrisk.runtime.repository import sanitize_runtime_metadata

from .repository import ReleaseReadinessRepository


class ReleaseReadinessService:
    """Run deterministic, non-mutating pre-release checks for LOGRISK."""

    def __init__(
        self,
        repository: ReleaseReadinessRepository,
        *,
        runtime_service: Any,
        project_root: str | Path,
        connections: Any,
        model_profiles: Any,
        prompt_registry: Any,
        drain_quality: Any,
        semantic_dictionaries: Any,
        multi_source: Any,
        benchmark_center: Any,
    ) -> None:
        self.repository = repository
        self.runtime_service = runtime_service
        self.project_root = Path(project_root)
        self.connections = connections
        self.model_profiles = model_profiles
        self.prompt_registry = prompt_registry
        self.drain_quality = drain_quality
        self.semantic_dictionaries = semantic_dictionaries
        self.multi_source = multi_source
        self.benchmark_center = benchmark_center

    def validate(self, *, target_version: str, idempotency_key: str) -> dict[str, Any]:
        existing = self.repository.by_idempotency_key(idempotency_key)
        if existing is not None:
            return existing
        checks = [
            self._runtime_check(),
            self._frontend_and_seed_check(),
            self._model_profile_check(),
            self._prompt_check(),
            self._drain3_check(),
            self._semantic_check(),
            self._multi_source_check(),
            self._benchmark_check(),
        ]
        status = "blocked" if any(item["status"] == "blocked" for item in checks) else (
            "warning" if any(item["status"] == "warning" for item in checks) else "passed"
        )
        summary = {
            "status": status,
            "passed": sum(item["status"] == "passed" for item in checks),
            "warnings": sum(item["status"] == "warning" for item in checks),
            "blocked": sum(item["status"] == "blocked" for item in checks),
            "check_count": len(checks),
        }
        return self.repository.record_validation(
            target_version=target_version,
            idempotency_key=idempotency_key,
            status=status,
            summary=summary,
            checks=checks,
        )

    def overview(self) -> dict[str, Any]:
        latest = self.repository.latest()
        return {
            "schema_version": "release_readiness_overview_v1",
            "latest": latest,
            "history": self.repository.list_history(limit=20)["items"],
            "safe_to_release": bool(latest and latest.get("status") == "passed"),
        }

    def diagnostic(self) -> dict[str, Any]:
        latest = self.repository.latest()
        return sanitize_runtime_metadata({
            "schema_version": "release_readiness_diagnostic_v1",
            "latest_validation_id": latest.get("validation_id") if latest else None,
            "target_version": latest.get("target_version") if latest else None,
            "status": latest.get("status") if latest else "not_validated",
            "summary": latest.get("summary") if latest else {},
            "checks": [
                {"check_id": item["check_id"], "status": item["status"], "code": item["code"]}
                for item in (latest or {}).get("checks", [])
            ],
        })

    def _runtime_check(self) -> dict[str, Any]:
        try:
            readiness = dict(self.runtime_service.readiness())
            ready = bool(readiness.get("ready"))
            return self._check(
                "runtime", "运行时与数据库", "passed" if ready else "blocked",
                "runtime_ready" if ready else "runtime_not_ready",
                "运行时、数据库迁移、目录权限和存储配额已检查" if ready else "运行时就绪检查未通过",
                {"status": readiness.get("status"), "checks": readiness.get("checks"), "storage": readiness.get("storage_usage")},
            )
        except Exception:
            return self._check("runtime", "运行时与数据库", "blocked", "runtime_check_failed", "无法完成运行时就绪检查")

    def _frontend_and_seed_check(self) -> dict[str, Any]:
        required = {
            "frontend_bundle": self.project_root / "frontend" / "dist" / "index.html",
            "ai_harness": self.project_root / "configs" / "ai_harness.yaml",
            "risk_rules": self.project_root / "configs" / "risk_rules.yaml",
            "drain3": self.project_root / "configs" / "drain3_recommended.ini",
            "multi_source": self.project_root / "configs" / "multi_source.yaml",
        }
        missing = sorted(name for name, path in required.items() if not path.is_file())
        return self._check(
            "frontend_bundle", "前端与基础配置", "blocked" if missing else "passed",
            "required_assets_missing" if missing else "required_assets_ready",
            "缺少必需的发布文件" if missing else "前端静态包和基础配置均存在",
            {"required": sorted(required), "missing": missing},
        )

    def _model_profile_check(self) -> dict[str, Any]:
        try:
            profile_id = str(getattr(self.model_profiles, "default_profile_id", "") or "")
            if not profile_id:
                return self._check("model_profile", "默认模型画像", "blocked", "default_profile_missing", "未设置默认模型画像")
            profile = self.model_profiles.get(profile_id)
            enabled = bool(self._value(profile, "enabled", False))
            connection_id = str(self._value(profile, "connection_id", "") or "")
            connection = next((item for item in self.connections.list() if str(item.get("connection_id")) == connection_id), None)
            if not enabled:
                return self._check("model_profile", "默认模型画像", "blocked", "default_profile_disabled", "默认模型画像已停用", {"profile_id": profile_id})
            if not connection or not bool(connection.get("enabled")):
                return self._check("model_profile", "默认模型画像", "blocked", "connection_unavailable", "默认模型连接不存在或已停用", {"profile_id": profile_id, "connection_id": connection_id})
            provider = str(connection.get("provider") or "")
            if provider == "openai_compatible" and not bool(connection.get("api_key_configured")):
                return self._check("model_profile", "默认模型画像", "blocked", "api_key_not_configured", "远端模型连接未配置所需 API Key 环境变量", {"profile_id": profile_id, "connection_id": connection_id, "provider": provider})
            missing = [str(name) for name, value in dict(connection.get("credential_envs_configured") or {}).items() if not value]
            if provider == "extension" and missing:
                return self._check("model_profile", "默认模型画像", "blocked", "extension_credentials_missing", "扩展模型连接缺少所需凭据环境变量", {"profile_id": profile_id, "connection_id": connection_id, "provider": provider, "missing_credential_names": missing})
            return self._check("model_profile", "默认模型画像", "passed", "default_profile_ready", "默认模型画像与连接可用于新分析任务", {"profile_id": profile_id, "connection_id": connection_id, "provider": provider})
        except Exception:
            return self._check("model_profile", "默认模型画像", "blocked", "profile_check_failed", "无法读取默认模型画像或连接配置")

    def _prompt_check(self) -> dict[str, Any]:
        try:
            prompt = self.prompt_registry.get_default("feature_extract")
            prompt_id = str(self._value(prompt, "prompt_id", "") or "")
            if not prompt_id:
                raise ValueError("missing prompt")
            return self._check("prompt", "特征识别 Prompt", "passed", "default_prompt_ready", "默认特征识别 Prompt 已加载", {"prompt_id": prompt_id})
        except Exception:
            return self._check("prompt", "特征识别 Prompt", "blocked", "default_prompt_missing", "未找到可用的默认特征识别 Prompt")

    def _drain3_check(self) -> dict[str, Any]:
        try:
            snapshot = dict(self.drain_quality.configs.active_snapshot())
            return self._check("drain3", "Drain3 配置", "passed", "drain3_config_ready", "活动 Drain3 配置已加载", {key: snapshot.get(key) for key in ("config_id", "version", "content_hash", "status")})
        except Exception:
            return self._check("drain3", "Drain3 配置", "blocked", "drain3_config_unavailable", "无法加载活动 Drain3 配置")

    def _semantic_check(self) -> dict[str, Any]:
        try:
            snapshot = dict(self.semantic_dictionaries.active_snapshot())
            versions = dict(snapshot.get("versions") or {})
            if not versions:
                return self._check("semantic_dictionary", "语义词典", "warning", "semantic_dictionary_empty", "当前没有活动语义词典版本", {"dictionary_count": 0})
            return self._check("semantic_dictionary", "语义词典", "passed", "semantic_dictionary_ready", "活动语义词典已加载", {"dictionary_count": len(versions), "dictionary_ids": sorted(versions)})
        except Exception:
            return self._check("semantic_dictionary", "语义词典", "blocked", "semantic_dictionary_unavailable", "无法加载活动语义词典")

    def _multi_source_check(self) -> dict[str, Any]:
        try:
            rules = list(dict(self.multi_source.rules_view()).get("items") or [])
            enabled = sum(bool(item.get("enabled")) for item in rules if isinstance(item, Mapping))
            return self._check(
                "multi_source", "多来源关联规则", "passed" if enabled else "warning",
                "multi_source_rules_ready" if enabled else "multi_source_rules_empty",
                "已加载启用的确定性多来源关联规则" if enabled else "没有启用的多来源关联规则，需要人工确认是否符合发布预期",
                {"rule_count": len(rules), "enabled_rule_count": enabled},
            )
        except Exception:
            return self._check("multi_source", "多来源关联规则", "blocked", "multi_source_unavailable", "无法读取多来源关联规则")

    def _benchmark_check(self) -> dict[str, Any]:
        try:
            overview = dict(self.benchmark_center.overview())
            blocked = int(dict(overview.get("gate_counts") or {}).get("blocked") or 0)
            return self._check(
                "benchmark", "评测与基线", "warning" if blocked else "passed",
                "benchmark_gate_blocked" if blocked else "benchmark_overview_ready",
                "存在被阻断的评测门禁，需要人工复核" if blocked else "评测中心概览已加载，未发现被阻断门禁",
                {key: overview.get(key) for key in ("suite_count", "run_count", "completed_run_count", "gate_counts", "failure_count")},
            )
        except Exception:
            return self._check("benchmark", "评测与基线", "warning", "benchmark_overview_unavailable", "无法读取评测概览，需要人工确认")

    @staticmethod
    def _value(item: Any, name: str, default: Any = None) -> Any:
        return item.get(name, default) if isinstance(item, Mapping) else getattr(item, name, default)

    @staticmethod
    def _check(
        check_id: str,
        title: str,
        status: str,
        code: str,
        message: str,
        evidence: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "check_id": check_id,
            "title": title,
            "status": status,
            "code": code,
            "message": message,
            "evidence": sanitize_runtime_metadata(dict(evidence or {})),
        }
