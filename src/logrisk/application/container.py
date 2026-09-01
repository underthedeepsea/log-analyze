from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import yaml

from logrisk.approval_dedup import InMemoryApprovalGroupStore
from logrisk.ai_eval.runner import load_cases
from logrisk.agentic import (
    AgentRepository, AgentRuntime, AgentService, ModelAgentPlanner, WorkflowLimits,
    WorkflowRepository, WorkflowScheduler, WorkflowService, WorkflowWorker, build_role_registry,
)
from logrisk.agentic.tools import build_agent_tool_registry
from logrisk.ai_harness.connections import ConnectionStore
from logrisk.ai_harness.evaluator import evaluate_feature_output
from logrisk.ai_harness.model_profile import ModelProfileRegistry
from logrisk.ai_harness.prompt_registry import PromptTemplate, SQLitePromptRegistry
from logrisk.ai_harness.providers import create_model_client
from logrisk.ai_harness.trace_logger import AITraceLogger
from logrisk.artifact_storage import SharedArtifactStore
from logrisk.benchmark_center import BenchmarkRepository, BenchmarkService
from logrisk.database import Database, create_database, utc_now
from logrisk.database_config import DatabaseConnectionSettings, resolve_database_runtime
from logrisk.drain_eval.service import DrainQualityService
from logrisk.feature_extractor_ollama import FEATURE_RESPONSE_SCHEMA, _validate_model_feature
from logrisk.feature_jobs import FeatureJobError, FeatureJobManager
from logrisk.incremental_sources import FileIncrementalSource
from logrisk.input_jobs import InputJobConfig
from logrisk.knowledge_packages.service import KnowledgePackageService
from logrisk.knowledge_packages.asset_adapters import build_domain_adapter_registry
from logrisk.large_file_pipeline import run_large_file_pipeline
from logrisk.legacy_import import LegacyStateImporter
from logrisk.multi_source.config import load_multi_source_config
from logrisk.multi_source.repository import MultiSourceRepository
from logrisk.multi_source.service import MultiSourceService
from logrisk.node_risk import NodeRiskService
from logrisk.orchestration import (
    InputOrchestrationRepository,
    InputOrchestrationService,
    OrchestrationRepository,
    OrchestrationService,
)
from logrisk.observability import ObservabilityRepository, PromptSnapshotResolver, ReplayError, ReplayService, SpanRecorder
from logrisk.release_readiness import ReleaseReadinessRepository, ReleaseReadinessService
from logrisk.risk_semantics import RiskSemanticService
from logrisk.rule_governance import RuleGovernanceRepository, RuleGovernanceService
from logrisk.runtime.config import RuntimeConfig, RuntimeConfigError
from logrisk.runtime.repository import RuntimeRepository
from logrisk.runtime.service import RuntimeService
from logrisk.semantic.store import SemanticDictionaryStore
from logrisk.sqlite_stores import (
    SQLiteAICache,
    SQLiteAITraceLogger,
    SQLiteApprovalGroupStore,
    SQLiteApprovedRuleStore,
    SQLiteDrainQualityService,
    SQLiteFeatureJobStore,
    SQLiteInputJobStore,
    SQLiteProcessingMetricsStore,
    SQLiteSemanticDictionaryStore,
    SQLiteUploadSessionStore,
)
from logrisk.streaming_state import StreamingConflictError, StreamingStateRepository
from logrisk.upload_sessions import UploadConfig
from pipeline.manual_import_pipeline import analyze_records


@dataclass(frozen=True)
class ApplicationConfig:
    """Explicit runtime inputs shared by HTTP servers, Django, and Airflow."""

    project_root: Path
    state_root: Path
    output_root: Path
    database_provider: str | None = None
    database_url: str | None = None
    database_path: Path | None = None
    shared_root: Path | None = None
    default_model: str = "qwen3:1.7b"
    default_ollama_url: str = "http://127.0.0.1:11434"
    default_timeout: float = 120.0
    drain_quality_root: Path | None = None
    semantic_root: Path | None = None
    runtime_config: RuntimeConfig | None = None
    runtime_config_path: Path | None = None
    import_legacy_state: bool = False
    interrupt_streaming_tasks: bool = False
    feature_jobs_auto_start: bool = True
    interrupt_feature_jobs: bool = True
    migrate_database: bool = True
    agentic_enabled: bool = False
    agent_workflows_enabled: bool = False

    @classmethod
    def for_test(cls, *, project_root: str | Path, state_root: str | Path) -> "ApplicationConfig":
        root = Path(project_root).resolve()
        state = Path(state_root).resolve()
        return cls(
            project_root=root,
            state_root=state,
            output_root=root / "output",
            database_path=state / "logrisk.sqlite3",
        )


@dataclass
class ApplicationContainer:
    """Shared LOGRISK services with no dependency on an HTTP framework."""

    config: ApplicationConfig
    database: Database
    database_runtime: Any
    database_settings: DatabaseConnectionSettings
    runtime_config: RuntimeConfig
    runtime_repository: RuntimeRepository
    runtime_service: RuntimeService
    connections: ConnectionStore
    model_profiles: ModelProfileRegistry
    prompt_registry: SQLitePromptRegistry
    trace_logger: SQLiteAITraceLogger
    observability_repository: ObservabilityRepository
    feature_jobs: FeatureJobManager
    orchestration: OrchestrationService
    input_orchestration: InputOrchestrationService
    rule_governance: RuleGovernanceService
    replay_service: ReplayService
    upload_store: SQLiteUploadSessionStore
    input_jobs: SQLiteInputJobStore
    artifact_store: SharedArtifactStore
    knowledge_packages: KnowledgePackageService
    agent_runs: AgentService | None
    agent_workflows: WorkflowService | None
    streaming_state: StreamingStateRepository
    drain_quality: Any
    semantic_dictionaries: Any
    risk_semantics: RiskSemanticService
    node_risks: NodeRiskService
    multi_source: MultiSourceService
    benchmark_center: BenchmarkService
    release_readiness: ReleaseReadinessService
    govern_drain_result: Callable[[dict[str, Any]], dict[str, Any]] | None = None
    input_analyzer: Callable[..., dict[str, Any]] | None = None
    input_analyzer_accepts_config: bool = True
    run_input_job: Callable[[str], None] | None = None

    def recover_agent_runs(self, submit: Callable[[str], None] | None = None) -> list[str]:
        """Recover persisted local Agent runs only when the feature is explicitly enabled."""
        if self.agent_runs is None:
            return []
        run_ids = self.agent_runs.recover_active_runs()
        if submit:
            for run_id in run_ids:
                submit(run_id)
        return run_ids

    def recover_agent_workflows(self, submit: Callable[[str], None] | None = None) -> list[str]:
        if self.agent_workflows is None:
            return []
        run_ids = self.agent_workflows.recover_active_runs()
        if submit:
            for run_id in run_ids:
                submit(run_id)
        return run_ids


def build_application_container(
    config: ApplicationConfig,
    *,
    manager: FeatureJobManager | None = None,
    input_analyzer: Callable[[list[dict[str, Any]]], dict[str, Any]] | None = None,
) -> ApplicationContainer:
    """Build reusable domain services without binding an HTTP socket."""
    root = config.project_root.resolve()
    state_root = config.state_root.resolve()
    output_root = config.output_root.resolve()
    shared_root = Path(config.shared_root or os.getenv("LOGRISK_SHARED_ROOT") or state_root).resolve()
    database_settings = DatabaseConnectionSettings(state_root / "database_connection.json")
    database_runtime = resolve_database_runtime(
        provider=config.database_provider,
        database_url=config.database_url,
        database_path=config.database_path or os.getenv("LOGRISK_DB_PATH") or state_root / "logrisk.sqlite3",
        settings=database_settings,
    )
    database = create_database(
        provider=database_runtime.provider,
        sqlite_path=database_runtime.sqlite_path,
        database_url=database_runtime.database_url,
        state_root=state_root,
        migrate=config.migrate_database,
    )
    runtime_config = config.runtime_config or _load_runtime_config(config, root)

    connections = ConnectionStore(database)
    connections.seed_defaults(config.default_ollama_url)
    profiles = ModelProfileRegistry(root / "configs" / "model_profiles.yaml", database=database)
    prompts = SQLitePromptRegistry(database, root / "prompts", root / "configs" / "ai_harness.yaml")
    traces = SQLiteAITraceLogger(database)
    observability_repository = ObservabilityRepository(database)
    span_recorder = SpanRecorder(observability_repository)
    ai_cache = SQLiteAICache(database)
    drain_quality = (
        DrainQualityService(config.drain_quality_root, root / "configs" / "drain3_profiles", root / "configs" / "drain3_recommended.ini")
        if config.drain_quality_root
        else SQLiteDrainQualityService(database, root / "configs" / "drain3_profiles", root / "configs" / "drain3_recommended.ini")
    )
    semantic_dictionaries = (
        SemanticDictionaryStore(config.semantic_root, root / "configs" / "semantic_dictionary")
        if config.semantic_root
        else SQLiteSemanticDictionaryStore(database, root / "configs" / "semantic_dictionary")
    )
    risk_semantics = RiskSemanticService(database, root / "configs" / "risk_semantics" / "builtin.yaml")
    node_risks = NodeRiskService(database, root / "configs" / "node_risk.yaml")
    multi_source_config = load_multi_source_config(root / "configs" / "multi_source.yaml")
    multi_source = MultiSourceService(
        MultiSourceRepository(database),
        aliases=multi_source_config["aliases"],
        rules=multi_source_config["rules"],
        enabled=multi_source_config["enabled"],
    )
    if config.import_legacy_state:
        LegacyStateImporter(database, state_root, output_root / "uploads").run()

    def configured_extractor(entity: dict[str, Any], **kwargs: Any) -> list[dict[str, Any]]:
        import logrisk.feature_extractor_ollama as feature_extractor

        profile_snapshot = kwargs.get("profile_snapshot")
        profile = profiles.from_snapshot(profile_snapshot) if isinstance(profile_snapshot, dict) else profiles.get(kwargs.get("model_profile_id"))
        connection_snapshot = kwargs.get("connection_snapshot")
        connection = dict(connection_snapshot) if isinstance(connection_snapshot, dict) else connections.get(profile.connection_id)
        prompt_snapshot = kwargs.get("prompt_snapshot")
        prompt_template = PromptTemplate(**prompt_snapshot) if isinstance(prompt_snapshot, dict) else None
        if not connection["enabled"]:
            raise FeatureJobError(f"模型连接已停用: {profile.connection_id}")
        feature_extractor.PROMPT_REGISTRY = prompts
        feature_extractor.TRACE_LOGGER = traces
        feature_extractor.AI_CACHE = ai_cache
        return feature_extractor.extract_features_for_entity(
            entity,
            model=profile.model,
            base_url=connection["base_url"],
            timeout=float(kwargs.get("timeout") or connection["timeout_seconds"]),
            model_client=create_model_client(connection),
            prompt_id=kwargs.get("prompt_id") or profile.default_prompt_id,
            prompt_template=prompt_template,
            job_id=kwargs.get("job_id"),
            cache_enabled=bool(kwargs.get("cache_enabled", True)),
            model_profile_id=profile.profile_id,
            provider=connection["provider"],
            model_profile=profile,
            connection_snapshot=connection,
        )

    def validate_replay_output(snapshot: dict[str, Any], output: dict[str, Any]) -> dict[str, Any]:
        source = dict(snapshot.get("source_trace") or {})
        evidence = dict(source.get("input_evidence") or {})
        known_hashes = {
            str(item.get("template_hash"))
            for item in evidence.get("templates") or []
            if item.get("template_hash")
        }
        try:
            features = [_validate_model_feature(item, known_hashes) for item in output.get("features") or []]
        except Exception as exc:
            return {
                "parsed_output": output,
                "validation_result": {"valid": False, "errors": [str(exc)], "warnings": []},
                "evaluator_result": {"passed": False, "errors": [], "warnings": [], "score": 0.0},
            }
        evaluator_results = [
            evaluate_feature_output(
                feature=feature,
                entity={"entity_id": (evidence.get("entity") or {}).get("id")},
                evidence=evidence,
            )
            for feature in features
        ]
        errors = [error for result in evaluator_results for error in result.get("errors", [])]
        return {
            "parsed_output": {"features": features},
            "validation_result": {"valid": True, "errors": [], "warnings": []},
            "evaluator_result": {
                "passed": not errors,
                "errors": errors,
                "warnings": [warning for result in evaluator_results for warning in result.get("warnings", [])],
                "score": min((float(result.get("score") or 0) for result in evaluator_results), default=1.0),
            },
        }

    def replay_model(snapshot: dict[str, Any]) -> dict[str, Any]:
        source = dict(snapshot.get("source_trace") or {})
        evidence = dict(source.get("input_evidence") or {})
        prompt = dict(snapshot.get("prompt") or {})
        connection = dict(source.get("connection_snapshot") or {})
        if not evidence or not prompt.get("prompt_content") or not connection:
            raise ReplayError(
                "来源 Trace 缺少锁定 Evidence、Prompt 或连接快照",
                code="replay_snapshot_incomplete",
                status_code=422,
            )
        if not connection.get("enabled"):
            raise ReplayError("来源模型连接已停用", code="connection_disabled", status_code=409)
        client = create_model_client(connection)
        started = time.perf_counter()
        output = client.generate_json(
            [
                {"role": "system", "content": prompt["prompt_content"]},
                {"role": "user", "content": json.dumps(evidence, ensure_ascii=False, separators=(",", ":"))},
            ],
            FEATURE_RESPONSE_SCHEMA,
            model=str(source.get("model") or ""),
            timeout=float(connection.get("timeout_seconds") or 120),
            options=dict(source.get("model_options") or {}),
        )
        return {
            **validate_replay_output(snapshot, output),
            "latency_ms": int((time.perf_counter() - started) * 1000),
            "usage": dict(getattr(client, "last_metadata", {}).get("usage") or {}),
        }

    benchmark_cases = load_cases()

    def benchmark_asset_inventory() -> dict[str, int]:
        table_names = {
            "ai_traces": "ai_traces",
            "model_profiles": "model_profiles",
            "prompt_templates": "prompt_templates",
            "drain_eval_runs": "drain_eval_runs",
            "drain_templates": "drain_templates",
            "ai_cache_entries": "ai_cache_entries",
        }
        with database.connect() as connection:
            counts = {
                key: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for key, table in table_names.items()
            }
        counts["canonical_eval_cases"] = len(benchmark_cases)
        return counts

    benchmark_center = BenchmarkService(
        BenchmarkRepository(database),
        canonical_cases=benchmark_cases,
        real_extractor=configured_extractor,
        asset_inventory=benchmark_asset_inventory,
        profile_resolver=lambda profile_id: profiles.get(profile_id).public_dict(),
        connection_resolver=connections.get,
        prompt_resolver=lambda prompt_id: dict(prompts.load(prompt_id).__dict__),
    )
    runtime_repository = RuntimeRepository(database)
    runtime_service = RuntimeService(
        database,
        state_root=state_root,
        output_root=output_root,
        config=runtime_config,
        repository=runtime_repository,
    )
    rule_store = SQLiteApprovedRuleStore(database)
    approval_group_store = SQLiteApprovalGroupStore(database)
    if manager is not None and manager.rule_store is None:
        manager.rule_store = rule_store
    if manager is not None and isinstance(getattr(manager, "approval_group_store", None), InMemoryApprovalGroupStore):
        manager.approval_group_store = approval_group_store
    feature_jobs = manager or FeatureJobManager(
        extractor=configured_extractor,
        rule_store=rule_store,
        approval_group_store=approval_group_store,
        metrics_store=SQLiteProcessingMetricsStore(database),
        persistence=SQLiteFeatureJobStore(database),
        observability=span_recorder,
        auto_start=config.feature_jobs_auto_start,
        interrupt_on_restore=config.interrupt_feature_jobs,
    )
    feature_jobs.observability = span_recorder
    rule_governance = RuleGovernanceService(RuleGovernanceRepository(database))
    orchestration = OrchestrationService(OrchestrationRepository(database))
    input_orchestration = InputOrchestrationService(InputOrchestrationRepository(database))
    artifact_store = SharedArtifactStore(shared_root)
    knowledge_packages = KnowledgePackageService(
        database,
        artifact_store,
        app_version="1.35.2",
        adapters=build_domain_adapter_registry(
            prompt_registry=prompts,
            drain_quality=drain_quality,
            semantic_dictionaries=semantic_dictionaries,
            risk_semantics=risk_semantics,
        ),
    )
    agent_repository = AgentRepository(database)

    def agent_planner(run: dict[str, Any]) -> ModelAgentPlanner:
        snapshot = dict(run.get("locked_snapshot") or {})
        profile = profiles.from_snapshot(dict(snapshot["profile_snapshot"]))
        connection = dict(snapshot["connection_snapshot"])
        prompt = prompts.load_by_hash(str(snapshot["prompt_id"]), str(snapshot["prompt_sha256"]))
        if not connection.get("enabled"):
            raise FeatureJobError("模型连接已停用")
        return ModelAgentPlanner(
            create_model_client(connection),
            model=profile.model,
            prompt_content=prompt.content,
            timeout=float(run["timeout_seconds"]),
            options=profile.build_model_options(),
        )

    agent_runs = None
    agent_workflows = None
    if config.agentic_enabled:
        agent_tools = build_agent_tool_registry(feature_jobs, rule_governance, knowledge_packages)
        agent_runs = AgentService(agent_repository, AgentRuntime(agent_repository, agent_planner, agent_tools))
        if config.agent_workflows_enabled:
            workflow_settings = _load_agent_workflow_settings(root)
            workflow_repository = WorkflowRepository(database)
            workflow_worker = WorkflowWorker(workflow_repository, agent_runs)
            workflow_scheduler = WorkflowScheduler(workflow_repository, workflow_worker)
            agent_workflows = WorkflowService(
                workflow_repository, workflow_scheduler,
                build_role_registry(workflow_settings["allowed_roles"]), workflow_settings["limits"],
            )
    upload_store = SQLiteUploadSessionStore(
        UploadConfig(upload_dir=state_root / "uploads", artifact_store=artifact_store), database
    )
    input_jobs = SQLiteInputJobStore(
        InputJobConfig(output_dir=output_root / "uploads", artifact_store=artifact_store), database
    )
    streaming_state = StreamingStateRepository(database)
    if config.interrupt_streaming_tasks:
        streaming_state.interrupt_running_tasks()
    release_readiness = ReleaseReadinessService(
        ReleaseReadinessRepository(database),
        runtime_service=runtime_service,
        project_root=root,
        connections=connections,
        model_profiles=profiles,
        prompt_registry=prompts,
        drain_quality=drain_quality,
        semantic_dictionaries=semantic_dictionaries,
        multi_source=multi_source,
        benchmark_center=benchmark_center,
    )
    replay_service = ReplayService(
        observability_repository,
        traces,
        PromptSnapshotResolver(prompts),
        model_runner=replay_model,
        validation_runner=validate_replay_output,
    )
    container = ApplicationContainer(
        config=config,
        database=database,
        database_runtime=database_runtime,
        database_settings=database_settings,
        runtime_config=runtime_config,
        runtime_repository=runtime_repository,
        runtime_service=runtime_service,
        connections=connections,
        model_profiles=profiles,
        prompt_registry=prompts,
        trace_logger=traces,
        observability_repository=observability_repository,
        feature_jobs=feature_jobs,
        orchestration=orchestration,
        input_orchestration=input_orchestration,
        rule_governance=rule_governance,
        replay_service=replay_service,
        upload_store=upload_store,
        input_jobs=input_jobs,
        artifact_store=artifact_store,
        knowledge_packages=knowledge_packages,
        agent_runs=agent_runs,
        agent_workflows=agent_workflows,
        streaming_state=streaming_state,
        drain_quality=drain_quality,
        semantic_dictionaries=semantic_dictionaries,
        risk_semantics=risk_semantics,
        node_risks=node_risks,
        multi_source=multi_source,
        benchmark_center=benchmark_center,
        release_readiness=release_readiness,
    )

    def govern_drain_result(result: dict[str, Any]) -> dict[str, Any]:
        def active_templates(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
            governed = [container.drain_quality.templates.apply_override(item) for item in items]
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
            container.drain_quality.templates.import_templates(catalog_rows)
        governed = dict(result)
        governed["top_templates"] = active_templates(result.get("top_templates") or [])
        governed["risk_entities"] = [
            dict(entity, top_templates=active_templates(entity.get("top_templates") or []))
            for entity in result.get("risk_entities") or []
        ]
        return governed

    analysis_lock = threading.Lock()

    def default_input_analyzer(
        records: list[dict[str, Any]],
        config_path: str | Path | None = None,
        semantic_snapshot: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with analysis_lock:
            result = analyze_records(
                records,
                config_path=str(config_path or root / "configs" / "drain3_recommended.ini"),
                rules_path=str(root / "configs" / "risk_rules.yaml"),
                state_dir=str(state_root / "dashboard_drain3"),
                semantic_snapshot=semantic_snapshot,
                risk_semantics=container.risk_semantics,
                node_risks=container.node_risks,
            )
            result.setdefault("summary", {})["multi_source"] = container.multi_source.ingest_risk_entities(
                result.get("risk_entities") or [],
                source_job_id=None,
            )
            return result

    def run_input_job(input_job_id: str) -> None:
        job = container.input_jobs.get_job(input_job_id)
        source_path = container.input_jobs.resolve_source_path(job)
        config_path = job.get("drain_config_path") or root / "configs" / "drain3_recommended.ini"
        if not job.get("streaming_task_id"):
            source = FileIncrementalSource(source_path, filename=job["filename"])
            config_hash = hashlib.sha256(Path(config_path).read_bytes()).hexdigest()
            streaming_task = container.streaming_state.create_or_load(
                descriptor=source.descriptor(),
                config_hash=config_hash,
            )
            container.streaming_state.attach_input_job(streaming_task["task_id"], input_job_id)
            job["streaming_task_id"] = streaming_task["task_id"]
            container.input_jobs.write_job(input_job_id, job)
        job.update({"status": "running", "stage": "reading", "started_at": utc_now()})
        container.input_jobs.write_job(input_job_id, job)
        try:
            result = run_large_file_pipeline(
                input_job_id=input_job_id,
                input_path=source_path,
                filename=job["filename"],
                config_path=config_path,
                rules_path=root / "configs" / "risk_rules.yaml",
                state_dir=state_root / "dashboard_drain3_large" / input_job_id,
                progress_callback=lambda progress: container.input_jobs.write_progress(input_job_id, progress),
                semantic_snapshot=job.get("semantic_dictionary_snapshot"),
                risk_semantics=container.risk_semantics,
                node_risks=container.node_risks,
                multi_source=container.multi_source,
                streaming_repository=container.streaming_state,
                resume_task_id=job["streaming_task_id"],
            )
            result = govern_drain_result(result)
            container.input_jobs.write_result(input_job_id, result)
            job.update({"status": "completed", "stage": "completed", "completed_at": utc_now(), "error": None})
            container.input_jobs.write_job(input_job_id, job)
            container.input_jobs.write_progress(input_job_id, {
                "input_job_id": input_job_id,
                "status": "completed",
                "stage": "completed",
                "progress": 1.0,
                "risk_entities": len(result.get("risk_entities") or []),
            })
        except Exception as exc:
            if job.get("streaming_task_id"):
                container.streaming_state.mark_failed(
                    job["streaming_task_id"],
                    str(exc),
                    conflict=isinstance(exc, StreamingConflictError),
                )
            job.update({"status": "failed", "stage": "failed", "completed_at": utc_now(), "error": str(exc)})
            container.input_jobs.write_job(input_job_id, job)
            container.input_jobs.write_progress(input_job_id, {
                "input_job_id": input_job_id,
                "status": "failed",
                "stage": "failed",
                "progress": 1.0,
                "error": str(exc),
            })

    container.govern_drain_result = govern_drain_result
    container.input_analyzer = input_analyzer or default_input_analyzer
    container.input_analyzer_accepts_config = input_analyzer is None
    container.run_input_job = run_input_job
    return container


def _load_agent_workflow_settings(root: Path) -> dict[str, Any]:
    """Load bounded M21 limits from the committed seed config when enabled."""
    path = root / "configs" / "ai_harness.yaml"
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        section = raw.get("agent_workflows") or {}
        if not isinstance(section, dict):
            raise ValueError("agent_workflows 必须是对象")
        allowed = section.get("allowed_roles", ["evidence_specialist", "rule_specialist", "feature_specialist"])
        if not isinstance(allowed, list) or not allowed or any(not isinstance(item, str) for item in allowed) or len(allowed) != len(set(allowed)):
            raise ValueError("allowed_roles 必须是非空字符串数组")
        if section.get("network_access", False) is not False or section.get("approval_policy", "human_required") != "human_required":
            raise ValueError("Agent 工作流必须禁用网络访问并要求人工审批")
        limits = WorkflowLimits(
            max_nodes=int(section.get("max_nodes", 8)),
            max_concurrency=int(section.get("max_concurrency", 4)),
            max_tool_calls=int(section.get("max_tool_calls", 40)),
            max_timeout_seconds=float(section.get("timeout_seconds", 900)),
            max_attempts=int(section.get("max_attempts", 3)),
        )
        if min(limits.max_nodes, limits.max_concurrency, limits.max_tool_calls, limits.max_timeout_seconds, limits.max_attempts) <= 0:
            raise ValueError("工作流限制必须为正数")
        return {"allowed_roles": tuple(allowed), "limits": limits}
    except (OSError, TypeError, ValueError, yaml.YAMLError) as exc:
        raise RuntimeError("Agent 工作流配置无效；请检查 configs/ai_harness.yaml") from exc


def _load_runtime_config(config: ApplicationConfig, root: Path) -> RuntimeConfig:
    selected_runtime_config = config.runtime_config_path or root / "configs" / "runtime.yaml"
    try:
        runtime_payload = yaml.safe_load(Path(selected_runtime_config).read_text(encoding="utf-8")) or {}
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise RuntimeConfigError("运行时配置文件无效") from exc
    return RuntimeConfig.from_mapping({"runtime": runtime_payload.get("runtime", {})})
