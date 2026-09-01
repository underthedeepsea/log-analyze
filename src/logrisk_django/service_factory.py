from __future__ import annotations

from typing import Any

from django.conf import settings
from django.utils.module_loading import import_string

from logrisk.application import ApiFacade, ApplicationContainer, build_application_container
from logrisk.orchestration import AirflowOrchestrator, AirflowOrchestratorError
from logrisk_django.identity import IdentityResolver
from logrisk_django.settings import LogriskConfig


_container: ApplicationContainer | None = None
_facade: ApiFacade | None = None


def get_config() -> LogriskConfig:
    return LogriskConfig.from_django_settings(settings)


def get_container() -> ApplicationContainer:
    global _container
    if _container is None:
        _container = build_application_container(get_config().application_config())
    return _container


def get_facade() -> ApiFacade:
    global _facade
    if _facade is None:
        _facade = ApiFacade(
            get_container(),
            version="1.36.1",
            service_resolver=_resolve_facade_service,
        )
    return _facade


def _resolve_facade_service(name: str, default: Any) -> Any:
    if name == "airflow_readiness":
        return get_airflow_readiness()
    if name == "airflow_orchestrator":
        return get_airflow_orchestrator()
    if name == "input_airflow_orchestrator":
        return get_input_airflow_orchestrator()
    return default


def get_identity_resolver() -> IdentityResolver:
    resolver_type: Any = import_string(get_config().identity_resolver)
    resolver = resolver_type()
    if not callable(getattr(resolver, "resolve", None)):
        raise TypeError("identity_resolver 必须实现 resolve(request)")
    return resolver


def get_airflow_orchestrator() -> AirflowOrchestrator:
    config = get_config()
    return AirflowOrchestrator(
        config.airflow_base_url,
        config.airflow_dag_id,
        timeout=config.airflow_timeout_seconds,
        authorization_env=config.airflow_authorization_env,
    )


def get_input_airflow_orchestrator() -> AirflowOrchestrator:
    config = get_config()
    return AirflowOrchestrator(
        config.airflow_base_url,
        config.airflow_input_dag_id,
        timeout=config.airflow_timeout_seconds,
        authorization_env=config.airflow_authorization_env,
    )


def get_agent_airflow_orchestrator() -> AirflowOrchestrator:
    config = get_config()
    return AirflowOrchestrator(
        config.airflow_base_url,
        config.airflow_agent_dag_id,
        timeout=config.airflow_timeout_seconds,
        authorization_env=config.airflow_authorization_env,
    )


def get_agent_workflow_airflow_orchestrator() -> AirflowOrchestrator:
    config = get_config()
    return AirflowOrchestrator(
        config.airflow_base_url, config.airflow_agent_workflow_dag_id,
        timeout=config.airflow_timeout_seconds, authorization_env=config.airflow_authorization_env,
    )


def get_airflow_readiness() -> dict[str, Any]:
    """Probe both production DAGs and return only stable, non-sensitive status fields."""
    config = get_config()
    probes = [
        ("analysis", config.airflow_dag_id, get_airflow_orchestrator),
        ("input_preprocess", config.airflow_input_dag_id, get_input_airflow_orchestrator),
    ]
    if config.agentic_enabled:
        probes.append(("agent", config.airflow_agent_dag_id, get_agent_airflow_orchestrator))
    if config.agent_workflows_enabled:
        probes.append(("agent_workflow", config.airflow_agent_workflow_dag_id, get_agent_workflow_airflow_orchestrator))
    statuses: list[dict[str, Any]] = []
    for role, expected_dag_id, factory in probes:
        try:
            client = factory()
            health = client.health()
            dag_id = str(getattr(health, "dag_id", "") or client.dag_id)
            paused = bool(getattr(health, "is_paused", False))
            statuses.append({
                "role": role,
                "dag_id": dag_id,
                "online": True,
                "dag_registered": dag_id == client.dag_id,
                "paused": paused,
            })
        except AirflowOrchestratorError as exc:
            statuses.append({
                "role": role,
                "dag_id": expected_dag_id,
                "online": False,
                "dag_registered": False,
                "paused": False,
                "code": exc.code,
            })
        except Exception:
            statuses.append({
                "role": role,
                "dag_id": expected_dag_id,
                "online": False,
                "dag_registered": False,
                "paused": False,
                "code": "airflow_check_failed",
            })
    online = all(bool(item["online"]) for item in statuses)
    registered = all(bool(item["dag_registered"]) for item in statuses)
    ready = online and registered and not any(bool(item["paused"]) for item in statuses)
    return {
        "status": "ready" if ready else "not_ready",
        "ready": ready,
        "online": online,
        "dag_registered": registered,
        "dags": [str(item["dag_id"]) for item in statuses if item.get("dag_id")],
        "details": statuses,
    }


def clear_cached_container() -> None:
    """Test-only hook; production never hot-switches LOGRISK storage."""
    global _container, _facade
    _container = None
    _facade = None
