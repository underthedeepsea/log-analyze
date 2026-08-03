from __future__ import annotations

from typing import Any

from django.conf import settings
from django.utils.module_loading import import_string

from logrisk.application import ApiFacade, ApplicationContainer, build_application_container
from logrisk.orchestration import AirflowOrchestrator
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
        _facade = ApiFacade(get_container(), version="1.31.0")
    return _facade


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


def clear_cached_container() -> None:
    """Test-only hook; production never hot-switches LOGRISK storage."""
    global _container, _facade
    _container = None
    _facade = None
