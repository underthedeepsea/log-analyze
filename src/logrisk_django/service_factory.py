from __future__ import annotations

from typing import Any

from django.conf import settings
from django.utils.module_loading import import_string

from logrisk.application import ApplicationContainer, build_application_container
from logrisk_django.identity import IdentityResolver
from logrisk_django.settings import LogriskConfig


_container: ApplicationContainer | None = None


def get_config() -> LogriskConfig:
    return LogriskConfig.from_django_settings(settings)


def get_container() -> ApplicationContainer:
    global _container
    if _container is None:
        _container = build_application_container(get_config().application_config())
    return _container


def get_identity_resolver() -> IdentityResolver:
    resolver_type: Any = import_string(get_config().identity_resolver)
    resolver = resolver_type()
    if not callable(getattr(resolver, "resolve", None)):
        raise TypeError("identity_resolver 必须实现 resolve(request)")
    return resolver


def clear_cached_container() -> None:
    """Test-only hook; production never hot-switches LOGRISK storage."""
    global _container
    _container = None
