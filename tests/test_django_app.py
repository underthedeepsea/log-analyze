from __future__ import annotations

import os

import django


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_test_project.settings")
django.setup()


def test_django_app_loads_without_models_or_migrations() -> None:
    from django.apps import apps

    config = apps.get_app_config("logrisk_django")

    assert config.name == "logrisk_django"
    assert list(config.get_models()) == []


def test_django_settings_are_strict_and_do_not_expose_database_url(monkeypatch) -> None:
    from django.conf import settings
    from logrisk_django.settings import LogriskConfig

    monkeypatch.setenv("LOGRISK_DATABASE_URL", "postgresql://user:never-expose@db/logrisk")
    config = LogriskConfig.from_django_settings(settings)

    assert config.database_url_env == "LOGRISK_DATABASE_URL"
    assert "never-expose" not in repr(config)


def test_django_identity_uses_existing_authenticated_user_groups_only() -> None:
    from django.test import RequestFactory
    from logrisk_django.identity import DjangoUserIdentityResolver

    class Groups:
        def values_list(self, _name: str, flat: bool) -> list[str]:
            assert flat is True
            return ["logrisk:operator", "security:viewer"]

    class User:
        is_authenticated = True
        groups = Groups()

        @staticmethod
        def get_username() -> str:
            return "pacas-alice"

    request = RequestFactory().get("/api/health", HTTP_X_REQUEST_ID="request-1")
    request.user = User()

    identity = DjangoUserIdentityResolver().resolve(request)

    assert identity.actor == "pacas-alice"
    assert identity.roles == ("logrisk:operator", "security:viewer")
    assert identity.request_id == "request-1"
    assert identity.source == "django"
