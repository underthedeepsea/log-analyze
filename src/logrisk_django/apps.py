from __future__ import annotations

from django.apps import AppConfig


class LogriskDjangoAppConfig(AppConfig):
    name = "logrisk_django"
    verbose_name = "LOGRISK"
    default_auto_field = "django.db.models.BigAutoField"
