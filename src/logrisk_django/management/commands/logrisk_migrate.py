from __future__ import annotations

import json

from django.core.management.base import BaseCommand

from logrisk.database import MigrationManager
from logrisk_django.management.commands._database import configured_database


class Command(BaseCommand):
    help = "显式应用 LOGRISK 数据库迁移；Django 启动不会自动执行此操作。"

    def add_arguments(self, parser: object) -> None:
        parser.add_argument("--check", action="store_true")
        parser.add_argument("--json", action="store_true", dest="json_output")

    def handle(self, *args: object, **options: object) -> str:
        manager = MigrationManager(configured_database(migrate=False))
        status = manager.status() if options.get("check") else manager.apply()
        payload = {"database": status.public_dict()}
        return json.dumps(payload, ensure_ascii=False, sort_keys=True) if options.get("json_output") else _plain(payload)


def _plain(payload: dict[str, object]) -> str:
    database = dict(payload["database"])
    return f"LOGRISK 数据库迁移完成；待迁移 {database['pending_migrations']} 项"
