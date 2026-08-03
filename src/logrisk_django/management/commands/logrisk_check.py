from __future__ import annotations

import json

from django.core.management.base import BaseCommand

from logrisk.database import MigrationManager
from logrisk.runtime.health import directory_writable
from logrisk_django.management.commands._database import configured_database
from logrisk_django.service_factory import get_config


class Command(BaseCommand):
    help = "检查 LOGRISK 数据库迁移和共享目录，不应用任何迁移。"

    def add_arguments(self, parser: object) -> None:
        parser.add_argument("--json", action="store_true", dest="json_output")

    def handle(self, *args: object, **options: object) -> str:
        config = get_config()
        database_status = MigrationManager(configured_database(migrate=False)).status().public_dict()
        try:
            shared_root_ready = directory_writable(config.shared_root)
        except OSError:
            shared_root_ready = False
        payload = {
            "ready": bool(database_status["ready"] and shared_root_ready),
            "database": database_status,
            "shared_root": {"ready": shared_root_ready},
            "airflow": {"configured": bool(config.airflow_base_url), "online": "not_checked"},
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True) if options.get("json_output") else _plain(payload)


def _plain(payload: dict[str, object]) -> str:
    database = dict(payload["database"])
    return "LOGRISK 检查: " + ("就绪" if payload["ready"] else "未就绪") + (
        f"；待迁移 {database['pending_migrations']} 项"
    )
