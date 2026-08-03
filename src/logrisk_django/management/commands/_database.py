from __future__ import annotations

from logrisk.database import Database, create_database
from logrisk_django.service_factory import get_config


def configured_database(*, migrate: bool) -> Database:
    config = get_config().application_config()
    return create_database(
        provider=config.database_provider or "sqlite",
        sqlite_path=config.database_path or config.state_root / "logrisk.sqlite3",
        state_root=config.state_root,
        database_url=config.database_url,
        migrate=migrate,
    )
