from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
import uuid

import pytest

from logrisk.application import ApplicationConfig, build_application_container


def test_postgres_cursor_supports_store_iteration() -> None:
    from logrisk.database import PostgresCursor

    class Cursor:
        rowcount = 2

        def fetchall(self):
            return [{"value": "one"}, {"value": "two"}]

    assert [row["value"] for row in PostgresCursor(Cursor())] == ["one", "two"]


@pytest.mark.skipif(not os.getenv("LOGRISK_TEST_POSTGRES_URL"), reason="未设置 LOGRISK_TEST_POSTGRES_URL")
def test_application_container_seeds_postgres_boolean_columns(tmp_path: Path) -> None:
    psycopg = pytest.importorskip("psycopg")
    base_url = os.environ["LOGRISK_TEST_POSTGRES_URL"]
    schema = "logrisk_seed_" + uuid.uuid4().hex[:12]
    parts = urlsplit(base_url)
    query = parse_qsl(parts.query, keep_blank_values=True) + [("options", "-c search_path=" + schema)]
    target_url = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query, quote_via=quote), parts.fragment))
    with psycopg.connect(base_url, autocommit=True) as connection:
        connection.execute('CREATE SCHEMA "' + schema + '"')
    try:
        build_application_container(
            ApplicationConfig(
                project_root=Path.cwd(),
                state_root=tmp_path / "state",
                output_root=tmp_path / "output",
                database_provider="postgres",
                database_url=target_url,
                shared_root=tmp_path / "shared",
                import_legacy_state=False,
                feature_jobs_auto_start=False,
                interrupt_feature_jobs=False,
                migrate_database=True,
            )
        )
        with psycopg.connect(target_url) as connection:
            row = connection.execute("SELECT enabled FROM risk_semantic_rules LIMIT 1").fetchone()
        assert row is not None
        assert row[0] is True
    finally:
        with psycopg.connect(base_url, autocommit=True) as connection:
            connection.execute('DROP SCHEMA IF EXISTS "' + schema + '" CASCADE')
