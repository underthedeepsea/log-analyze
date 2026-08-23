from __future__ import annotations

import hashlib
import importlib
import json
import re
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Protocol, Sequence


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class DatabaseError(RuntimeError):
    """A storage error that is safe to expose without a connection URL."""


class Database(Protocol):
    provider: str
    state_root: Path

    def connect(self) -> Any: ...

    def transaction(self) -> Any: ...


@dataclass(frozen=True)
class MigrationStatus:
    provider: str
    applied_versions: tuple[str, ...]
    pending_versions: tuple[str, ...]
    changed_versions: tuple[str, ...]
    latest_version: str | None

    def public_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "applied_migrations": len(self.applied_versions),
            "pending_migrations": len(self.pending_versions),
            "changed_migrations": list(self.changed_versions),
            "latest_version": self.latest_version,
            "ready": not self.pending_versions and not self.changed_versions,
        }


def migration_status(database: Database) -> MigrationStatus:
    """Inspect migration state without applying any schema change."""
    migrations_dir = Path(getattr(database, "migrations_dir"))
    expected = {
        path.name.split("_", 1)[0]: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(migrations_dir.glob("*.sql"))
    }
    try:
        with database.connect() as connection:
            rows = connection.execute("SELECT version, sha256 FROM schema_migrations").fetchall()
        applied = {str(row["version"]): str(row["sha256"]) for row in rows}
    except Exception:
        applied = {}
    return MigrationStatus(
        provider=str(database.provider),
        applied_versions=tuple(sorted(version for version in expected if version in applied)),
        pending_versions=tuple(sorted(version for version in expected if version not in applied)),
        changed_versions=tuple(sorted(version for version, digest in expected.items() if applied.get(version) not in {None, digest})),
        latest_version=max(expected) if expected else None,
    )


class MigrationManager:
    """Explicit migration entrypoint used by production management commands only."""

    def __init__(self, database: Database) -> None:
        self.database = database

    def status(self) -> MigrationStatus:
        return migration_status(self.database)

    def apply(self) -> MigrationStatus:
        migrate = getattr(self.database, "migrate", None)
        if not callable(migrate):
            raise DatabaseError("当前数据库 Provider 不支持显式迁移")
        migrate()
        return self.status()


def _backfill_continuous_learning_datasets(connection: Any) -> None:
    """Finish the provider-neutral 0018 Dataset metadata backfill.

    SQLite and PostgreSQL do not share a built-in SHA256 JSON function.  The
    migration owns the columns and lifecycle defaults; this hook uses the
    same canonical JSON encoding as the continuous-learning repository while
    the migration transaction is still open.
    """

    rows = connection.execute("SELECT dataset_id, dataset_json, content_sha256, record_count FROM drain_datasets").fetchall()
    for row in rows:
        if row["content_sha256"] is not None and row["record_count"] is not None:
            continue
        payload = row["dataset_json"]
        if not isinstance(payload, (dict, list)):
            try:
                payload = json.loads(str(payload))
            except (TypeError, json.JSONDecodeError):
                payload = {}
        records = payload.get("records") if isinstance(payload, dict) else []
        if not isinstance(records, list):
            records = []
        digest = hashlib.sha256(
            json.dumps(records, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
        ).hexdigest()
        connection.execute(
            "UPDATE drain_datasets SET content_sha256=COALESCE(content_sha256, ?), record_count=COALESCE(record_count, ?) WHERE dataset_id=?",
            (digest, len(records), row["dataset_id"]),
        )


def qmark_to_pyformat(sql: str) -> str:
    """Convert SQLite qmark placeholders without touching SQL literals/comments."""

    output: list[str] = []
    index = 0
    quote: str | None = None
    block_comment = False
    line_comment = False
    dollar_quote: str | None = None
    while index < len(sql):
        current = sql[index]
        following = sql[index + 1] if index + 1 < len(sql) else ""
        if dollar_quote:
            if sql.startswith(dollar_quote, index):
                output.append(dollar_quote)
                index += len(dollar_quote)
                dollar_quote = None
                continue
            output.append(current)
            index += 1
            continue
        if line_comment:
            output.append(current)
            line_comment = current != "\n"
            index += 1
            continue
        if block_comment:
            output.append(current)
            if current == "*" and following == "/":
                output.append(following)
                index += 2
                block_comment = False
            else:
                index += 1
            continue
        if quote:
            output.append(current)
            if current == quote:
                if following == quote:
                    output.append(following)
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if current == "-" and following == "-":
            output.extend((current, following))
            index += 2
            line_comment = True
            continue
        if current == "/" and following == "*":
            output.extend((current, following))
            index += 2
            block_comment = True
            continue
        if current in {"'", '"'}:
            output.append(current)
            quote = current
            index += 1
            continue
        if current == "$":
            match = re.match(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$", sql[index:])
            if match:
                dollar_quote = match.group(0)
                output.append(dollar_quote)
                index += len(dollar_quote)
                continue
        output.append("%s" if current == "?" else current)
        index += 1
    return "".join(output)


def split_sql_statements(sql: str) -> list[str]:
    """Split simple migration scripts while preserving quoted text and comments."""

    statements: list[str] = []
    buffer: list[str] = []
    index = 0
    quote: str | None = None
    block_comment = False
    line_comment = False
    dollar_quote: str | None = None
    while index < len(sql):
        current = sql[index]
        following = sql[index + 1] if index + 1 < len(sql) else ""
        if dollar_quote:
            if sql.startswith(dollar_quote, index):
                buffer.append(dollar_quote)
                index += len(dollar_quote)
                dollar_quote = None
                continue
            buffer.append(current)
            index += 1
            continue
        if line_comment:
            buffer.append(current)
            line_comment = current != "\n"
            index += 1
            continue
        if block_comment:
            buffer.append(current)
            if current == "*" and following == "/":
                buffer.append(following)
                index += 2
                block_comment = False
            else:
                index += 1
            continue
        if quote:
            buffer.append(current)
            if current == quote:
                if following == quote:
                    buffer.append(following)
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if current == "-" and following == "-":
            buffer.extend((current, following))
            index += 2
            line_comment = True
            continue
        if current == "/" and following == "*":
            buffer.extend((current, following))
            index += 2
            block_comment = True
            continue
        if current in {"'", '"'}:
            buffer.append(current)
            quote = current
            index += 1
            continue
        if current == "$":
            match = re.match(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$", sql[index:])
            if match:
                dollar_quote = match.group(0)
                buffer.append(dollar_quote)
                index += len(dollar_quote)
                continue
        if current == ";":
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
            index += 1
            continue
        buffer.append(current)
        index += 1
    statement = "".join(buffer).strip()
    if statement:
        statements.append(statement)
    return statements


class SQLiteDatabase:
    provider = "sqlite"

    def __init__(self, path: str | Path, migrations_dir: str | Path | None = None, *, migrate: bool = True) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.state_root = self.path.parent
        self.migrations_dir = Path(migrations_dir) if migrations_dir else Path(__file__).parents[2] / "database" / "migrations"
        if migrate:
            self.migrate()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = NORMAL")
        return connection

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def migrate(self) -> None:
        with self.connect() as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                "version TEXT PRIMARY KEY, name TEXT NOT NULL, sha256 TEXT NOT NULL, applied_at TEXT NOT NULL)"
            )
            applied = {row[0]: row[1] for row in connection.execute("SELECT version, sha256 FROM schema_migrations")}
            for path in sorted(self.migrations_dir.glob("*.sql")):
                version = path.name.split("_", 1)[0]
                sql = path.read_text(encoding="utf-8")
                digest = hashlib.sha256(sql.encode("utf-8")).hexdigest()
                if version in applied:
                    if applied[version] != digest:
                        raise RuntimeError(f"数据库迁移文件已被修改: {path.name}")
                    continue
                connection.executescript(sql)
                if version == "0018":
                    _backfill_continuous_learning_datasets(connection)
                connection.execute(
                    "INSERT INTO schema_migrations(version, name, sha256, applied_at) VALUES (?, ?, ?, ?)",
                    (version, path.name, digest, utc_now()),
                )
            connection.commit()


class RowRecord(dict[str, Any]):
    """Mapping rows that also retain the numeric SQLite row access used by stores."""

    def __getitem__(self, key: str | int) -> Any:
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


def _normalise_postgres_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat() if value.tzinfo else value.replace(tzinfo=timezone.utc).isoformat()
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return value


class PostgresCursor:
    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor

    def __iter__(self) -> Iterator[RowRecord]:
        """Match sqlite3 cursors used by the existing stores."""
        return iter(self.fetchall())

    @property
    def rowcount(self) -> int:
        return int(self._cursor.rowcount or 0)

    def fetchone(self) -> RowRecord | None:
        row = self._cursor.fetchone()
        return RowRecord({key: _normalise_postgres_value(value) for key, value in row.items()}) if row is not None else None

    def fetchall(self) -> list[RowRecord]:
        return [RowRecord({key: _normalise_postgres_value(value) for key, value in row.items()}) for row in self._cursor.fetchall()]


class PostgresConnection:
    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def __enter__(self) -> "PostgresConnection":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        try:
            if exc_type is None:
                self._connection.commit()
            else:
                self._connection.rollback()
        finally:
            self._connection.close()

    def execute(self, sql: str, parameters: Sequence[Any] | None = None) -> PostgresCursor:
        cursor = self._connection.cursor()
        cursor.execute(qmark_to_pyformat(sql), tuple(parameters or ()))
        return PostgresCursor(cursor)

    def executemany(self, sql: str, parameters: Sequence[Sequence[Any]]) -> None:
        cursor = self._connection.cursor()
        cursor.executemany(qmark_to_pyformat(sql), parameters)

    def executescript(self, sql: str) -> None:
        for statement in split_sql_statements(sql):
            self.execute(statement)

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()


class PostgresDatabase:
    provider = "postgres"

    def __init__(
        self,
        database_url: str,
        *,
        state_root: str | Path,
        migrations_dir: str | Path | None = None,
        migrate: bool = True,
    ) -> None:
        if not database_url.strip():
            raise ValueError("PostgreSQL 连接地址不能为空")
        self.database_url = database_url
        self.state_root = Path(state_root)
        self.state_root.mkdir(parents=True, exist_ok=True)
        self.migrations_dir = Path(migrations_dir) if migrations_dir else Path(__file__).parents[2] / "database" / "postgres" / "migrations"
        self._psycopg, self._dict_row = self._load_driver()
        if migrate:
            self.migrate()

    @staticmethod
    def _load_driver() -> tuple[Any, Any]:
        try:
            psycopg = importlib.import_module("psycopg")
            rows = importlib.import_module("psycopg.rows")
        except ImportError as exc:
            raise DatabaseError("PostgreSQL 模式需要可选依赖 psycopg[binary]；请安装 requirements-postgres.txt") from exc
        return psycopg, rows.dict_row

    def connect(self) -> PostgresConnection:
        try:
            connection = self._psycopg.connect(self.database_url, row_factory=self._dict_row)
        except Exception as exc:
            raise DatabaseError("无法连接 PostgreSQL；请检查地址、网络、SSL 和密码环境变量") from exc
        return PostgresConnection(connection)

    @contextmanager
    def transaction(self) -> Iterator[PostgresConnection]:
        connection = self.connect()
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def migrate(self) -> None:
        with self.transaction() as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                "version TEXT PRIMARY KEY, name TEXT NOT NULL, sha256 TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL)"
            )
            applied = {row["version"]: row["sha256"] for row in connection.execute("SELECT version, sha256 FROM schema_migrations").fetchall()}
            for path in sorted(self.migrations_dir.glob("*.sql")):
                version = path.name.split("_", 1)[0]
                sql = path.read_text(encoding="utf-8")
                digest = hashlib.sha256(sql.encode("utf-8")).hexdigest()
                if version in applied:
                    if applied[version] != digest:
                        raise RuntimeError(f"数据库迁移文件已被修改: {path.name}")
                    continue
                connection.executescript(sql)
                if version == "0018":
                    _backfill_continuous_learning_datasets(connection)
                connection.execute(
                    "INSERT INTO schema_migrations(version, name, sha256, applied_at) VALUES (?, ?, ?, ?)",
                    (version, path.name, digest, utc_now()),
                )

    def test_connection(self) -> None:
        with self.connect() as connection:
            connection.execute("SELECT 1").fetchone()


def create_database(
    *,
    provider: str,
    sqlite_path: str | Path,
    state_root: str | Path,
    database_url: str | None = None,
    migrate: bool = True,
) -> SQLiteDatabase | PostgresDatabase:
    """Build the explicitly selected runtime storage provider without fallback."""

    if provider == "sqlite":
        return SQLiteDatabase(sqlite_path, migrate=migrate)
    if provider == "postgres":
        if not database_url:
            raise ValueError("PostgreSQL 模式需要数据库连接地址")
        return PostgresDatabase(database_url, state_root=state_root, migrate=migrate)
    raise ValueError("数据库 Provider 必须是 sqlite 或 postgres")
