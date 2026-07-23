from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from logrisk.database import PostgresDatabase


class MigrationError(RuntimeError):
    pass


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _source_connection(path: str | Path) -> sqlite3.Connection:
    source = Path(path)
    if not source.is_file():
        raise MigrationError(f"SQLite 源数据库不存在: {source}")
    connection = sqlite3.connect(f"file:{source.resolve()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _source_tables(connection: sqlite3.Connection) -> list[str]:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name != 'schema_migrations'"
    ).fetchall()
    tables = sorted(str(row[0]) for row in rows)
    dependencies: dict[str, set[str]] = {table: set() for table in tables}
    available = set(tables)
    for table in tables:
        for row in connection.execute(f"PRAGMA foreign_key_list({_quote_identifier(table)})").fetchall():
            parent = str(row[2])
            if parent in available and parent != table:
                dependencies[table].add(parent)
    ordered: list[str] = []
    pending = set(tables)
    while pending:
        ready = sorted(table for table in pending if not (dependencies[table] & pending))
        if not ready:
            ordered.extend(sorted(pending))
            break
        ordered.extend(ready)
        pending.difference_update(ready)
    return ordered


def _source_columns(connection: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    return [dict(row) for row in connection.execute(f"PRAGMA table_info({_quote_identifier(table)})").fetchall()]


def _artifact_paths(connection: sqlite3.Connection, tables: Iterable[str]) -> dict[str, Any]:
    checked: list[str] = []
    for table in tables:
        columns = {str(item["name"]) for item in _source_columns(connection, table)}
        for column in sorted(columns & {"path", "source_path"}):
            rows = connection.execute(
                f"SELECT {_quote_identifier(column)} FROM {_quote_identifier(table)} WHERE {_quote_identifier(column)} IS NOT NULL"
            ).fetchall()
            checked.extend(str(row[0]) for row in rows if str(row[0]).strip())
    missing = sorted({path for path in checked if not Path(path).exists()})
    return {"checked": len(checked), "existing": len(checked) - len(missing), "missing": missing}


def build_migration_preview(source_sqlite: str | Path) -> dict[str, Any]:
    with _source_connection(source_sqlite) as connection:
        tables = _source_tables(connection)
        counts = {
            table: int(connection.execute(f"SELECT COUNT(*) FROM {_quote_identifier(table)}").fetchone()[0])
            for table in tables
        }
        artifacts = _artifact_paths(connection, tables)
    return {
        "schema_version": "sqlite_to_postgres_preview_v1",
        "mode": "metadata_only",
        "source_sqlite": str(Path(source_sqlite)),
        "tables": tables,
        "counts": counts,
        "total_rows": sum(counts.values()),
        "artifact_paths": artifacts,
        "excluded": ["schema_migrations", "原始日志文件", "上传分片", "Drain3 .bin", "导出文件"],
    }


def _normalise(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"bytes_sha256": hashlib.sha256(value).hexdigest()}
    if isinstance(value, str):
        stripped = value.strip()
        if stripped[:1] in {"{", "["}:
            try:
                return _normalise(json.loads(stripped))
            except json.JSONDecodeError:
                pass
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value
        if parsed.tzinfo:
            return parsed.astimezone(timezone.utc).isoformat()
        return parsed.replace(tzinfo=timezone.utc).isoformat()
    if isinstance(value, dict):
        return {str(key): _normalise(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalise(item) for item in value]
    return value


def _digest_rows(rows: Iterable[dict[str, Any]]) -> str:
    canonical = sorted(
        json.dumps(_normalise(row), ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
        for row in rows
    )
    return hashlib.sha256("\n".join(canonical).encode("utf-8")).hexdigest()


class SQLiteToPostgresMigration:
    def __init__(self, source_sqlite: str | Path, target_url: str, *, state_root: str | Path = "state") -> None:
        self.source_sqlite = Path(source_sqlite)
        self.target_url = target_url
        self.state_root = Path(state_root)

    def preview(self) -> dict[str, Any]:
        return build_migration_preview(self.source_sqlite)

    def _target_columns(self, connection: Any, table: str) -> list[dict[str, Any]]:
        return [dict(row) for row in connection.execute(
            "SELECT column_name, data_type, udt_name, is_identity FROM information_schema.columns "
            "WHERE table_schema=current_schema() AND table_name=? ORDER BY ordinal_position",
            (table,),
        ).fetchall()]

    @staticmethod
    def _prepare_row(row: sqlite3.Row, boolean_columns: set[str]) -> tuple[Any, ...]:
        values = []
        for key in row.keys():
            value = row[key]
            if key in boolean_columns and value is not None:
                value = bool(value)
            values.append(value)
        return tuple(values)

    def _copy_table(self, source: sqlite3.Connection, target: Any, table: str) -> int:
        columns = _source_columns(source, table)
        names = [str(item["name"]) for item in columns]
        target_columns = self._target_columns(target, table)
        if set(names) != {str(item["column_name"]) for item in target_columns}:
            raise MigrationError(f"目标表结构不一致: {table}")
        boolean_columns = {str(item["column_name"]) for item in target_columns if str(item["data_type"]) == "boolean"}
        rows = source.execute(f"SELECT * FROM {_quote_identifier(table)}").fetchall()
        if not rows:
            return 0
        column_sql = ", ".join(_quote_identifier(name) for name in names)
        placeholders = ", ".join("?" for _ in names)
        target.executemany(
            f"INSERT INTO {_quote_identifier(table)} ({column_sql}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
            [self._prepare_row(row, boolean_columns) for row in rows],
        )
        return len(rows)

    def _reset_identity_sequences(self, target: Any, tables: Iterable[str]) -> None:
        for table in tables:
            for column in self._target_columns(target, table):
                if str(column["is_identity"]) != "YES":
                    continue
                name = str(column["column_name"])
                quoted_table, quoted_column = _quote_identifier(table), _quote_identifier(name)
                target.execute(
                    "SELECT setval(pg_get_serial_sequence(?, ?), "
                    f"COALESCE((SELECT MAX({quoted_column}) FROM {quoted_table}), 1), "
                    f"(SELECT COUNT(*) > 0 FROM {quoted_table}))",
                    (table, name),
                )

    def _verify(self, source: sqlite3.Connection, target: Any, tables: Iterable[str]) -> dict[str, Any]:
        table_reports: dict[str, Any] = {}
        failures: list[str] = []
        for table in tables:
            boolean_columns = {
                str(column["column_name"])
                for column in self._target_columns(target, table)
                if str(column["data_type"]) == "boolean"
            }
            source_rows = []
            for row in source.execute(f"SELECT * FROM {_quote_identifier(table)}").fetchall():
                item = dict(row)
                for column in boolean_columns:
                    if item.get(column) is not None:
                        item[column] = bool(item[column])
                source_rows.append(item)
            target_rows = [dict(row) for row in target.execute(f"SELECT * FROM {_quote_identifier(table)}").fetchall()]
            source_columns = _source_columns(source, table)
            primary_keys = [str(column["name"]) for column in source_columns if int(column["pk"])]
            source_keys = {tuple(row.get(key) for key in primary_keys) for row in source_rows} if primary_keys else set()
            target_keys = {tuple(row.get(key) for key in primary_keys) for row in target_rows} if primary_keys else set()
            source_digest, target_digest = _digest_rows(source_rows), _digest_rows(target_rows)
            valid = len(source_rows) == len(target_rows) and source_keys == target_keys and source_digest == target_digest
            table_reports[table] = {
                "source_count": len(source_rows), "target_count": len(target_rows), "primary_key_match": source_keys == target_keys,
                "source_digest": source_digest, "target_digest": target_digest, "valid": valid,
            }
            if not valid:
                failures.append(table)
        return {"valid": not failures, "tables": table_reports, "failed_tables": failures}

    def execute(self) -> dict[str, Any]:
        preview = self.preview()
        target = PostgresDatabase(self.target_url, state_root=self.state_root)
        with _source_connection(self.source_sqlite) as source, target.transaction() as connection:
            copied = {table: self._copy_table(source, connection, table) for table in preview["tables"]}
            self._reset_identity_sequences(connection, preview["tables"])
            verification = self._verify(source, connection, preview["tables"])
            if not verification["valid"]:
                raise MigrationError("迁移校验失败，PostgreSQL 事务已回滚: " + ", ".join(verification["failed_tables"]))
        return {**preview, "copied": copied, "verification": verification}

    def verify(self) -> dict[str, Any]:
        preview = self.preview()
        target = PostgresDatabase(self.target_url, state_root=self.state_root, migrate=False)
        with _source_connection(self.source_sqlite) as source, target.connect() as connection:
            verification = self._verify(source, connection, preview["tables"])
        return {**preview, "verification": verification}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="停机迁移 LOGRISK SQLite 元数据到外部 PostgreSQL")
    parser.add_argument("--source-sqlite", required=True, help="源 SQLite 数据库路径")
    parser.add_argument("--target-postgres-url", default=os.getenv("LOGRISK_DATABASE_URL"), help="目标 PostgreSQL URL；也可使用 LOGRISK_DATABASE_URL")
    parser.add_argument("--state-root", default=os.getenv("DASHBOARD_STATE_DIR", "state"), help="本机状态与临时文件目录")
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--dry-run", action="store_true", help="仅输出迁移计划和产物路径核验")
    modes.add_argument("--verify", action="store_true", help="只核对已迁移的 PostgreSQL 数据")
    modes.add_argument("--execute", action="store_true", help="执行一次性元数据导入并在提交前核验")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    if args.dry_run:
        result = build_migration_preview(args.source_sqlite)
    else:
        if not args.target_postgres_url:
            raise SystemExit("--target-postgres-url 或 LOGRISK_DATABASE_URL 为必填项")
        migration = SQLiteToPostgresMigration(args.source_sqlite, args.target_postgres_url, state_root=args.state_root)
        result = migration.verify() if args.verify else migration.execute()
        if args.verify and not result["verification"]["valid"]:
            raise SystemExit("PostgreSQL 校验未通过")
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
