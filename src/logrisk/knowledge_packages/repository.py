from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from logrisk.database import Database
from .manifest import AssetManifest, PackageManifest


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _db_json(database: Database, value: Any) -> Any:
    """Use native JSONB parameters on PostgreSQL and TEXT on SQLite."""
    return value if getattr(database, "provider", "sqlite") == "postgres" else _json(value)


def _decode(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return default
    return parsed


class KnowledgePackageRepository:
    """Small SQL boundary shared by SQLite and PostgreSQL providers."""

    def __init__(self, database: Database) -> None:
        self.database = database

    def list_packages(self) -> list[dict[str, Any]]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT p.*, "
                "(SELECT COUNT(*) FROM knowledge_package_versions v WHERE v.package_id=p.package_id) AS version_count, "
                "(SELECT MAX(v.updated_at) FROM knowledge_package_versions v WHERE v.package_id=p.package_id) AS latest_updated_at "
                "FROM knowledge_packages p ORDER BY p.updated_at DESC, p.package_id"
            ).fetchall()
        return [dict(row) for row in rows]

    def get_package(self, package_id: str) -> dict[str, Any] | None:
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM knowledge_packages WHERE package_id=?", (package_id,)).fetchone()
        return dict(row) if row else None

    def get_version(self, package_id: str, version: str) -> dict[str, Any] | None:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT * FROM knowledge_package_versions WHERE package_id=? AND version=?",
                (package_id, version),
            ).fetchone()
            if not row:
                return None
            result = dict(row)
            assets = connection.execute(
                "SELECT * FROM knowledge_package_assets WHERE package_id=? AND version=? ORDER BY asset_id",
                (package_id, version),
            ).fetchall()
            dependencies = connection.execute(
                "SELECT dependency_package_id, dependency_version FROM knowledge_package_dependencies "
                "WHERE package_id=? AND version=? ORDER BY dependency_package_id, dependency_version",
                (package_id, version),
            ).fetchall()
        result["manifest"] = _decode(result.pop("manifest_json", "{}"), {})
        result["assets"] = [dict(asset) for asset in assets]
        result["dependencies"] = [dict(item) for item in dependencies]
        return result

    def get_import(self, upload_id: str) -> dict[str, Any] | None:
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM knowledge_package_imports WHERE upload_id=?", (upload_id,)).fetchone()
        if not row:
            return None
        result = dict(row)
        result["report"] = _decode(result.pop("report_json", "{}"), {})
        return result

    def create_import(self, *, upload_id: str, filename: str, staging_path: str, compressed_bytes: int) -> dict[str, Any]:
        now = _now()
        with self.database.transaction() as connection:
            connection.execute(
                "INSERT INTO knowledge_package_imports(upload_id, filename, staging_path, artifact_path, package_sha256, "
                "compressed_bytes, expanded_bytes, status, report_json, confirmed_by, created_at, updated_at, expires_at) "
                "VALUES (?, ?, ?, NULL, NULL, ?, 0, 'uploaded', ?, NULL, ?, ?, NULL)",
                (upload_id, filename, staging_path, int(compressed_bytes), _db_json(self.database, {}), now, now),
            )
        return self.get_import(upload_id) or {}

    def update_import(self, upload_id: str, *, status: str, report: dict[str, Any] | None = None, package_sha256: str | None = None, expanded_bytes: int | None = None, artifact_path: str | None = None, confirmed_by: str | None = None) -> dict[str, Any]:
        changes = ["status=?", "updated_at=?"]
        values: list[Any] = [status, _now()]
        if report is not None:
            changes.append("report_json=?")
            values.append(_db_json(self.database, report))
        if package_sha256 is not None:
            changes.append("package_sha256=?")
            values.append(package_sha256)
        if expanded_bytes is not None:
            changes.append("expanded_bytes=?")
            values.append(int(expanded_bytes))
        if artifact_path is not None:
            changes.append("artifact_path=?")
            values.append(artifact_path)
        if confirmed_by is not None:
            changes.append("confirmed_by=?")
            values.append(confirmed_by)
        values.append(upload_id)
        with self.database.transaction() as connection:
            connection.execute(f"UPDATE knowledge_package_imports SET {', '.join(changes)} WHERE upload_id=?", values)
        return self.get_import(upload_id) or {}

    def create_installation(self, *, inspection: Any, import_record: dict[str, Any], actor: str, request_id: str, artifact_path: str) -> dict[str, Any]:
        manifest: PackageManifest = inspection.manifest
        now = _now()
        with self.database.transaction() as connection:
            existing = connection.execute(
                "SELECT package_sha256, artifact_path FROM knowledge_package_versions WHERE package_id=? AND version=?",
                (manifest.package_id, manifest.version),
            ).fetchone()
            if existing:
                if str(existing["package_sha256"]) != inspection.package_sha256:
                    raise ValueError("同一 package_id/version 已存在不同 SHA256 的知识包")
                connection.execute(
                    "UPDATE knowledge_package_imports SET status='installed', artifact_path=?, package_sha256=?, expanded_bytes=?, updated_at=? WHERE upload_id=?",
                    (str(existing["artifact_path"]), inspection.package_sha256, inspection.expanded_bytes, now, import_record["upload_id"]),
                )
                self._audit(connection, manifest.package_id, manifest.version, None, "install", "success", actor, request_id, {"idempotent": True})
            else:
                connection.execute(
                    "INSERT INTO knowledge_packages(package_id, name, description, created_at, updated_at) VALUES (?, ?, ?, ?, ?) "
                    "ON CONFLICT(package_id) DO UPDATE SET name=excluded.name, description=excluded.description, updated_at=excluded.updated_at",
                    (manifest.package_id, manifest.name, manifest.description, now, now),
                )
                connection.execute(
                    "INSERT INTO knowledge_package_versions(package_id, version, manifest_json, package_sha256, artifact_path, compressed_bytes, expanded_bytes, "
                    "platform_min_version, platform_max_version_exclusive, status, installed_by, installed_at, state_version, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'installed', ?, ?, 1, ?, ?)",
                    (manifest.package_id, manifest.version, _db_json(self.database, manifest.to_dict()), inspection.package_sha256, artifact_path,
                     inspection.compressed_bytes, inspection.expanded_bytes, manifest.platform["min_version"],
                     manifest.platform["max_version_exclusive"], actor, now, now, now),
                )
                for dependency in manifest.dependencies:
                    connection.execute(
                        "INSERT INTO knowledge_package_dependencies(package_id, version, dependency_package_id, dependency_version, created_at) VALUES (?, ?, ?, ?, ?)",
                        (manifest.package_id, manifest.version, dependency["package_id"], dependency["version"], now),
                    )
                for asset in manifest.assets:
                    connection.execute(
                        "INSERT INTO knowledge_package_assets(package_id, version, asset_id, asset_type, asset_path, asset_sha256, media_type, "
                        "target_domain, target_resource_id, target_version, status, error_code, error_message, state_version, created_at, updated_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, 'disabled', NULL, NULL, 1, ?, ?)",
                        (manifest.package_id, manifest.version, asset.asset_id, asset.type, asset.path, asset.sha256, asset.media_type, now, now),
                    )
                connection.execute(
                    "UPDATE knowledge_package_imports SET status='installed', artifact_path=?, package_sha256=?, expanded_bytes=?, updated_at=? WHERE upload_id=?",
                    (artifact_path, inspection.package_sha256, inspection.expanded_bytes, now, import_record["upload_id"]),
                )
                self._audit(connection, manifest.package_id, manifest.version, None, "install", "success", actor, request_id, {"asset_count": len(manifest.assets)})
        return self.get_version(manifest.package_id, manifest.version) or {}

    def get_asset(self, package_id: str, version: str, asset_id: str) -> dict[str, Any] | None:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT * FROM knowledge_package_assets WHERE package_id=? AND version=? AND asset_id=?",
                (package_id, version, asset_id),
            ).fetchone()
        return dict(row) if row else None

    def materialize_asset(self, package_id: str, version: str, asset_id: str, *, target_domain: str, target_resource_id: str, target_version: str, actor: str, request_id: str) -> dict[str, Any]:
        now = _now()
        with self.database.transaction() as connection:
            row = connection.execute(
                "SELECT status, state_version FROM knowledge_package_assets WHERE package_id=? AND version=? AND asset_id=?",
                (package_id, version, asset_id),
            ).fetchone()
            if not row:
                raise KeyError("知识包资产不存在")
            if row["status"] == "materialized":
                result = connection.execute(
                    "SELECT * FROM knowledge_package_assets WHERE package_id=? AND version=? AND asset_id=?",
                    (package_id, version, asset_id),
                ).fetchone()
                return dict(result)
            updated = connection.execute(
                "UPDATE knowledge_package_assets SET target_domain=?, target_resource_id=?, target_version=?, status='materialized', "
                "error_code=NULL, error_message=NULL, state_version=state_version+1, updated_at=? "
                "WHERE package_id=? AND version=? AND asset_id=? AND state_version=? AND status='disabled'",
                (target_domain, target_resource_id, target_version, now, package_id, version, asset_id, row["state_version"]),
            )
            if updated.rowcount != 1:
                raise ValueError("知识包资产状态已被其他操作更新")
            self._audit(connection, package_id, version, asset_id, "materialize", "success", actor, request_id, {"target_domain": target_domain, "target_resource_id": target_resource_id})
            result = connection.execute(
                "SELECT * FROM knowledge_package_assets WHERE package_id=? AND version=? AND asset_id=?",
                (package_id, version, asset_id),
            ).fetchone()
        return dict(result) if result else {}

    def retire_version(self, package_id: str, version: str, *, actor: str, request_id: str) -> dict[str, Any]:
        now = _now()
        with self.database.transaction() as connection:
            changed = connection.execute(
                "UPDATE knowledge_package_versions SET status='retired', state_version=state_version+1, updated_at=? WHERE package_id=? AND version=? AND status='installed'",
                (now, package_id, version),
            )
            if changed.rowcount != 1:
                raise KeyError("知识包版本不存在或已退休")
            connection.execute(
                "UPDATE knowledge_package_assets SET status='retired', updated_at=? WHERE package_id=? AND version=? AND status='disabled'",
                (now, package_id, version),
            )
            self._audit(connection, package_id, version, None, "retire", "success", actor, request_id, {})
        return self.get_version(package_id, version) or {}

    def list_audit(self, *, package_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        if package_id:
            clauses.append("package_id=?")
            values.append(package_id)
        values.append(max(1, min(int(limit), 500)))
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self.database.connect() as connection:
            rows = connection.execute(
                f"SELECT * FROM knowledge_package_audit_events{where} ORDER BY created_at DESC, audit_id DESC LIMIT ?",
                values,
            ).fetchall()
        return [dict(row, roles=_decode(row["roles_json"], []), attributes=_decode(row["attributes_json"], {})) for row in rows]

    def _audit(self, connection: Any, package_id: str, version: str, asset_id: str | None, action: str, outcome: str, actor: str, request_id: str, attributes: dict[str, Any]) -> None:
        connection.execute(
            "INSERT INTO knowledge_package_audit_events(audit_id, package_id, version, asset_id, action, outcome, actor, roles_json, request_id, attributes_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (uuid.uuid4().hex, package_id, version, asset_id, action, outcome, actor, _db_json(self.database, []), request_id, _db_json(self.database, attributes), _now()),
        )
