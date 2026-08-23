from __future__ import annotations

import re
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from logrisk.artifact_storage import SharedArtifactStore, StagedArtifact
from logrisk.database import Database

from .archive import ArchiveInspection, validate_archive
from .asset_adapters import KnowledgeAssetAdapterRegistry
from .errors import KnowledgePackageError
from .repository import KnowledgePackageRepository


PACKAGE_FILENAME_RE = re.compile(r"^[^/\\]+\.logrisk-package\.zip$", re.IGNORECASE)
MAX_PACKAGE_BYTES = 100 * 1024 * 1024


class KnowledgePackageService:
    def __init__(self, database: Database, artifact_store: SharedArtifactStore, *, app_version: str = "1.35.1", adapters: KnowledgeAssetAdapterRegistry | None = None) -> None:
        self.database = database
        self.artifact_store = artifact_store
        self.repository = KnowledgePackageRepository(database)
        self.app_version = app_version
        self.adapters = adapters or KnowledgeAssetAdapterRegistry()

    def list_packages(self) -> list[dict[str, Any]]:
        return self.repository.list_packages()

    def get_package(self, package_id: str) -> dict[str, Any]:
        package = self.repository.get_package(package_id)
        if not package:
            raise KeyError("知识包不存在")
        versions = []
        with self.database.connect() as connection:
            rows = connection.execute("SELECT version FROM knowledge_package_versions WHERE package_id=? ORDER BY version DESC", (package_id,)).fetchall()
        for row in rows:
            version = self.repository.get_version(package_id, str(row["version"]))
            if version:
                versions.append(version)
        return dict(package, versions=versions)

    def get_version(self, package_id: str, version: str) -> dict[str, Any]:
        result = self.repository.get_version(package_id, version)
        if not result:
            raise KeyError("知识包版本不存在")
        return result

    def upload(self, filename: str, content: bytes) -> dict[str, Any]:
        safe_filename = Path(str(filename or "")).name
        if not PACKAGE_FILENAME_RE.fullmatch(safe_filename):
            raise KnowledgePackageError("上传文件必须使用 .logrisk-package.zip 后缀", code="package_filename_invalid")
        if not isinstance(content, (bytes, bytearray)):
            raise KnowledgePackageError("知识包内容必须是二进制数据", code="package_content_invalid")
        if len(content) > MAX_PACKAGE_BYTES:
            raise KnowledgePackageError("知识包压缩大小超过限制", code="package_too_large")
        staged = self.artifact_store.stage_bytes("knowledge-packages/uploads", content)
        upload_id = uuid.uuid4().hex
        staging_path = self.artifact_store.relative_path(staged.path)
        try:
            return self.repository.create_import(
                upload_id=upload_id,
                filename=safe_filename,
                staging_path=staging_path,
                compressed_bytes=len(content),
            )
        except Exception:
            self.artifact_store.delete_candidate(staged)
            raise

    def inspect_upload(self, upload_id: str) -> dict[str, Any]:
        record = self.repository.get_import(upload_id)
        if not record:
            raise KeyError("知识包上传记录不存在")
        if record.get("status") == "installed" and record.get("report"):
            return dict(record, inspection=record["report"])
        staging_path = record.get("staging_path")
        if not staging_path:
            raise KnowledgePackageError("上传记录缺少暂存 Artifact", code="package_stage_missing")
        try:
            inspection = validate_archive(self.artifact_store.resolve(str(staging_path)))
            compatibility = self._compatibility(inspection)
            report = dict(inspection.public_dict(), compatibility=compatibility, conflicts=self._conflicts(inspection))
            status = "validated" if compatibility["compatible"] and not report["conflicts"] else "rejected"
            record = self.repository.update_import(upload_id, status=status, report=report, package_sha256=inspection.package_sha256, expanded_bytes=inspection.expanded_bytes)
            return dict(record, inspection=report)
        except KnowledgePackageError as exc:
            report = {"valid": False, "code": exc.code, "error": str(exc)}
            record = self.repository.update_import(upload_id, status="rejected", report=report)
            return dict(record, inspection=report)

    def install(self, upload_id: str, *, preview_sha256: str, confirmed: bool, actor: str, request_id: str) -> dict[str, Any]:
        if not confirmed:
            raise KnowledgePackageError("安装前必须确认预览摘要", code="package_confirmation_required")
        inspected = self.inspect_upload(upload_id)
        report = inspected.get("inspection") or {}
        if report.get("package_sha256") != str(preview_sha256):
            raise KnowledgePackageError("安装摘要与预览摘要不一致，请重新预览", code="package_preview_conflict")
        if inspected.get("status") != "validated":
            if inspected.get("status") == "installed" and report.get("manifest"):
                manifest = report["manifest"]
                existing = self.repository.get_version(str(manifest["package_id"]), str(manifest["version"]))
                if existing:
                    return dict(existing, package_sha256=str(report["package_sha256"]))
            raise KnowledgePackageError("知识包未通过安装前校验", code="package_not_validated")
        inspection = self._inspection_from_report(inspected)
        manifest = inspection.manifest
        target_relative = f"knowledge-packages/packages/{manifest.package_id}-{manifest.version}-{inspection.package_sha256[:12]}.logrisk-package.zip"
        target = self.artifact_store.resolve(target_relative)
        staged_relative = str(inspected["staging_path"])
        staged_path = self.artifact_store.resolve(staged_relative)
        try:
            if not target.exists():
                promoted = self.artifact_store.promote(
                    StagedArtifact(path=staged_path, namespace="knowledge-packages/uploads"),
                    target_relative,
                    expected_sha256=inspection.package_sha256,
                )
                target_relative = promoted.relative_path
            result = self.repository.create_installation(
                inspection=inspection,
                import_record=inspected,
                actor=actor or "unknown",
                request_id=request_id,
                artifact_path=target_relative,
            )
            return dict(result, package_sha256=inspection.package_sha256)
        except Exception:
            if target.exists() and not self.repository.get_version(manifest.package_id, manifest.version):
                target.unlink(missing_ok=True)
            if staged_path.exists():
                staged_path.unlink(missing_ok=True)
            raise

    def materialize_asset(self, package_id: str, version: str, asset_id: str, *, actor: str, request_id: str) -> dict[str, Any]:
        asset = self.repository.get_asset(package_id, version, asset_id)
        if not asset:
            raise KeyError("知识包资产不存在")
        if asset["status"] == "materialized":
            return asset
        package_version = self.repository.get_version(package_id, version)
        if not package_version:
            raise KeyError("知识包版本不存在")
        artifact_path = str(package_version["artifact_path"])
        try:
            with self.artifact_store.open_read(artifact_path) as handle:
                with zipfile.ZipFile(handle) as archive:
                    content = archive.read(str(asset["asset_path"]))
            materialized = self.adapters.materialize(dict(asset, package_id=package_id, version=version), content)
            return self.repository.materialize_asset(
                package_id,
                version,
                asset_id,
                target_domain=materialized["target_domain"],
                target_resource_id=materialized["resource_id"],
                target_version=materialized["version"],
                actor=actor or "unknown",
                request_id=request_id,
            )
        except Exception as exc:
            self._mark_asset_failed(package_id, version, asset_id, exc)
            raise

    def retire_version(self, package_id: str, version: str, *, actor: str, request_id: str) -> dict[str, Any]:
        return self.repository.retire_version(package_id, version, actor=actor or "unknown", request_id=request_id)

    def audit(self, *, package_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_audit(package_id=package_id, limit=limit)

    def _compatibility(self, inspection: ArchiveInspection) -> dict[str, Any]:
        current = _version_tuple(self.app_version)
        minimum = _version_tuple(inspection.manifest.platform["min_version"])
        maximum = _version_tuple(inspection.manifest.platform["max_version_exclusive"])
        return {"compatible": minimum <= current < maximum, "current_version": self.app_version, "min_version": inspection.manifest.platform["min_version"], "max_version_exclusive": inspection.manifest.platform["max_version_exclusive"]}

    def _conflicts(self, inspection: ArchiveInspection) -> list[dict[str, Any]]:
        conflicts: list[dict[str, Any]] = []
        existing = self.repository.get_version(inspection.manifest.package_id, inspection.manifest.version)
        if existing and existing.get("package_sha256") != inspection.package_sha256:
            conflicts.append({"code": "package_version_checksum_conflict", "message": "同一包版本已安装但 SHA256 不同"})
        if inspection.manifest.dependencies:
            with self.database.connect() as connection:
                for dependency in inspection.manifest.dependencies:
                    row = connection.execute(
                        "SELECT status FROM knowledge_package_versions WHERE package_id=? AND version=?",
                        (dependency["package_id"], dependency["version"]),
                    ).fetchone()
                    if not row or str(row["status"]) != "installed":
                        conflicts.append({
                            "code": "package_dependency_missing",
                            "message": f"缺少精确依赖 {dependency['package_id']}@{dependency['version']}",
                            "package_id": dependency["package_id"],
                            "version": dependency["version"],
                        })
        return conflicts

    @staticmethod
    def _inspection_from_report(record: dict[str, Any]) -> ArchiveInspection:
        from .manifest import PackageManifest
        report = record.get("inspection") or record.get("report") or {}
        manifest = PackageManifest.from_dict(report.get("manifest"))
        return ArchiveInspection(
            path=Path(str(record.get("staging_path") or "")),
            manifest=manifest,
            package_sha256=str(report["package_sha256"]),
            compressed_bytes=int(report["compressed_bytes"]),
            expanded_bytes=int(report["expanded_bytes"]),
            files=tuple(report.get("files") or ()),
            asset_sha256={asset.asset_id: asset.sha256 for asset in manifest.assets},
        )

    def _mark_asset_failed(self, package_id: str, version: str, asset_id: str, error: Exception) -> None:
        safe_message = str(error).splitlines()[0][:240]
        with self.database.transaction() as connection:
            connection.execute(
                "UPDATE knowledge_package_assets SET status='failed', error_code=?, error_message=?, state_version=state_version+1, updated_at=? WHERE package_id=? AND version=? AND asset_id=?",
                (getattr(error, "code", "asset_materialize_failed"), safe_message, datetime.now(timezone.utc).isoformat(), package_id, version, asset_id),
            )


def _version_tuple(value: str) -> tuple[int, int, int]:
    parts = str(value).split(".")
    if len(parts) != 3 or not all(part.isdigit() for part in parts):
        raise KnowledgePackageError("版本格式无效")
    return tuple(int(part) for part in parts)  # type: ignore[return-value]
