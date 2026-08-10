from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest


def _source(
    tmp_path: Path,
    *,
    prompt: bytes = b"prompt",
    package_id: str = "demo-package",
    dependencies: list[dict[str, str]] | None = None,
) -> Path:
    from logrisk.knowledge_packages.archive import build_archive

    source = tmp_path / "source"
    (source / "assets").mkdir(parents=True)
    (source / "assets" / "feature_prompt.md").write_bytes(prompt)
    manifest = {
        "schema_version": 1,
        "package_id": package_id,
        "name": "演示知识包",
        "version": "1.0.0",
        "description": "测试包",
        "platform": {"min_version": "1.32.0", "max_version_exclusive": "2.0.0"},
        "dependencies": dependencies or [],
        "assets": [{
            "asset_id": "demo-prompt",
            "type": "feature_prompt",
            "path": "assets/feature_prompt.md",
            "sha256": hashlib.sha256(prompt).hexdigest(),
            "media_type": "text/markdown",
        }],
    }
    (source / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    archive = tmp_path / "demo.logrisk-package.zip"
    build_archive(source, archive)
    return archive


def test_upload_preview_install_and_materialize_are_safe_and_idempotent(tmp_path: Path) -> None:
    from logrisk.artifact_storage import SharedArtifactStore
    from logrisk.database import SQLiteDatabase
    from logrisk.knowledge_packages.service import KnowledgePackageService

    archive = _source(tmp_path)
    service = KnowledgePackageService(
        SQLiteDatabase(tmp_path / "state" / "logrisk.sqlite3"),
        SharedArtifactStore(tmp_path / "shared"),
    )
    upload = service.upload(archive.name, archive.read_bytes())
    preview = service.inspect_upload(upload["upload_id"])
    assert preview["status"] == "validated"
    installed = service.install(
        upload["upload_id"],
        preview_sha256=preview["inspection"]["package_sha256"],
        confirmed=True,
        actor="admin",
        request_id="request-1",
    )
    assert installed["status"] == "installed"
    assert installed["assets"][0]["status"] == "disabled"
    materialized = service.materialize_asset("demo-package", "1.0.0", "demo-prompt", actor="admin", request_id="request-2")
    assert materialized["status"] == "materialized"
    assert materialized["target_domain"] == "feature_prompt"
    repeated = service.install(
        upload["upload_id"],
        preview_sha256=preview["inspection"]["package_sha256"],
        confirmed=True,
        actor="admin",
        request_id="request-3",
    )
    assert repeated["package_sha256"] == preview["inspection"]["package_sha256"]
    assert len(service.audit()) >= 2


def test_install_requires_confirmation_and_preview_digest(tmp_path: Path) -> None:
    from logrisk.artifact_storage import SharedArtifactStore
    from logrisk.database import SQLiteDatabase
    from logrisk.knowledge_packages.errors import KnowledgePackageError
    from logrisk.knowledge_packages.service import KnowledgePackageService

    archive = _source(tmp_path)
    service = KnowledgePackageService(SQLiteDatabase(tmp_path / "state" / "db.sqlite3"), SharedArtifactStore(tmp_path / "shared"))
    upload = service.upload(archive.name, archive.read_bytes())
    with pytest.raises(KnowledgePackageError, match="确认"):
        service.install(upload["upload_id"], preview_sha256="0" * 64, confirmed=False, actor="admin", request_id="request")
    preview = service.inspect_upload(upload["upload_id"])
    with pytest.raises(KnowledgePackageError, match="摘要"):
        service.install(upload["upload_id"], preview_sha256="0" * 64, confirmed=True, actor="admin", request_id="request")
    assert preview["status"] == "validated"


def test_install_requires_each_exact_dependency_to_be_installed(tmp_path: Path) -> None:
    from logrisk.artifact_storage import SharedArtifactStore
    from logrisk.database import SQLiteDatabase
    from logrisk.knowledge_packages.service import KnowledgePackageService

    archive = _source(
        tmp_path,
        package_id="dependent-package",
        dependencies=[{"package_id": "base-package", "version": "1.0.0"}],
    )
    service = KnowledgePackageService(SQLiteDatabase(tmp_path / "state" / "db.sqlite3"), SharedArtifactStore(tmp_path / "shared"))
    upload = service.upload(archive.name, archive.read_bytes())
    preview = service.inspect_upload(upload["upload_id"])
    assert preview["status"] == "rejected"
    assert preview["inspection"]["conflicts"][0]["code"] == "package_dependency_missing"
