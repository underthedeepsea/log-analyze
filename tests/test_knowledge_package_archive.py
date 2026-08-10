from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path

import pytest


def _manifest(*, asset_path: str = "assets/feature_prompt.md", digest: str | None = None) -> bytes:
    digest = digest or hashlib.sha256(b"prompt").hexdigest()
    return json.dumps(
        {
            "schema_version": 1,
            "package_id": "demo-package",
            "name": "演示知识包",
            "version": "1.0.0",
            "description": "测试包",
            "platform": {"min_version": "1.32.0", "max_version_exclusive": "2.0.0"},
            "dependencies": [],
            "assets": [{
                "asset_id": "demo-prompt",
                "type": "feature_prompt",
                "path": asset_path,
                "sha256": digest,
                "media_type": "text/markdown",
            }],
        },
        ensure_ascii=False,
    ).encode("utf-8")


def _write_zip(path: Path, files: dict[str, bytes]) -> Path:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, value in files.items():
            archive.writestr(name, value)
    return path


def test_build_and_validate_archive_is_deterministic(tmp_path: Path) -> None:
    from logrisk.knowledge_packages.archive import build_archive, validate_archive

    source = tmp_path / "source"
    (source / "assets").mkdir(parents=True)
    prompt = b"prompt"
    (source / "assets" / "feature_prompt.md").write_bytes(prompt)
    (source / "manifest.json").write_bytes(_manifest())
    first = build_archive(source, tmp_path / "first.logrisk-package.zip")
    second = build_archive(source, tmp_path / "second.logrisk-package.zip")

    assert first.package_sha256 == second.package_sha256
    assert validate_archive(first.path).package_sha256 == first.package_sha256
    assert first.manifest.package_id == "demo-package"


@pytest.mark.parametrize(
    "filename",
    ["../outside.txt", "/absolute.txt", "assets/unknown.py"],
)
def test_validate_archive_rejects_unsafe_or_unknown_paths(tmp_path: Path, filename: str) -> None:
    from logrisk.knowledge_packages.archive import validate_archive
    from logrisk.knowledge_packages.errors import KnowledgePackageError

    archive = _write_zip(tmp_path / "unsafe.zip", {"manifest.json": _manifest(asset_path=filename), filename: b"prompt"})
    with pytest.raises(KnowledgePackageError):
        validate_archive(archive)


def test_validate_archive_rejects_checksum_mismatch(tmp_path: Path) -> None:
    from logrisk.knowledge_packages.archive import validate_archive
    from logrisk.knowledge_packages.errors import KnowledgePackageError

    archive = _write_zip(
        tmp_path / "tampered.zip",
        {"manifest.json": _manifest(digest="0" * 64), "assets/feature_prompt.md": b"prompt"},
    )
    with pytest.raises(KnowledgePackageError, match="SHA256"):
        validate_archive(archive)


def test_validate_archive_rejects_sensitive_gold_dataset(tmp_path: Path) -> None:
    from logrisk.knowledge_packages.archive import validate_archive
    from logrisk.knowledge_packages.errors import KnowledgePackageError

    gold = b'{"evidence":{"samples":["raw log"]}}\n'
    digest = hashlib.sha256(gold).hexdigest()
    manifest = _manifest(asset_path="assets/gold_dataset.jsonl", digest=digest)
    # Adjust the asset type for this focused security check.
    payload = json.loads(manifest)
    payload["assets"][0].update({"asset_id": "gold", "type": "gold_dataset", "media_type": "application/jsonl"})
    archive = _write_zip(
        tmp_path / "sensitive.zip",
        {"manifest.json": json.dumps(payload).encode(), "assets/gold_dataset.jsonl": gold},
    )
    with pytest.raises(KnowledgePackageError, match="敏感字段"):
        validate_archive(archive)
