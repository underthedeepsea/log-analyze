from __future__ import annotations

import hashlib
import argparse
import json
import os
import stat
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from .errors import KnowledgePackageError
from .manifest import PackageManifest, parse_manifest


DEFAULT_MAX_COMPRESSED_BYTES = 100 * 1024 * 1024
DEFAULT_MAX_EXPANDED_BYTES = 500 * 1024 * 1024
DEFAULT_MAX_FILES = 2_000
_FORBIDDEN_DATA_KEYS = {
    "samples", "raw_sample", "raw_samples", "raw_log", "raw_logs", "raw_message",
    "api_key", "apikey", "token", "password", "secret", "dsn", "authorization",
}


@dataclass(frozen=True)
class ArchiveInspection:
    path: Path
    manifest: PackageManifest
    package_sha256: str
    compressed_bytes: int
    expanded_bytes: int
    files: tuple[str, ...]
    asset_sha256: dict[str, str]

    def public_dict(self) -> dict[str, Any]:
        return {
            "package_sha256": self.package_sha256,
            "compressed_bytes": self.compressed_bytes,
            "expanded_bytes": self.expanded_bytes,
            "files": list(self.files),
            "manifest": self.manifest.to_dict(),
        }


def build_archive(source_root: str | Path, output_path: str | Path) -> ArchiveInspection:
    source = Path(source_root).resolve()
    target = Path(output_path).resolve()
    if not source.is_dir() or source.is_symlink():
        raise KnowledgePackageError("知识包源目录不存在或不是普通目录")
    manifest_path = source / "manifest.json"
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise KnowledgePackageError("知识包源目录缺少 manifest.json")
    manifest = parse_manifest(manifest_path.read_bytes())
    expected_paths = {"manifest.json", *(asset.path for asset in manifest.assets)}
    actual_paths = {
        path.relative_to(source).as_posix()
        for path in source.rglob("*")
        if path.is_file() and not path.is_symlink()
    }
    if actual_paths != expected_paths:
        missing = expected_paths - actual_paths
        extra = actual_paths - expected_paths
        detail = []
        if missing:
            detail.append("缺少 " + ", ".join(sorted(missing)))
        if extra:
            detail.append("未声明 " + ", ".join(sorted(extra)))
        raise KnowledgePackageError("知识包源文件清单不一致: " + "; ".join(detail))
    for asset in manifest.assets:
        digest = _sha256(source / asset.path)
        if digest != asset.sha256:
            raise KnowledgePackageError(f"资产 SHA256 校验失败: {asset.asset_id}", code="package_checksum_invalid")
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        target.unlink()
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for relative in sorted(actual_paths):
            data = (source / relative).read_bytes()
            info = zipfile.ZipInfo(relative, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = (stat.S_IFREG | 0o600) << 16
            archive.writestr(info, data)
    return validate_archive(target)


def validate_archive(
    path: str | Path,
    *,
    max_compressed_bytes: int = DEFAULT_MAX_COMPRESSED_BYTES,
    max_expanded_bytes: int = DEFAULT_MAX_EXPANDED_BYTES,
    max_files: int = DEFAULT_MAX_FILES,
) -> ArchiveInspection:
    archive_path = Path(path).resolve()
    if not archive_path.is_file() or archive_path.is_symlink():
        raise KnowledgePackageError("知识包文件不存在或不是普通文件", code="package_not_found")
    compressed_bytes = archive_path.stat().st_size
    if compressed_bytes > max_compressed_bytes:
        raise KnowledgePackageError("知识包压缩大小超过限制", code="package_too_large")
    try:
        with zipfile.ZipFile(archive_path) as archive:
            infos = archive.infolist()
            if len(infos) > max_files:
                raise KnowledgePackageError("知识包文件数量超过限制", code="package_too_many_files")
            names = [info.filename for info in infos]
            if len(set(names)) != len(names):
                raise KnowledgePackageError("知识包包含重复路径", code="package_path_duplicate")
            _validate_zip_members(infos)
            expanded_bytes = sum(max(0, int(info.file_size)) for info in infos)
            if expanded_bytes > max_expanded_bytes:
                raise KnowledgePackageError("知识包展开大小超过限制", code="package_expanded_too_large")
            if "manifest.json" not in names:
                raise KnowledgePackageError("知识包缺少 manifest.json")
            manifest = parse_manifest(archive.read("manifest.json"))
            expected = {asset.path: asset for asset in manifest.assets}
            actual = set(names) - {"manifest.json"}
            if actual != set(expected):
                missing = set(expected) - actual
                extra = actual - set(expected)
                detail = []
                if missing:
                    detail.append("缺少 " + ", ".join(sorted(missing)))
                if extra:
                    detail.append("未声明 " + ", ".join(sorted(extra)))
                raise KnowledgePackageError("知识包资产清单不一致: " + "; ".join(detail))
            asset_sha256: dict[str, str] = {}
            for asset in manifest.assets:
                content = archive.read(asset.path)
                digest = hashlib.sha256(content).hexdigest()
                if digest != asset.sha256:
                    raise KnowledgePackageError(f"资产 SHA256 校验失败: {asset.asset_id}", code="package_checksum_invalid")
                if asset.type == "gold_dataset":
                    _validate_gold_dataset(content)
                asset_sha256[asset.asset_id] = digest
    except zipfile.BadZipFile as exc:
        raise KnowledgePackageError("知识包不是有效 ZIP", code="package_format_invalid") from exc
    return ArchiveInspection(
        path=archive_path,
        manifest=manifest,
        package_sha256=_sha256(archive_path),
        compressed_bytes=compressed_bytes,
        expanded_bytes=expanded_bytes,
        files=tuple(sorted(names)),
        asset_sha256=asset_sha256,
    )


def _validate_zip_members(infos: list[zipfile.ZipInfo]) -> None:
    for info in infos:
        name = info.filename
        if not name or "\\" in name:
            raise KnowledgePackageError("知识包包含非法路径", code="package_path_invalid")
        path = PurePosixPath(name)
        if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
            raise KnowledgePackageError("知识包包含路径穿越", code="package_path_invalid")
        mode = (info.external_attr >> 16) & 0xFFFF
        if stat.S_ISLNK(mode) or stat.S_ISDIR(mode):
            raise KnowledgePackageError("知识包不允许目录或符号链接条目", code="package_member_unsafe")
        if name != "manifest.json" and not name.startswith("assets/"):
            raise KnowledgePackageError("知识包只能包含 manifest.json 和 assets/ 文件", code="package_path_invalid")


def _validate_gold_dataset(content: bytes) -> None:
    try:
        lines = content.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise KnowledgePackageError("Gold Dataset 必须是 UTF-8 JSONL", code="gold_dataset_invalid") from exc
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise KnowledgePackageError(f"Gold Dataset 第 {line_number} 行不是有效 JSON", code="gold_dataset_invalid") from exc
        _reject_sensitive_values(payload, f"Gold Dataset 第 {line_number} 行")


def _reject_sensitive_values(value: Any, location: str) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = str(key).strip().lower().replace("-", "_")
            if normalized in _FORBIDDEN_DATA_KEYS:
                raise KnowledgePackageError(f"{location} 包含敏感字段 {key}", code="gold_dataset_sensitive")
            _reject_sensitive_values(child, location)
    elif isinstance(value, list):
        for child in value:
            _reject_sensitive_values(child, location)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="构建或校验 LOGRISK 离线知识包")
    subparsers = parser.add_subparsers(dest="command", required=True)
    build_parser = subparsers.add_parser("build", help="从知识包源目录构建确定性 ZIP")
    build_parser.add_argument("source_root", type=Path)
    build_parser.add_argument("output_path", type=Path)
    validate_parser = subparsers.add_parser("validate", help="校验知识包 ZIP")
    validate_parser.add_argument("package_path", type=Path)
    args = parser.parse_args(argv)
    try:
        inspection = (
            build_archive(args.source_root, args.output_path)
            if args.command == "build"
            else validate_archive(args.package_path)
        )
    except KnowledgePackageError as exc:
        parser.error(str(exc))
    print(json.dumps(inspection.public_dict(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through the CLI
    raise SystemExit(main())
