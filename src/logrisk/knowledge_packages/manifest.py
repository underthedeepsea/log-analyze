from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any

from .errors import KnowledgePackageError


PACKAGE_ID_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
VERSION_RE = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

ASSET_EXTENSIONS: dict[str, frozenset[str]] = {
    "drain3_profile": frozenset({".ini"}),
    "semantic_dictionary": frozenset({".yaml", ".yml"}),
    "feature_prompt": frozenset({".md"}),
    "risk_semantics": frozenset({".yaml", ".yml"}),
    "approved_rule_candidates": frozenset({".json"}),
    "gold_dataset": frozenset({".jsonl"}),
}


def _safe_asset_path(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise KnowledgePackageError("资产路径不能为空")
    path = PurePosixPath(value)
    if path.is_absolute() or "\\" in value or any(part in {"", ".", ".."} for part in path.parts):
        raise KnowledgePackageError("资产路径必须是 assets/ 下的规范相对路径", code="package_path_invalid")
    normalized = path.as_posix()
    if not normalized.startswith("assets/") or normalized == "assets/":
        raise KnowledgePackageError("资产路径必须位于 assets/ 目录", code="package_path_invalid")
    return normalized


def _required_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise KnowledgePackageError(f"{label}不能为空")
    return value.strip()


def _version(value: Any, label: str) -> str:
    result = _required_string(value, label)
    if not VERSION_RE.fullmatch(result):
        raise KnowledgePackageError(f"{label}必须是三段数字版本")
    return result


@dataclass(frozen=True)
class AssetManifest:
    asset_id: str
    type: str
    path: str
    sha256: str
    media_type: str

    @classmethod
    def from_dict(cls, payload: Any) -> "AssetManifest":
        if not isinstance(payload, dict):
            raise KnowledgePackageError("assets 必须是对象数组")
        asset_id = _required_string(payload.get("asset_id"), "asset_id")
        if not PACKAGE_ID_RE.fullmatch(asset_id):
            raise KnowledgePackageError("asset_id 格式无效")
        asset_type = _required_string(payload.get("type"), "资产类型")
        if asset_type not in ASSET_EXTENSIONS:
            raise KnowledgePackageError(f"不支持的资产类型: {asset_type}", code="asset_type_unsupported")
        path = _safe_asset_path(payload.get("path"))
        if PurePosixPath(path).suffix.lower() not in ASSET_EXTENSIONS[asset_type]:
            raise KnowledgePackageError(f"资产扩展名与类型不匹配: {asset_id}")
        digest = _required_string(payload.get("sha256"), "资产 SHA256").lower()
        if not SHA256_RE.fullmatch(digest):
            raise KnowledgePackageError("资产 SHA256 格式无效", code="package_checksum_invalid")
        return cls(
            asset_id=asset_id,
            type=asset_type,
            path=path,
            sha256=digest,
            media_type=_required_string(payload.get("media_type"), "资产媒体类型"),
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "asset_id": self.asset_id,
            "type": self.type,
            "path": self.path,
            "sha256": self.sha256,
            "media_type": self.media_type,
        }


@dataclass(frozen=True)
class PackageManifest:
    schema_version: int
    package_id: str
    name: str
    version: str
    description: str
    platform: dict[str, str]
    dependencies: tuple[dict[str, str], ...]
    assets: tuple[AssetManifest, ...]

    @classmethod
    def from_dict(cls, payload: Any) -> "PackageManifest":
        if not isinstance(payload, dict):
            raise KnowledgePackageError("manifest.json 必须是 JSON object")
        expected = {"schema_version", "package_id", "name", "version", "description", "platform", "dependencies", "assets"}
        unknown = set(payload) - expected
        missing = expected - set(payload)
        if unknown:
            raise KnowledgePackageError(f"Manifest 包含未知字段: {', '.join(sorted(unknown))}")
        if missing:
            raise KnowledgePackageError(f"Manifest 缺少字段: {', '.join(sorted(missing))}")
        if payload.get("schema_version") != 1:
            raise KnowledgePackageError("不支持的 Manifest schema_version")
        package_id = _required_string(payload.get("package_id"), "package_id")
        if not PACKAGE_ID_RE.fullmatch(package_id):
            raise KnowledgePackageError("package_id 格式无效")
        version = _version(payload.get("version"), "version")
        platform = payload.get("platform")
        if not isinstance(platform, dict):
            raise KnowledgePackageError("platform 必须是 object")
        platform_expected = {"min_version", "max_version_exclusive"}
        if set(platform) != platform_expected:
            raise KnowledgePackageError("platform 必须包含 min_version 和 max_version_exclusive")
        normalized_platform = {
            "min_version": _version(platform.get("min_version"), "platform.min_version"),
            "max_version_exclusive": _version(platform.get("max_version_exclusive"), "platform.max_version_exclusive"),
        }
        if _version_tuple(normalized_platform["min_version"]) >= _version_tuple(normalized_platform["max_version_exclusive"]):
            raise KnowledgePackageError("platform 版本范围无效")
        dependencies_payload = payload.get("dependencies")
        if not isinstance(dependencies_payload, list):
            raise KnowledgePackageError("dependencies 必须是数组")
        dependencies: list[dict[str, str]] = []
        seen_dependencies: set[tuple[str, str]] = set()
        for item in dependencies_payload:
            if not isinstance(item, dict) or set(item) != {"package_id", "version"}:
                raise KnowledgePackageError("dependencies 项必须包含 package_id 和 version")
            dependency_id = _required_string(item.get("package_id"), "依赖 package_id")
            dependency_version = _version(item.get("version"), "依赖 version")
            if not PACKAGE_ID_RE.fullmatch(dependency_id):
                raise KnowledgePackageError("依赖 package_id 格式无效")
            key = (dependency_id, dependency_version)
            if key in seen_dependencies:
                raise KnowledgePackageError("dependencies 不得重复")
            seen_dependencies.add(key)
            dependencies.append({"package_id": dependency_id, "version": dependency_version})
        assets_payload = payload.get("assets")
        if not isinstance(assets_payload, list) or not assets_payload:
            raise KnowledgePackageError("assets 必须是非空数组")
        assets = tuple(AssetManifest.from_dict(item) for item in assets_payload)
        if len({asset.asset_id for asset in assets}) != len(assets):
            raise KnowledgePackageError("asset_id 不得重复")
        if len({asset.path for asset in assets}) != len(assets):
            raise KnowledgePackageError("资产路径不得重复")
        return cls(
            schema_version=1,
            package_id=package_id,
            name=_required_string(payload.get("name"), "name"),
            version=version,
            description=_required_string(payload.get("description"), "description"),
            platform=normalized_platform,
            dependencies=tuple(dependencies),
            assets=assets,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "package_id": self.package_id,
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "platform": dict(self.platform),
            "dependencies": [dict(item) for item in self.dependencies],
            "assets": [asset.to_dict() for asset in self.assets],
        }


def parse_manifest(raw: bytes) -> PackageManifest:
    """Parse the UTF-8 ``manifest.json`` bytes from a package."""
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise KnowledgePackageError("manifest.json 不是有效 UTF-8 JSON", code="manifest_invalid") from exc
    return PackageManifest.from_dict(payload)


def _version_tuple(value: str) -> tuple[int, int, int]:
    return tuple(int(part) for part in value.split("."))  # type: ignore[return-value]
