"""Offline, data-only knowledge package support."""

from .errors import KnowledgePackageError
from .manifest import AssetManifest, PackageManifest, parse_manifest

__all__ = [
    "ArchiveInspection",
    "AssetManifest",
    "KnowledgePackageError",
    "PackageManifest",
    "build_archive",
    "parse_manifest",
    "validate_archive",
]


def __getattr__(name: str):
    # Keep ``python -m logrisk.knowledge_packages.archive`` free of the
    # runpy warning caused by eagerly importing the executed submodule.
    if name in {"ArchiveInspection", "build_archive", "validate_archive"}:
        from .archive import ArchiveInspection, build_archive, validate_archive
        return {"ArchiveInspection": ArchiveInspection, "build_archive": build_archive, "validate_archive": validate_archive}[name]
    raise AttributeError(name)
