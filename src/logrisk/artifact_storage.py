from __future__ import annotations

import hashlib
import os
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO


class ArtifactPathError(ValueError):
    """Raised when an artifact path is not contained by the shared root."""


class ArtifactIntegrityError(ValueError):
    """Raised when a staged artifact does not match its declared digest."""


@dataclass(frozen=True)
class StagedArtifact:
    path: Path
    namespace: str


@dataclass(frozen=True)
class SharedArtifact:
    relative_path: str
    size_bytes: int
    sha256: str


class SharedArtifactStore:
    """Filesystem-only artifact boundary shared by Django, Airflow workers, and Dashboard.

    The database stores ``relative_path`` only. Files are staged under the same
    shared root, validated, and then atomically promoted with ``os.replace``.
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        if self.root.is_symlink():
            raise ArtifactPathError("共享目录不能是符号链接")
        self._staging_root = self.root / ".staging"
        self._staging_root.mkdir(parents=True, exist_ok=True)

    def resolve(self, relative_path: str | Path) -> Path:
        relative = self._relative(relative_path)
        candidate = self.root / relative
        self._reject_symlink_components(candidate)
        resolved = candidate.resolve(strict=False)
        if resolved == self.root or self.root not in resolved.parents:
            raise ArtifactPathError("Artifact 路径超出共享目录")
        return resolved

    def relative_path(self, path: str | Path) -> str:
        candidate = Path(path).resolve(strict=False)
        self._reject_symlink_components(Path(path))
        if candidate == self.root or self.root not in candidate.parents:
            raise ArtifactPathError("Artifact 路径超出共享目录")
        return candidate.relative_to(self.root).as_posix()

    def stage_bytes(self, namespace: str, data: bytes) -> StagedArtifact:
        path = self._stage_path(namespace)
        try:
            with path.open("xb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            return StagedArtifact(path=path, namespace=self._relative(namespace).as_posix())
        except Exception:
            self.delete_candidate(path)
            raise

    def stage_file(self, namespace: str, source: str | Path) -> StagedArtifact:
        source_path = Path(source)
        if not source_path.is_file() or source_path.is_symlink():
            raise ArtifactPathError("暂存来源必须是普通文件")
        path = self._stage_path(namespace)
        try:
            with source_path.open("rb") as reader, path.open("xb") as writer:
                shutil.copyfileobj(reader, writer, length=1024 * 1024)
                writer.flush()
                os.fsync(writer.fileno())
            return StagedArtifact(path=path, namespace=self._relative(namespace).as_posix())
        except Exception:
            self.delete_candidate(path)
            raise

    def promote(
        self,
        staged: StagedArtifact,
        relative_path: str | Path,
        *,
        expected_sha256: str | None = None,
    ) -> SharedArtifact:
        staged_path = Path(staged.path)
        self._ensure_staged(staged_path)
        target = self.resolve(relative_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target = self.resolve(relative_path)
        try:
            digest, size_bytes = self._digest(staged_path)
            if expected_sha256 and digest != self._digest_value(expected_sha256):
                raise ArtifactIntegrityError("Artifact SHA256 校验失败")
            if target.exists():
                raise ArtifactPathError("Artifact 目标路径已存在")
            os.replace(staged_path, target)
            self._fsync_directory(target.parent)
            return SharedArtifact(
                relative_path=self.relative_path(target),
                size_bytes=size_bytes,
                sha256=digest,
            )
        except Exception:
            self.delete_candidate(staged_path)
            raise

    def open_read(self, relative_path: str | Path) -> BinaryIO:
        path = self.resolve(relative_path)
        if not path.is_file() or path.is_symlink():
            raise ArtifactPathError("Artifact 不存在或不是普通文件")
        return path.open("rb")

    def delete_candidate(self, candidate: StagedArtifact | str | Path) -> None:
        path = Path(candidate.path if isinstance(candidate, StagedArtifact) else candidate).resolve(strict=False)
        if path == self._staging_root or self._staging_root not in path.parents:
            raise ArtifactPathError("仅允许删除本次暂存文件")
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path, ignore_errors=True)
        else:
            path.unlink(missing_ok=True)

    def _stage_path(self, namespace: str) -> Path:
        safe_namespace = self._relative(namespace).as_posix().replace("/", "-")
        return self._staging_root / f"{safe_namespace}-{uuid.uuid4().hex}.tmp"

    def _ensure_staged(self, path: Path) -> None:
        resolved = path.resolve(strict=False)
        if self._staging_root not in resolved.parents or not resolved.is_file() or resolved.is_symlink():
            raise ArtifactPathError("暂存 Artifact 无效")

    @staticmethod
    def _digest(path: Path) -> tuple[str, int]:
        digest = hashlib.sha256()
        size_bytes = 0
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
                size_bytes += len(chunk)
        return digest.hexdigest(), size_bytes

    @staticmethod
    def _digest_value(value: str) -> str:
        digest = str(value).strip().lower()
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ArtifactIntegrityError("Artifact SHA256 格式无效")
        return digest

    def _relative(self, value: str | Path) -> Path:
        raw = str(value).strip()
        if not raw:
            raise ArtifactPathError("Artifact 相对路径不能为空")
        path = Path(raw)
        if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
            raise ArtifactPathError("Artifact 路径必须位于共享目录内")
        return path

    def _reject_symlink_components(self, candidate: Path) -> None:
        try:
            relative = candidate.relative_to(self.root)
        except ValueError as exc:
            raise ArtifactPathError("Artifact 路径超出共享目录") from exc
        current = self.root
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                raise ArtifactPathError("Artifact 不允许使用符号链接")

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        try:
            descriptor = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(descriptor)
        except OSError:
            pass
        finally:
            os.close(descriptor)
