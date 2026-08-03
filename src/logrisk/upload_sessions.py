from __future__ import annotations

import hashlib
import json
import math
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from logrisk.artifact_storage import SharedArtifactStore


@dataclass(frozen=True)
class UploadConfig:
    upload_dir: Path
    inline_max_bytes: int = 10 * 1024 * 1024
    chunk_size_bytes: int = 1024 * 1024
    max_upload_bytes: int = 500 * 1024 * 1024
    retain_days: int = 7
    allowed_extensions: tuple[str, ...] = (".json", ".jsonl", ".ndjson", ".txt", ".log", ".gz", "")
    artifact_store: SharedArtifactStore | None = None


class UploadSessionStore:
    def __init__(self, config: UploadConfig):
        self.config = config
        self.config.upload_dir.mkdir(parents=True, exist_ok=True)

    def create(self, *, filename: str, size_bytes: int, chunk_size_bytes: int | None = None) -> dict[str, Any]:
        safe_filename = Path(filename).name or "upload.log"
        self._validate_filename(safe_filename)
        self._validate_size(size_bytes)
        chunk_size = int(chunk_size_bytes or self.config.chunk_size_bytes)
        if chunk_size <= 0:
            raise ValueError("chunk_size_bytes must be positive")
        upload_id = "upl_" + uuid.uuid4().hex
        root = self._root(upload_id)
        (root / "chunks").mkdir(parents=True, exist_ok=False)
        now = self._now()
        manifest = {
            "upload_id": upload_id,
            "filename": filename,
            "safe_filename": safe_filename,
            "size_bytes": int(size_bytes),
            "chunk_size_bytes": chunk_size,
            "total_chunks": math.ceil(int(size_bytes) / chunk_size),
            "received_chunks": [],
            "sha256": None,
            "status": "uploading",
            "created_at": now,
            "updated_at": now,
            "completed_at": None,
            "error": None,
        }
        self._write_manifest(upload_id, manifest)
        return manifest

    def append_chunk(self, *, upload_id: str, index: int, data: bytes, chunk_sha256: str | None = None) -> dict[str, Any]:
        manifest = self.get(upload_id)
        if manifest["status"] != "uploading":
            raise ValueError("Upload is not accepting chunks")
        if index < 0 or index >= int(manifest["total_chunks"]):
            raise ValueError("Invalid chunk index")
        if chunk_sha256 and hashlib.sha256(data).hexdigest() != chunk_sha256:
            raise ValueError("Chunk SHA256 mismatch")
        (self._root(upload_id) / "chunks" / f"{index:06d}.part").write_bytes(data)
        received = set(int(item) for item in manifest.get("received_chunks", []))
        received.add(index)
        manifest["received_chunks"] = sorted(received)
        manifest["updated_at"] = self._now()
        self._write_manifest(upload_id, manifest)
        return manifest

    def complete(self, *, upload_id: str, final_sha256: str | None = None) -> dict[str, Any]:
        manifest = self.get(upload_id)
        missing = [i for i in range(int(manifest["total_chunks"])) if i not in set(manifest.get("received_chunks", []))]
        if missing:
            raise ValueError(f"Missing chunks: {missing[:10]}")
        root = self._root(upload_id)
        assembled = root / "source.log.assembled"
        digest = hashlib.sha256()
        with assembled.open("wb") as out:
            for index in range(int(manifest["total_chunks"])):
                data = (root / "chunks" / f"{index:06d}.part").read_bytes()
                out.write(data)
                digest.update(data)
        actual = digest.hexdigest()
        if final_sha256 and actual != final_sha256:
            assembled.unlink(missing_ok=True)
            raise ValueError("Final file SHA256 mismatch")
        if assembled.stat().st_size != int(manifest["size_bytes"]):
            assembled.unlink(missing_ok=True)
            raise ValueError("Final file size mismatch")
        if self.config.artifact_store:
            try:
                staged = self.config.artifact_store.stage_file("uploads", assembled)
                artifact = self.config.artifact_store.promote(
                    staged,
                    f"uploads/{upload_id}/source.log",
                    expected_sha256=actual,
                )
            finally:
                assembled.unlink(missing_ok=True)
            manifest["artifact_relative_path"] = artifact.relative_path
        else:
            target = root / "source.log"
            assembled.replace(target)
            manifest["artifact_relative_path"] = None
        manifest.update({"sha256": actual, "status": "completed", "completed_at": self._now(), "updated_at": self._now()})
        self._write_manifest(upload_id, manifest)
        (root / "upload.done").write_text("done", encoding="utf-8")
        return manifest

    def get(self, upload_id: str) -> dict[str, Any]:
        path = self._manifest_path(upload_id)
        if not path.is_file():
            raise KeyError(f"Upload not found: {upload_id}")
        return json.loads(path.read_text(encoding="utf-8"))

    def source_path(self, upload_id: str) -> Path:
        manifest = self.get(upload_id)
        relative_path = manifest.get("artifact_relative_path")
        if relative_path and self.config.artifact_store:
            path = self.config.artifact_store.resolve(str(relative_path))
            if not path.is_file():
                raise FileNotFoundError(path)
            return path
        path = self._root(upload_id) / "source.log"
        if not path.is_file():
            raise FileNotFoundError(path)
        return path

    def source_reference(self, upload_id: str) -> str:
        manifest = self.get(upload_id)
        relative_path = manifest.get("artifact_relative_path")
        if relative_path:
            return str(relative_path)
        return str(self.source_path(upload_id))

    def _root(self, upload_id: str) -> Path:
        return self.config.upload_dir / upload_id

    def _manifest_path(self, upload_id: str) -> Path:
        return self._root(upload_id) / "manifest.json"

    def _write_manifest(self, upload_id: str, manifest: dict[str, Any]) -> None:
        path = self._manifest_path(upload_id)
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)

    def _validate_size(self, size_bytes: int) -> None:
        if int(size_bytes) <= 0:
            raise ValueError("Empty file is not allowed")
        if int(size_bytes) > self.config.max_upload_bytes:
            raise ValueError("File exceeds max_upload_bytes")

    def _validate_filename(self, safe_filename: str) -> None:
        suffix = Path(safe_filename).suffix.lower()
        if suffix not in self.config.allowed_extensions:
            raise ValueError(f"Unsupported file extension: {safe_filename}")

    def _now(self) -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%S%z")
