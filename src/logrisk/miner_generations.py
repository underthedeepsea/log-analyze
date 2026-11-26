from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from pathlib import Path
from typing import Any


def prepare_generation(root: Path, previous: dict[str, Any] | None) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    target = root / uuid.uuid4().hex
    if previous:
        source = root / str(previous["generation"])
        if source.resolve().parent != root.resolve():
            raise ValueError("Invalid miner generation path")
        manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
        if manifest != previous:
            raise ValueError("Miner generation manifest mismatch")
        for name, digest in manifest["files"].items():
            path = source / name
            if path.resolve().parent != source.resolve() or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                raise ValueError("Miner generation content mismatch")
        shutil.copytree(source, target, ignore=shutil.ignore_patterns("manifest.json"))
    else:
        target.mkdir()
    return target


def seal_generation(path: Path) -> dict[str, Any]:
    files = {}
    for item in sorted(path.glob("*.bin")):
        with item.open("rb") as stream:
            files[item.name] = hashlib.sha256(stream.read()).hexdigest()
            os.fsync(stream.fileno())
    manifest = {"generation": path.name, "files": files}
    with (path / "manifest.json").open("x", encoding="utf-8") as stream:
        json.dump(manifest, stream, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    for directory in (path, path.parent):
        descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return manifest


class MinerGenerationWorkspace:
    """Stage changed shards privately and retain the live, sealed shard index."""

    def __init__(
        self, generation_root: Path, config_hash: str, previous: dict[str, Any] | None, shard_names: set[str],
    ) -> None:
        if len(config_hash) != 64 or any(char not in "0123456789abcdef" for char in config_hash):
            raise ValueError("Invalid miner configuration hash")
        self.legacy_root = generation_root
        self.root = generation_root / "rolling_v2" / config_hash
        self.previous = previous
        self.shard_names = sorted(shard_names)
        self.generation = uuid.uuid4().hex
        self.work = self.root / f"work-{self.generation}"
        self.pending = self.root / "pending.json"
        self.active = self.root / "current.json"
        self.index: dict[str, Any] = {}
        self._lock = None

    def __enter__(self) -> MinerGenerationWorkspace:
        self._acquire()
        try:
            self._prepare()
        except BaseException:
            self._lock.close()
            raise
        return self

    def __exit__(self, *_: Any) -> None:
        self._lock.close()

    def _acquire(self) -> None:
        import fcntl

        lock_path = self.root / "workspace.lock"
        if any(path.is_symlink() for path in (self.legacy_root, self.root.parent, self.root, lock_path)):
            raise ValueError("Invalid miner workspace path")
        if not self.root.is_dir():
            self.root.mkdir(parents=True, exist_ok=True)
            for directory in (self.root.parent, self.legacy_root, self.legacy_root.parent, self.legacy_root.parent.parent):
                self._sync_directory(directory)
        self._lock = lock_path.open("a")
        try:
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            self._lock.close()
            raise

    def reconcile(self) -> None:
        """Resolve a crash after checkpointing but before filesystem activation."""
        self._acquire()
        try:
            self._recover_frontier()
        finally:
            self._lock.close()

    def _recover_frontier(self) -> dict[str, Any] | None:
        if self.pending.exists():
            self._settle(self._read(self.pending), self.previous)
        active = self._read(self.active) if self.active.exists() else None
        if self._is_rolling(self.previous) and self.previous != active:
            raise ValueError("Miner workspace frontier mismatch")
        return active

    def _prepare(self) -> None:
        active = self._recover_frontier()
        if self._is_rolling(self.previous):
            self.index = self._load_index(active)
        elif self.previous:
            generation = self._valid_id(self.previous["generation"])
            manifest = self._read(self.legacy_root / generation / "manifest.json")
            if manifest != self.previous:
                raise ValueError("Miner generation manifest mismatch")
            self.index = {
                name: {"legacy": generation, "sha256": digest}
                for name, digest in manifest["files"].items()
            }
        self._write(self.pending, {
            "generation": self.generation, "base": active, "shards": self.shard_names,
            "reset": not self._is_rolling(self.previous),
        })
        self.work.mkdir()
        for name in self.shard_names:
            prior = self.index.get(name)
            if prior is None:
                continue
            source = self._shard_path(name, prior)
            digest = hashlib.sha256()
            with source.open("rb") as reader, (self.work / self._valid_name(name)).open("xb") as writer:
                while data := reader.read(1024 * 1024):
                    digest.update(data)
                    writer.write(data)
            if digest.hexdigest() != prior["sha256"]:
                raise ValueError("Miner generation content mismatch")

    def seal(self) -> dict[str, Any]:
        files = {}
        index = dict(self.index)
        for name in self.shard_names:
            item = self.work / self._valid_name(name)
            with item.open("rb") as stream:
                hasher = hashlib.sha256()
                while data := stream.read(1024 * 1024):
                    hasher.update(data)
                os.fsync(stream.fileno())
            digest = hasher.hexdigest()
            files[name] = digest
            if digest != (index.get(name) or {}).get("sha256"):
                os.replace(item, self._blob(self.generation, name))
                index[name] = {"generation": self.generation, "sha256": digest}
        index_sha256 = self._write(self._index_path(self.generation), index)
        return {
            "format": "rolling_v2", "generation": self.generation,
            "index_sha256": index_sha256, "files": files,
        }

    def activate(self, manifest: dict[str, Any]) -> None:
        """Finalize after the database transaction has committed this manifest."""
        self._settle(self._read(self.pending), manifest)

    def _settle(self, intent: dict[str, Any], frontier: dict[str, Any] | None) -> None:
        generation = self._valid_id(intent["generation"])
        old = intent["base"]
        if self._is_rolling(frontier) and frontier["generation"] == generation:
            active_index = self._load_index(frontier)
            if not self.active.exists() or self._read(self.active) != frontier:
                self._write(self.active, frontier)
            if old:
                old_path = self._index_path(old["generation"])
                if old_path.exists():
                    old_index = self._load_index(old)
                    replaced = old_index if intent.get("reset") else intent["shards"]
                    for name in replaced:
                        prior = old_index.get(name)
                        if prior and prior.get("generation") and prior != active_index.get(name):
                            self._shard_path(name, prior).unlink(missing_ok=True)
                    old_path.unlink()
        else:
            if self._is_rolling(frontier) and old != frontier:
                raise ValueError("Miner workspace recovery frontier mismatch")
            for name in intent["shards"]:
                self._blob(generation, name).unlink(missing_ok=True)
            self._index_path(generation).unlink(missing_ok=True)
        work = self.root / f"work-{generation}"
        if work.exists():
            if work.is_symlink():
                raise ValueError("Invalid miner workspace path")
            shutil.rmtree(work)
        self.pending.unlink()
        self._sync_directory()

    def _load_index(self, manifest: dict[str, Any]) -> dict[str, Any]:
        path = self._index_path(manifest["generation"])
        if path.is_symlink():
            raise ValueError("Invalid miner index path")
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != manifest["index_sha256"]:
            raise ValueError("Miner generation index mismatch")
        return json.loads(data)

    def _shard_path(self, name: str, shard: dict[str, Any]) -> Path:
        if "legacy" in shard:
            path = self.legacy_root / self._valid_id(shard["legacy"]) / self._valid_name(name)
            if path.parent.is_symlink():
                raise ValueError("Invalid miner generation path")
        else:
            path = self._blob(shard["generation"], name)
        if path.is_symlink():
            raise ValueError("Invalid miner shard path")
        return path

    def _blob(self, generation: str, name: str) -> Path:
        shard = hashlib.sha256(self._valid_name(name).encode("utf-8")).hexdigest()
        return self.root / f"shard-{self._valid_id(generation)}-{shard}.bin"

    def _index_path(self, generation: str) -> Path:
        return self.root / f"index-{self._valid_id(generation)}.json"

    @staticmethod
    def _is_rolling(manifest: dict[str, Any] | None) -> bool:
        return bool(manifest and manifest.get("format") == "rolling_v2")

    @staticmethod
    def _valid_id(value: str) -> str:
        if len(value) != 32 or any(char not in "0123456789abcdef" for char in value):
            raise ValueError("Invalid miner generation ID")
        return value

    @staticmethod
    def _valid_name(name: str) -> str:
        if not name.endswith(".bin") or Path(name).name != name:
            raise ValueError("Invalid miner shard name")
        return name

    @staticmethod
    def _read(path: Path) -> dict[str, Any]:
        if path.is_symlink():
            raise ValueError("Invalid miner workspace path")
        return json.loads(path.read_bytes())

    def _write(self, path: Path, value: dict[str, Any]) -> str:
        data = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        temporary = path.with_suffix(".tmp")
        if temporary.is_symlink():
            raise ValueError("Invalid miner workspace path")
        with temporary.open("wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        self._sync_directory()
        return hashlib.sha256(data).hexdigest()

    def _sync_directory(self, path: Path | None = None) -> None:
        descriptor = os.open(path or self.root, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
