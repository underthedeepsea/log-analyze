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
