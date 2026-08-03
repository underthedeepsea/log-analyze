from __future__ import annotations

import hashlib

import pytest


def test_shared_store_rejects_escape_and_promotes_atomically(tmp_path) -> None:
    from logrisk.artifact_storage import ArtifactPathError, SharedArtifactStore

    store = SharedArtifactStore(tmp_path / "shared")
    with pytest.raises(ArtifactPathError):
        store.resolve("../secret")

    staged = store.stage_bytes("uploads", b"safe log")
    artifact = store.promote(
        staged,
        "uploads/messages",
        expected_sha256=hashlib.sha256(b"safe log").hexdigest(),
    )

    assert artifact.relative_path == "uploads/messages"
    assert store.resolve(artifact.relative_path).read_bytes() == b"safe log"
    assert store.open_read(artifact.relative_path).read() == b"safe log"


def test_shared_store_removes_failed_stage_without_creating_target(tmp_path) -> None:
    from logrisk.artifact_storage import ArtifactIntegrityError, SharedArtifactStore

    store = SharedArtifactStore(tmp_path / "shared")
    staged = store.stage_bytes("uploads", b"safe log")

    with pytest.raises(ArtifactIntegrityError):
        store.promote(staged, "uploads/messages", expected_sha256="0" * 64)

    assert not staged.path.exists()
    assert not store.resolve("uploads/messages").exists()


def test_shared_store_rejects_existing_symlink_artifacts(tmp_path) -> None:
    from logrisk.artifact_storage import ArtifactPathError, SharedArtifactStore

    store = SharedArtifactStore(tmp_path / "shared")
    target = tmp_path / "outside"
    target.write_text("outside", encoding="utf-8")
    link = store.root / "uploads" / "linked"
    link.parent.mkdir(parents=True)
    link.symlink_to(target)

    with pytest.raises(ArtifactPathError):
        store.resolve("uploads/linked")
