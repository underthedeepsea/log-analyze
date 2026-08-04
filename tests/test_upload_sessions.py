import hashlib

import pytest

from logrisk.upload_sessions import UploadConfig, UploadSessionStore


def store(tmp_path):
    return UploadSessionStore(UploadConfig(upload_dir=tmp_path / "uploads", max_upload_bytes=20))


def test_upload_session_accepts_extensionless_file_and_completes(tmp_path):
    sessions = store(tmp_path)
    manifest = sessions.create(filename="messages", size_bytes=11, chunk_size_bytes=5)

    sessions.append_chunk(upload_id=manifest["upload_id"], index=0, data=b"hello")
    sessions.append_chunk(upload_id=manifest["upload_id"], index=1, data=b" worl")
    done = sessions.append_chunk(upload_id=manifest["upload_id"], index=2, data=b"d")
    final = sessions.complete(upload_id=manifest["upload_id"], final_sha256=hashlib.sha256(b"hello world").hexdigest())

    assert done["received_chunks"] == [0, 1, 2]
    assert final["status"] == "completed"
    assert sessions.source_path(manifest["upload_id"]).read_bytes() == b"hello world"


def test_upload_session_rejects_bad_size_and_missing_chunks(tmp_path):
    sessions = store(tmp_path)
    with pytest.raises(ValueError, match="Empty"):
        sessions.create(filename="messages", size_bytes=0)
    with pytest.raises(ValueError, match="max_upload"):
        sessions.create(filename="messages", size_bytes=21)
    manifest = sessions.create(filename="messages", size_bytes=4, chunk_size_bytes=2)
    sessions.append_chunk(upload_id=manifest["upload_id"], index=0, data=b"ab")
    with pytest.raises(ValueError, match="Missing chunks"):
        sessions.complete(upload_id=manifest["upload_id"])


def test_upload_session_promotes_completed_source_into_shared_artifact_root(tmp_path):
    from logrisk.artifact_storage import SharedArtifactStore

    shared = SharedArtifactStore(tmp_path / "shared")
    sessions = UploadSessionStore(UploadConfig(
        upload_dir=tmp_path / "work" / "uploads",
        max_upload_bytes=20,
        artifact_store=shared,
    ))
    manifest = sessions.create(filename="messages", size_bytes=4, chunk_size_bytes=4)
    sessions.append_chunk(upload_id=manifest["upload_id"], index=0, data=b"safe")

    completed = sessions.complete(upload_id=manifest["upload_id"])

    assert completed["artifact_relative_path"] == f"uploads/{manifest['upload_id']}/source.log"
    assert sessions.source_path(manifest["upload_id"]) == shared.resolve(completed["artifact_relative_path"])
    assert sessions.source_path(manifest["upload_id"]).read_bytes() == b"safe"
