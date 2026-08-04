from logrisk.input_jobs import InputJobConfig, InputJobStore


def test_input_job_store_writes_progress_and_result(tmp_path):
    store = InputJobStore(InputJobConfig(output_dir=tmp_path / "jobs"))

    job = store.create(upload_id="upl_a", filename="messages", source_path="/tmp/messages")
    store.write_progress(job["input_job_id"], {"status": "running", "stage": "reading", "progress": 0.2})
    store.write_result(job["input_job_id"], {"summary": {"total_raw_logs": 1}, "risk_entities": []})

    progress = store.get_progress(job["input_job_id"])
    assert progress["upload_id"] == "upl_a"
    assert progress["stage"] == "reading"
    assert store.get_result(job["input_job_id"])["summary"]["total_raw_logs"] == 1


def test_input_job_persists_semantic_dictionary_snapshot(tmp_path):
    store = InputJobStore(InputJobConfig(output_dir=tmp_path / "jobs"))
    snapshot = {
        "schema_version": "semantic_snapshot_v1",
        "extractor_version": "1.0.0",
        "dictionaries": [{"dictionary_id": "linux", "version": 2, "rules": []}],
        "versions": {"linux": {"version": 2, "content_hash": "abc"}},
    }

    job = store.create(
        upload_id="upload-1",
        filename="messages",
        source_path="/tmp/messages",
        semantic_snapshot=snapshot,
    )

    persisted = store.get_job(job["input_job_id"])
    assert persisted["semantic_dictionary_snapshot"] == snapshot
    assert persisted["semantic_dictionary_versions"]["linux"]["version"] == 2


def test_input_job_persists_shared_source_as_a_relative_artifact_path(tmp_path):
    from logrisk.artifact_storage import SharedArtifactStore

    shared = SharedArtifactStore(tmp_path / "shared")
    staged = shared.stage_bytes("uploads", b"safe")
    artifact = shared.promote(staged, "uploads/messages")
    store = InputJobStore(InputJobConfig(output_dir=tmp_path / "jobs", artifact_store=shared))

    job = store.create(upload_id="upl_a", filename="messages", source_path=str(shared.resolve(artifact.relative_path)))

    assert job["source_path"] == "uploads/messages"
    assert job["source_artifact_path"] == "uploads/messages"
    assert store.resolve_source_path(job) == shared.resolve("uploads/messages")
