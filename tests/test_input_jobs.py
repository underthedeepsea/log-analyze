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
