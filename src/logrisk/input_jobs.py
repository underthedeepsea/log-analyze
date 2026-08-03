from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from logrisk.artifact_storage import SharedArtifactStore


@dataclass(frozen=True)
class InputJobConfig:
    output_dir: Path
    artifact_store: SharedArtifactStore | None = None


class InputJobStore:
    def __init__(self, config: InputJobConfig):
        self.config = config
        self.config.output_dir.mkdir(parents=True, exist_ok=True)

    def create(
        self,
        *,
        upload_id: str,
        filename: str,
        source_path: str,
        drain_config: dict[str, Any] | None = None,
        semantic_snapshot: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        input_job_id = "input_job_" + uuid.uuid4().hex
        self.root(input_job_id).mkdir(parents=True, exist_ok=False)
        now = self._now()
        source_reference = self._source_reference(source_path)
        job = {
            "input_job_id": input_job_id,
            "upload_id": upload_id,
            "filename": filename,
            "source_path": source_reference,
            "status": "queued",
            "stage": "queued",
            "created_at": now,
            "started_at": None,
            "completed_at": None,
            "error": None,
        }
        if self.config.artifact_store:
            job["source_artifact_path"] = source_reference
        if drain_config:
            job.update({
                "drain_config_id": drain_config["config_id"],
                "drain_config_version": drain_config["version"],
                "drain_config_hash": drain_config["content_hash"],
                "drain_config_path": drain_config["path"],
            })
        if semantic_snapshot:
            job["semantic_dictionary_snapshot"] = semantic_snapshot
            job["semantic_dictionary_versions"] = semantic_snapshot.get("versions", {})
        self.write_job(input_job_id, job)
        self.write_progress(input_job_id, {"input_job_id": input_job_id, "status": "queued", "stage": "queued", "progress": 0.0})
        return job

    def root(self, input_job_id: str) -> Path:
        return self.config.output_dir / input_job_id

    def job_path(self, input_job_id: str) -> Path:
        return self.root(input_job_id) / "job.json"

    def progress_path(self, input_job_id: str) -> Path:
        return self.root(input_job_id) / "progress.json"

    def result_path(self, input_job_id: str) -> Path:
        return self.root(input_job_id) / "result.json"

    def get_job(self, input_job_id: str) -> dict[str, Any]:
        return json.loads(self.job_path(input_job_id).read_text(encoding="utf-8"))

    def get_progress(self, input_job_id: str) -> dict[str, Any]:
        job = self.get_job(input_job_id)
        progress = json.loads(self.progress_path(input_job_id).read_text(encoding="utf-8"))
        return {**job, **progress}

    def get_result(self, input_job_id: str) -> dict[str, Any]:
        return json.loads(self.result_path(input_job_id).read_text(encoding="utf-8"))

    def resolve_source_path(self, job: dict[str, Any] | str) -> Path:
        record = self.get_job(job) if isinstance(job, str) else job
        source = str(record.get("source_artifact_path") or record.get("source_path") or "").strip()
        if not source:
            raise FileNotFoundError("输入任务缺少来源文件")
        if self.config.artifact_store:
            path = self.config.artifact_store.resolve(source)
        else:
            path = Path(source)
        if not path.is_file():
            raise FileNotFoundError(path)
        return path

    def write_job(self, input_job_id: str, job: dict[str, Any]) -> None:
        self._atomic_write(self.job_path(input_job_id), job)

    def write_progress(self, input_job_id: str, progress: dict[str, Any]) -> None:
        self._atomic_write(self.progress_path(input_job_id), progress)

    def write_result(self, input_job_id: str, result: dict[str, Any]) -> None:
        self._atomic_write(self.result_path(input_job_id), result)

    def _atomic_write(self, path: Path, data: dict[str, Any]) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)

    def _source_reference(self, source_path: str) -> str:
        if not self.config.artifact_store:
            return str(source_path)
        return self.config.artifact_store.relative_path(source_path)

    def _now(self) -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%S%z")
