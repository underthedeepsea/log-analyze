from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class InputJobConfig:
    output_dir: Path


class InputJobStore:
    def __init__(self, config: InputJobConfig):
        self.config = config
        self.config.output_dir.mkdir(parents=True, exist_ok=True)

    def create(self, *, upload_id: str, filename: str, source_path: str) -> dict[str, Any]:
        input_job_id = "input_job_" + uuid.uuid4().hex
        self.root(input_job_id).mkdir(parents=True, exist_ok=False)
        now = self._now()
        job = {
            "input_job_id": input_job_id,
            "upload_id": upload_id,
            "filename": filename,
            "source_path": source_path,
            "status": "queued",
            "stage": "queued",
            "created_at": now,
            "started_at": None,
            "completed_at": None,
            "error": None,
        }
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

    def _now(self) -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%S%z")
