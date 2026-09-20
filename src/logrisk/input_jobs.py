from __future__ import annotations

import json
import time
import uuid
import base64
import os
import shutil
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
        initial_status: str = "queued",
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
            "status": initial_status,
            "stage": initial_status,
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
        self.write_progress(input_job_id, {
            "input_job_id": input_job_id,
            "status": initial_status,
            "stage": initial_status,
            "progress": 0.0,
        })
        return job

    def root(self, input_job_id: str) -> Path:
        return self.config.output_dir / input_job_id

    def create_recompute(self, input_job_id: str, *, streaming_repository: Any) -> dict[str, Any]:
        """Explicitly create a fresh file run; never turn ordinary resume into replay."""
        import hashlib
        from logrisk.incremental_sources import (
            FileIncrementalSource, IncrementalSourceError, RecomputeSourceIdentityError,
        )
        old = self.get_job(input_job_id)
        task = streaming_repository.get_task(str(old.get("streaming_task_id") or ""))
        if task.get("status") == "running":
            raise ValueError("运行中的任务不能创建重算副本")
        if (task.get("source") or {}).get("kind") != "file":
            raise ValueError("仅允许可验证的 file/upload 来源显式重算；不支持 Kafka 重放")
        source_path = self.resolve_source_path(old)
        expected = task["source"].get("identity") or {}
        source = FileIncrementalSource(
            source_path, filename=old["filename"], immutable_identity=bool(expected.get("identity_digest")),
            environment=expected.get("environment", "production"), scope_key=expected.get("scope_key", "default"),
        )
        source.validate_descriptor(task["source"])
        identity = source.descriptor().identity
        # Recompute needs the original snapshot, not an appended/changed file.
        if any(identity.get(key) != expected.get(key) for key in
               ("size_bytes", "mtime_ns", "head_sha256", "tail_sha256")):
            raise IncrementalSourceError("原始文件快照已变化，不能创建重算副本")
        if expected.get("identity_digest"):
            if source.content_digest() != expected["identity_digest"]:
                raise IncrementalSourceError("原始文件完整摘要已变化，不能创建重算副本")
            verification_method = "historical_content_digest"
        elif int(expected.get("size_bytes") or 0) <= 2 * FileIncrementalSource._FINGERPRINT_BYTES:
            verification_method = "complete_head_tail_coverage"
        else:
            raise RecomputeSourceIdentityError(
                "历史来源仅有首尾指纹且未覆盖全部字节，不能证明仍是原始快照"
            )
        config_path = Path(old.get("drain_config_path") or Path(__file__).parents[2] / "configs" / "drain3_recommended.ini")
        if hashlib.sha256(config_path.read_bytes()).hexdigest() != task["config_hash"]:
            raise ValueError("原始 Drain3 配置已变化，不能创建重算副本")
        job = self.create(
            upload_id=old["upload_id"], filename=old["filename"], source_path=str(source_path),
            semantic_snapshot=old.get("semantic_dictionary_snapshot"), initial_status="building",
        )
        fresh = None
        try:
            verified_path = source_path
            if self.config.artifact_store is None:
                snapshot_dir = self.root(job["input_job_id"]) / "source_snapshot"
                snapshot_dir.mkdir(parents=True, exist_ok=False)
                verified_path = snapshot_dir / Path(old["filename"]).name
                with source_path.open("rb") as source_stream, verified_path.open("xb") as target_stream:
                    shutil.copyfileobj(source_stream, target_stream, length=1024 * 1024)
                    target_stream.flush()
                    os.fsync(target_stream.fileno())
                if source.content_digest() != FileIncrementalSource(verified_path).content_digest():
                    raise IncrementalSourceError("来源在受控快照复制期间发生变化")
                job["source_path"] = str(verified_path)
            verified_source = FileIncrementalSource(
                verified_path,
                filename=old["filename"],
                immutable_identity=True,
                environment=expected.get("environment", "production"),
                scope_key=expected.get("scope_key", "default"),
            )
            verified_descriptor = verified_source.descriptor()
            fresh = streaming_repository.create_or_load(
                descriptor=verified_descriptor, config_hash=task["config_hash"]
            )
            streaming_repository.attach_input_job(fresh["task_id"], job["input_job_id"])
            for key in ("drain_config_id", "drain_config_version", "drain_config_hash", "drain_config_path"):
                if key in old:
                    job[key] = old[key]
            job.update(
                status="queued", stage="queued", streaming_task_id=fresh["task_id"],
                recompute_of=input_job_id, recompute_of_streaming_task=task["task_id"],
                recompute_source_verification=verification_method,
                side_effect_policy="isolated_recompute",
            )
            self.write_job(job["input_job_id"], job)
            self.write_progress(job["input_job_id"], {
                "input_job_id": job["input_job_id"], "status": "queued", "stage": "queued", "progress": 0.0,
            })
            return job
        except BaseException as exc:
            if fresh is not None:
                try:
                    streaming_repository.mark_failed(fresh["task_id"], "重算任务创建未完成")
                except BaseException:
                    pass
            job.update(status="failed", stage="failed", error=str(exc), completed_at=self._now())
            self.write_job(job["input_job_id"], job)
            self.write_progress(job["input_job_id"], {
                "input_job_id": job["input_job_id"], "status": "failed", "stage": "failed",
                "progress": 1.0, "error": str(exc),
            })
            raise

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

    def get_result_page(self,input_job_id: str,*,cursor: str | None=None,limit: int=100,collection: str="entities",window_key: str | None=None) -> dict[str,Any]:
        result=self.get_result(input_job_id)
        reference=result.get("result_ref")
        if not reference:
            return result
        from logrisk.streaming_results import StreamingResultRepository
        repository=StreamingResultRepository(self.database)
        after=""
        if cursor:
            try:
                value=json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
                if value["reference"] != reference or value["input_job_id"] != input_job_id or value.get("collection","entities") != collection or value.get("window_key") != window_key:
                    raise ValueError("cursor mismatch")
                after=value["after"]
                if not isinstance(after,str):
                    raise ValueError("cursor key type")
            except (ValueError,KeyError,TypeError,UnicodeError) as exc:
                raise ValueError("结果游标无效") from exc
        page=(repository.entities(reference,after=after,limit=limit) if collection == "entities" else repository.facts(reference,collection=collection,window_key=window_key,after=after,limit=limit))
        next_cursor=base64.urlsafe_b64encode(json.dumps({"reference":reference,"input_job_id":input_job_id,"after":page["next_key"],"collection":collection,"window_key":window_key}).encode()).decode() if page["next_key"] else None
        if collection != "entities":
            return {"result_ref":reference,"collection":collection,"items":page["items"],"complete":False,"next_cursor":next_cursor}
        return dict(result,risk_entities=page["items"],complete=False,next_cursor=next_cursor)

    def export_complete_result(self,input_job_id: str) -> Path:
        result=self.get_result(input_job_id)
        reference=result.get("result_ref")
        if not reference:
            self._atomic_write(self.result_path(input_job_id),result)
            return self.result_path(input_job_id)
        from logrisk.streaming_results import StreamingResultRepository
        repository=StreamingResultRepository(self.database)
        root=self.root(input_job_id).resolve()
        if root.parent != self.config.output_dir.resolve():
            raise ValueError("结果导出路径无效")
        root.mkdir(parents=True,exist_ok=True)
        target=root / "result_complete.json"
        temporary=root / (".result_complete."+uuid.uuid4().hex+".tmp")
        with temporary.open("x",encoding="utf-8") as stream:
            stream.write('{"summary":'+json.dumps(result.get("summary") or {},ensure_ascii=False)+',"risk_entities":[')
            after=""; first=True
            while True:
                page=repository.entities(reference,after=after)
                for entity in page["items"]:
                    stream.write(("" if first else ",")+json.dumps(entity,ensure_ascii=False)); first=False
                if not page["next_key"]:
                    break
                after=page["next_key"]
            stream.write('],"top_templates":'+json.dumps(repository.top_windows(reference),ensure_ascii=False)+'}')
            stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary,target)
        return target

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
