from __future__ import annotations

import hashlib
import os
import stat
import threading
import time
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable

from logrisk.ai_harness.providers import create_model_client
from logrisk.ai_harness.model_client import ModelClientError
from logrisk.database import utc_now
from logrisk.server_risk.contracts import CLAIMS, LING_MODEL, PROMPT_ID, DeadlineHTTPResponse, ServerRiskError, decode, digest, identifier, json_text, query_contract, strict_object, utc
from logrisk.server_risk.evidence import snapshot, validate_narrative
from logrisk.server_risk.render import RENDERER_VERSION, render_html

NARRATIVE_SCHEMA = {"type": "object", "additionalProperties": False, "required": ["evidence_digest", "blocks"], "properties": {"evidence_digest": {"type": "string"}, "blocks": {"type": "array", "minItems": 1, "maxItems": 32, "items": {"type": "object", "additionalProperties": False, "required": ["claim_code", "fact_refs", "risk_refs"], "properties": {"claim_code": {"type": "string", "enum": list(CLAIMS)}, "fact_refs": {"type": "array", "items": {"type": "string"}}, "risk_refs": {"type": "array", "items": {"type": "string"}}}}}}}


class BoundedProviderResponse:
    def __init__(self, response: Any) -> None:
        self.response = response

    def __enter__(self) -> Any:
        return self

    def __exit__(self, *_args: Any) -> None:
        self.response.close()

    def read(self, _size: int = -1) -> bytes:
        try:
            body = self.response.read(65537)
        except ServerRiskError as exc:
            if exc.code == "http_response_limit":
                raise ServerRiskError("report_output_limit", "模型响应超过资源上限") from None
            raise
        if len(body) > 65536:
            raise ServerRiskError("report_output_limit", "模型响应超过资源上限")
        return body


def create_report_model_client(connection: dict[str, Any], *, transport_initializer: Any = None, transport_hook: Any = None) -> Any:
    client = create_model_client(connection)
    def bounded_open(request: Any, *, timeout: float) -> BoundedProviderResponse:
        response = DeadlineHTTPResponse(request, timeout=timeout, limit=65536, cancelled=getattr(client, "report_transport_cancelled", None), deadline=getattr(client, "report_transport_deadline", None), initializer=transport_initializer, lifecycle_hook=transport_hook)
        if response.code >= 300:
            response.close()
            raise ModelClientError("风险日报模型请求失败") from None
        return BoundedProviderResponse(response)
    client.opener = bounded_open
    return client


class ReportService:
    def __init__(self, repository: Any, profiles: Any, connections: Any, prompts: Any, output_root: Path, *, enabled: bool = False, clock: Callable[[], str] = utc_now, model_factory: Callable[..., Any] = create_report_model_client, stage_hook: Callable[..., None] | None = None, transport_initializer: Any = None, transport_hook: Any = None) -> None:
        self.repository, self.profiles, self.connections, self.prompts = repository, profiles, connections, prompts
        self.root = Path(output_root).resolve() / "node-risk-reports"
        self.enabled, self.clock, self.model_factory = enabled, clock, model_factory
        self.stage_hook = stage_hook
        self.transport_initializer, self.transport_hook = transport_initializer, transport_hook
        self.execution_lock = threading.Lock()

    def artifact_directory(self, report_id: str, *, create: bool = False) -> int:
        """Walk from the filesystem root without following any symlink."""
        path = self.root / report_id
        descriptor = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY)
        current = Path(path.anchor)
        try:
            for component in path.parts[1:]:
                current = current / component
                if create and current in (self.root.parent, self.root, path):
                    try:
                        os.mkdir(component, dir_fd=descriptor)
                    except FileExistsError:
                        pass
                next_descriptor = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
                os.close(descriptor)
                descriptor = next_descriptor
            return descriptor
        except OSError:
            os.close(descriptor)
            raise ServerRiskError("artifact_path_error", "报告路径不合法", 409) from None

    def hit(self, stage: str, report_id: str | None = None) -> None:
        if self.stage_hook:
            self.stage_hook(stage, report_id)

    def profile(self, profile_id: str) -> tuple[Any, dict[str, Any]]:
        self.hit("model_invocation")
        try:
            profile = self.profiles.get(identifier(profile_id))
            connection = self.connections.get(profile.connection_id)
        except (KeyError, ValueError):
            raise ServerRiskError("model_profile_unavailable", "指定 Ling Profile 或连接不可用")
        if profile.model != LING_MODEL or profile.provider != "ollama" or connection["provider"] != "ollama":
            raise ServerRiskError("model_profile_mismatch", "风险日报必须使用明确 Ling 模型")
        if not connection["enabled"]:
            raise ServerRiskError("model_profile_unavailable", "指定 Ling 连接已停用")
        return profile, connection

    def create(self, payload: dict[str, Any], actor_scope: str, *, request_id: str | None = None) -> dict[str, Any]:
        self.hit("query_validation")
        strict_object(payload, {"schema_version", "cluster", "node_id", "date", "timezone", "model_profile_id", "idempotency_key"}, {"schema_version", "cluster", "node_id", "date", "timezone", "model_profile_id", "idempotency_key"})
        if payload["schema_version"] != "node_risk_report_request_v1":
            raise ServerRiskError("invalid_schema", "报告请求版本无效")
        query = query_contract(payload["cluster"], payload["node_id"], payload["date"], payload["timezone"])
        self.repository.require_node(query["cluster"], query["node_id"])
        if not self.enabled:
            raise ServerRiskError("report_disabled", "风险日报未启用", 404)
        key = identifier(payload["idempotency_key"])
        request_digest = digest(payload)
        scope = digest([actor_scope, query["cluster"], query["node_id"]])
        with self.repository.database.connect() as conn:
            old = conn.execute("SELECT report_id,request_digest FROM server_risk_reports WHERE idempotency_scope=? AND idempotency_key=?", (scope, key)).fetchone()
        if old:
            if old["request_digest"] != request_digest:
                raise ServerRiskError("report_idempotency_conflict", "相同幂等键内容不同", 409)
            return self.get(old["report_id"])
        profile, _ = self.profile(payload["model_profile_id"])
        prompt = self.prompts.load(PROMPT_ID)
        self.hit("evidence_snapshot")
        evidence = snapshot(self.repository, query, self.clock())
        now, report_id = self.clock(), uuid.uuid4().hex
        request_id = identifier(request_id or ("request-" + uuid.uuid4().hex))
        deadline = (utc(now) + timedelta(seconds=120)).isoformat()
        self.hit("report_run_persist", report_id)
        with self.repository.database.transaction() as conn:
            conn.execute("INSERT INTO server_risk_reports(report_id,request_id,query_json,idempotency_scope,idempotency_key,request_digest,status,deadline,evidence_snapshot_json,evidence_digest,profile_json,prompt_id,prompt_hash,prompt_content,renderer_version,created_at,updated_at) VALUES (?,?,?,?,?,?,'queued',?,?,?,?,?,?,?,?,?,?) ON CONFLICT(idempotency_scope,idempotency_key) DO NOTHING", (report_id, request_id, json_text(query), scope, key, request_digest, deadline, json_text(evidence), evidence["evidence_digest"], json_text(profile.public_dict()), PROMPT_ID, prompt.sha256, prompt.content, RENDERER_VERSION, now, now))
            saved = conn.execute("SELECT report_id,request_digest FROM server_risk_reports WHERE idempotency_scope=? AND idempotency_key=?", (scope, key)).fetchone()
            if saved["request_digest"] != request_digest:
                raise ServerRiskError("report_idempotency_conflict", "相同幂等键内容不同", 409)
        return self.get(saved["report_id"])

    def get(self, report_id: str) -> dict[str, Any]:
        self.recover_interrupted()
        row = self.repository.report(identifier(report_id))
        return {"schema_version": "node_risk_report_run_v1", "report_id": row["report_id"], "request_id": row["request_id"], "status": row["status"], "cancel_requested": bool(row["cancel_requested"]), "query": decode(row["query_json"]), "profile": {key: decode(row["profile_json"]).get(key) for key in ("profile_id", "model", "provider", "connection_id")}, "evidence_digest": row["evidence_digest"], "artifact": {"url": "/api/node-risk-reports/" + row["report_id"] + "/html", "digest": row["artifact_digest"], "bytes": row["artifact_bytes"]} if row["status"] == "completed" else None, "error": {"code": row["error_code"], "message": "报告执行失败，请查看错误代码"} if row["error_code"] else None, "usage": decode(row["usage_json"]) if row["usage_json"] else None, "created_at": row["created_at"], "updated_at": row["updated_at"]}

    def cancel(self, report_id: str) -> dict[str, Any]:
        self.repository.report(identifier(report_id))
        with self.repository.database.transaction() as conn:
            conn.execute("UPDATE server_risk_reports SET cancel_requested=1,status=CASE WHEN status='queued' THEN 'cancelled' ELSE status END,version=version+1,updated_at=? WHERE report_id=? AND status IN ('queued','running')", (self.clock(), report_id))
        return self.get(report_id)

    def fail(self, report_id: str, code: str) -> None:
        with self.repository.database.transaction() as conn:
            conn.execute("UPDATE server_risk_reports SET status=CASE WHEN cancel_requested=1 THEN 'cancelled' ELSE 'failed' END,error_code=CASE WHEN cancel_requested=1 THEN NULL ELSE ? END,updated_at=?,version=version+1 WHERE report_id=? AND status IN ('queued','running')", (code, self.clock(), report_id))

    def check_cancel(self, report_id: str) -> bool:
        row = self.repository.report(report_id)
        if row["cancel_requested"]:
            self.fail(report_id, "report_cancelled")
            return True
        if utc(self.clock()) >= utc(row["deadline"]):
            raise ServerRiskError("report_deadline_exceeded", "报告超过执行期限")
        return False

    def execute(self, report_id: str) -> None:
        with self.execution_lock:
            claim = uuid.uuid4().hex
            with self.repository.database.transaction() as conn:
                updated = conn.execute("UPDATE server_risk_reports SET status='running',claim=?,version=version+1,updated_at=? WHERE report_id=? AND status='queued' AND cancel_requested=0", (claim, self.clock(), report_id))
                if updated.rowcount != 1:
                    return
            temporary = None
            published = None
            directory_fd = None
            try:
                if self.check_cancel(report_id):
                    return
                row = self.repository.report(report_id)
                execution_deadline = time.monotonic() + max(0, (utc(row["deadline"]) - utc(self.clock())).total_seconds())
                profile, connection = self.profile(decode(row["profile_json"])["profile_id"])
                if digest(profile.public_dict()) != digest(decode(row["profile_json"])):
                    raise ServerRiskError("report_profile_changed", "报告锁定Profile已变化")
                evidence = decode(row["evidence_snapshot_json"])
                model_evidence = decode(json_text(evidence))
                # Explanation remains complete in the locked snapshot and HTML.
                # Model input explicitly counts omitted repetitive references.
                model_evidence["summary"].pop("assessment_explanation", None)
                model_evidence["omitted_explanation_refs"] = 0
                model_evidence["omitted_check_count"] = 0
                for field in ("current_assessment", "historical_assessment"):
                    explanation = model_evidence.get(field, {})
                    checks = explanation.get("checks", [])
                    model_evidence["omitted_check_count"] += max(0, len(checks) - 100)
                    explanation["checks"] = checks[:100]
                    for check in explanation["checks"]:
                        refs = check.get("evidence_refs", [])
                        model_evidence["omitted_explanation_refs"] += max(0, len(refs) - 16)
                        check["evidence_refs"] = refs[:16]
                # Full snapshot and HTML remain complete. The model sees complete
                # totals plus an explicitly bounded set of references.
                ordered = sorted(evidence["facts"] + evidence.get("unfinished_episodes", []), key=lambda f: ({"critical": 0, "high": 1, "medium": 2, "low": 3}.get(f["severity"], 4), f.get("fact_kind") != "unfinished_episode", f["fact_ref"]))
                selected_refs = ordered[:100]
                input_limit = min(65536, (profile.recommended_input_tokens or 21845) * 3)
                while True:
                    model_evidence["facts"] = [f for f in selected_refs if f.get("fact_kind") != "unfinished_episode"]
                    model_evidence["unfinished_episodes"] = [f for f in selected_refs if f.get("fact_kind") == "unfinished_episode"]
                    model_evidence["omitted_fact_count"] = len(ordered) - len(selected_refs)
                    messages = [{"role": "system", "content": row["prompt_content"]}, {"role": "user", "content": json_text(model_evidence)}]
                    if len(json_text(messages).encode()) <= input_limit:
                        break
                    if not selected_refs:
                        raise ServerRiskError("report_input_limit", "完整事实摘要超过模型输入预算")
                    selected_refs = selected_refs[:-1]
                evidence = dict(evidence, omitted_fact_count=model_evidence["omitted_fact_count"])
                client = create_report_model_client(connection, transport_initializer=self.transport_initializer, transport_hook=self.transport_hook) if self.model_factory is create_report_model_client else self.model_factory(connection)
                client.report_transport_deadline = execution_deadline
                client.report_transport_cancelled = lambda: bool(self.repository.report(report_id)["cancel_requested"])
                options = profile.build_model_options()
                options["num_predict"] = min(profile.max_output_tokens or 2048, 2048)
                options.pop("max_output_tokens", None)
                if self.check_cancel(report_id):
                    return
                with self.repository.database.transaction() as conn:
                    called = conn.execute("UPDATE server_risk_reports SET model_calls=1 WHERE report_id=? AND claim=? AND status='running' AND model_calls=0 AND cancel_requested=0", (report_id, claim))
                    if called.rowcount != 1:
                        self.check_cancel(report_id)
                        return
                self.hit("model_invocation", report_id)
                remaining = min((utc(row["deadline"]) - utc(self.clock())).total_seconds(), execution_deadline - time.monotonic())
                if remaining <= 0:
                    raise ServerRiskError("report_deadline_exceeded", "报告超过执行期限")
                narrative = client.generate_json(messages, NARRATIVE_SCHEMA, model=profile.model, timeout=min(float(connection["timeout_seconds"]), 60.0, remaining), options=options)
                if self.check_cancel(report_id):
                    return
                if len(json_text(narrative).encode()) > 32768:
                    raise ServerRiskError("report_output_limit", "报告叙述超过资源上限")
                self.hit("narrative_validation", report_id)
                blocks = validate_narrative(narrative, evidence)
                self.hit("html_render", report_id)
                body = render_html(evidence, blocks, profile.public_dict())
                if len(body) > 5242880:
                    raise ServerRiskError("report_html_limit", "报告HTML超过资源上限")
                if self.check_cancel(report_id):
                    return
                directory_fd = self.artifact_directory(report_id, create=True)
                temporary = claim + ".tmp"
                file_fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory_fd)
                with os.fdopen(file_fd, "wb") as stream:
                    stream.write(body)
                    stream.flush()
                    os.fsync(stream.fileno())
                self.hit("artifact_publish", report_id)
                if self.check_cancel(report_id):
                    return
                try:
                    os.stat("report.html", dir_fd=directory_fd, follow_symlinks=False)
                except FileNotFoundError:
                    pass
                else:
                    raise ServerRiskError("artifact_path_error", "报告成品不可覆盖")
                os.replace(temporary, "report.html", src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
                published = "report.html"
                temporary = None
                body_digest = hashlib.sha256(body).hexdigest()
                metadata = getattr(client, "last_metadata", {})
                usage = metadata.get("usage") if isinstance(metadata, dict) else None
                with self.repository.database.transaction() as conn:
                    completed = conn.execute("UPDATE server_risk_reports SET status='completed',artifact_path=?,artifact_digest=?,artifact_bytes=?,usage_json=?,completed_at=?,updated_at=?,version=version+1 WHERE report_id=? AND claim=? AND status='running' AND cancel_requested=0 AND deadline>?", (report_id + "/report.html", body_digest, len(body), json_text(usage) if usage else None, self.clock(), self.clock(), report_id, claim, self.clock()))
                if completed.rowcount != 1:
                    os.unlink(published, dir_fd=directory_fd)
                    published = None
                    self.check_cancel(report_id)
            except ServerRiskError as exc:
                self.fail(report_id, "report_model_unavailable" if exc.code == "http_deadline_exceeded" else exc.code)
            except (ModelClientError, TimeoutError):
                self.fail(report_id, "report_model_unavailable")
            except Exception:
                self.fail(report_id, "report_internal_error")
            finally:
                if temporary:
                    try:
                        os.unlink(temporary, dir_fd=directory_fd)
                    except FileNotFoundError:
                        pass
                if published and self.repository.report(report_id)["status"] != "completed":
                    try:
                        os.unlink(published, dir_fd=directory_fd)
                    except FileNotFoundError:
                        pass
                if directory_fd is not None:
                    os.close(directory_fd)

    def recover_interrupted(self) -> None:
        """Only expire execution leases; never interrupt another live Worker at startup."""
        with self.repository.database.transaction() as conn:
            conn.execute("UPDATE server_risk_reports SET error_code=CASE WHEN cancel_requested=1 THEN NULL WHEN status='running' THEN 'report_interrupted' ELSE 'report_deadline_exceeded' END,status=CASE WHEN cancel_requested=1 THEN 'cancelled' ELSE 'failed' END,updated_at=?,version=version+1 WHERE status IN ('queued','running') AND deadline<=?", (self.clock(), self.clock()))

    def download(self, report_id: str) -> bytes:
        self.hit("http_download", report_id)
        row = self.repository.report(identifier(report_id))
        if row["status"] != "completed" or not row["artifact_path"]:
            raise ServerRiskError("report_not_completed", "报告尚无可下载成品", 409)
        relative = Path(row["artifact_path"])
        if relative.is_absolute() or relative.parts != (report_id, "report.html"):
            raise ServerRiskError("artifact_path_error", "报告路径不合法", 409)
        directory_fd = self.artifact_directory(report_id)
        try:
            file_fd = os.open("report.html", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory_fd)
            with os.fdopen(file_fd, "rb") as stream:
                if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                    raise ServerRiskError("artifact_path_error", "报告成品不是普通文件", 409)
                body = stream.read(5242881)
        except OSError:
            raise ServerRiskError("artifact_path_error", "报告路径不合法或成品不存在", 409) from None
        finally:
            os.close(directory_fd)
        if len(body) != row["artifact_bytes"] or hashlib.sha256(body).hexdigest() != row["artifact_digest"]:
            raise ServerRiskError("artifact_integrity_error", "报告成品完整性校验失败", 409)
        return body
