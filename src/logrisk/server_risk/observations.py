from __future__ import annotations

from contextlib import nullcontext
from datetime import timedelta
from typing import Any

from logrisk.server_risk.contracts import ServerRiskError, TYPED_FIELDS, digest, identifier, integer, json_text, strict_object, typed_fields, utc

BATCH_FIELDS = {"schema_version", "source_id", "source_epoch", "config_revision", "batch_id", "idempotency_key", "mode", "observations"}
OBS_FIELDS = {"identity", "kind", "signal_code", "risk_type", "occurred_at", "interval_start", "interval_end", "source_sequence", "source_position_ref", "count", "typed_fields", "template_ref", "evidence_refs", "coverage"}
IDENTITY_FIELDS = {"cluster", "node_id", "node_uid", "object_kind", "object_id", "boot_id"}
COVERAGE_FIELDS = {"signal_codes", "covered_start", "covered_end", "complete_through", "gaps", "dropped_count", "parse_failure_count", "input_frontier_ref"}


class ObservationService:
    def __init__(self, repository: Any, policies: Any, assessment: Any, clock: Any) -> None:
        self.repository, self.policies, self.assessment, self.clock = repository, policies, assessment, clock

    def submit(self, payload: Any, actor: str | None, *, connection: Any = None, physical_keys: list[str] | None = None, defer_persist: bool = False, context: Any = None) -> dict[str, Any]:
        strict_object(payload, BATCH_FIELDS, BATCH_FIELDS)
        if payload["schema_version"] != "server_risk_observation_batch_v1" or payload["mode"] not in ("historical", "current"):
            raise ServerRiskError("invalid_schema", "观测版本或模式无效")
        for key in ("source_id", "source_epoch", "config_revision", "batch_id", "idempotency_key"):
            identifier(payload[key])
        source = self.policies.source(payload["source_id"])
        if actor not in source["permitted_actors"]:
            raise ServerRiskError("observation_actor_forbidden", "身份不在来源允许名单", 403)
        if payload["source_epoch"] != source["source_epoch"] or payload["config_revision"] != source["config_revision"]:
            raise ServerRiskError("observation_epoch_mismatch", "来源epoch或配置版本不匹配", 409)
        if not isinstance(payload["observations"], list) or not 1 <= len(payload["observations"]) <= 500:
            raise ServerRiskError("observation_batch_limit", "观测批次必须为1至500条")
        now, batch_digest = self.clock(), digest(payload)
        receipt_key = "batch:" + digest([payload["source_id"], payload["idempotency_key"]])
        with nullcontext(connection) if connection is not None else self.repository.database.connect() as conn:
            previous = conn.execute("SELECT input_revision,status FROM server_risk_projection_state WHERE input_namespace=?", (receipt_key,)).fetchone()
        if previous:
            if previous["input_revision"] != batch_digest:
                raise ServerRiskError("observation_idempotency_conflict", "观测幂等键内容不同", 409)
            return {"schema_version": "server_risk_observation_receipt_v1", "batch_id": payload["batch_id"], "idempotent_replay": True, "projection_complete": previous["status"] == "complete"}
        validated = [self.validate(item, payload, source, now) for item in payload["observations"]]
        if physical_keys is not None:
            if len(physical_keys) != len(validated):
                raise ServerRiskError("commit_identity_invalid", "物理贡献与观测数量不一致")
            for item, physical in zip(validated, physical_keys):
                item["source_fact_key"] = "committed:" + physical
                item["observation_id"] = digest([item["source_fact_key"], source["config_revision"]])
        if len({item["source_fact_key"] for item in validated}) != len(validated):
            raise ServerRiskError("observation_duplicate_fact", "同批次来源事实重复")
        subjects = {(item["payload"]["identity"]["cluster"], item["payload"]["identity"]["node_id"]) for item in validated}
        with nullcontext(connection) if connection is not None else self.repository.database.transaction() as conn:
            admitted = conn.execute("INSERT INTO server_risk_projection_state(input_namespace,input_revision,status,updated_at) VALUES (?,?,'pending',?) ON CONFLICT(input_namespace) DO NOTHING", (receipt_key, batch_digest, now))
            lock = " FOR UPDATE" if getattr(self.repository.database, "provider", "sqlite") == "postgres" else ""
            saved = conn.execute("SELECT input_revision,status FROM server_risk_projection_state WHERE input_namespace=?" + lock, (receipt_key,)).fetchone()
            if saved["input_revision"] != batch_digest:
                raise ServerRiskError("observation_idempotency_conflict", "观测幂等键内容不同", 409)
            if admitted.rowcount != 1:
                return {"schema_version": "server_risk_observation_receipt_v1", "batch_id": payload["batch_id"], "idempotent_replay": True, "projection_complete": saved["status"] == "complete"}
            for item in validated:
                old = conn.execute("SELECT payload_digest FROM server_risk_observations WHERE source_fact_key=? AND semantic_revision=?", (item["source_fact_key"], payload["config_revision"])).fetchone()
                if old and old["payload_digest"] != item["payload_digest"]:
                    raise ServerRiskError("observation_fact_conflict", "相同来源事实版本内容不同", 409)
                conn.execute("INSERT INTO server_risk_observations(observation_id,source_fact_key,source_id,source_epoch,config_revision,semantic_revision,subject_key,kind,interval_start,interval_end,received_at,mode,quality,sanitized_payload_json,payload_digest,batch_id,idempotency_key,batch_digest) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(source_fact_key,semantic_revision) DO NOTHING", (item["observation_id"], item["source_fact_key"], payload["source_id"], payload["source_epoch"], payload["config_revision"], payload["config_revision"], item["subject_key"], item["payload"]["kind"], item["start"], item["end"], now, payload["mode"], "observed_interval", json_text(item["payload"]), item["payload_digest"], payload["batch_id"], payload["idempotency_key"], batch_digest))
                actual = conn.execute("SELECT observation_id,payload_digest FROM server_risk_observations WHERE source_fact_key=? AND semantic_revision=?" + lock, (item["source_fact_key"], payload["config_revision"])).fetchone()
                if actual["payload_digest"] != item["payload_digest"]:
                    raise ServerRiskError("observation_fact_conflict", "相同来源事实版本内容不同", 409)
                conn.execute("INSERT INTO server_risk_projection_state(input_namespace,input_revision,last_fact_ref,status,updated_at) VALUES (?,?,?,'complete',?) ON CONFLICT(input_namespace) DO UPDATE SET input_revision=excluded.input_revision,last_fact_ref=excluded.last_fact_ref,updated_at=excluded.updated_at", ("fact:" + item["source_fact_key"], payload["config_revision"], item["observation_id"], now))
            # This frontier proves only this module processed the registered batch.
            conn.execute("UPDATE server_risk_projection_state SET last_fact_ref=?,evaluated_through=?,status='complete',updated_at=? WHERE input_namespace=? AND input_revision=?", (validated[-1]["observation_id"], max(item["end"] for item in validated), now, receipt_key, batch_digest))
            if not defer_persist:
                for cluster, node in subjects:
                    self.assessment.persist(cluster, node, conn, now, context=context)
        return {"schema_version": "server_risk_observation_receipt_v1", "batch_id": payload["batch_id"], "idempotent_replay": False, "accepted_count": len(validated), "projection_complete": True}

    def validate(self, item: Any, batch: dict[str, Any], source: dict[str, Any], now: str) -> dict[str, Any]:
        strict_object(item, OBS_FIELDS, {"identity", "kind", "signal_code", "risk_type", "source_sequence", "source_position_ref", "typed_fields"})
        identity = strict_object(item["identity"], IDENTITY_FIELDS, {"cluster", "node_id", "object_kind", "object_id"})
        for value in identity.values():
            identifier(value)
        if identity["cluster"] != source["cluster"] or identity["node_id"] not in source["node_ids"] or (source.get("objects") and {"object_kind": identity["object_kind"], "object_id": identity["object_id"]} not in source["objects"]):
            raise ServerRiskError("observation_scope_mismatch", "观测身份不在登记来源范围", 403)
        if item["kind"] not in ("adverse", "state_snapshot", "recovery_candidate", "coverage"):
            raise ServerRiskError("invalid_schema", "观测类型无效")
        signal = source["signals"].get(item["signal_code"])
        if not signal or signal.get("kind") != item["kind"] or signal.get("risk_type") != item["risk_type"]:
            raise ServerRiskError("observation_signal_unregistered", "信号与登记风险映射不匹配")
        fields = strict_object(item["typed_fields"], set(TYPED_FIELDS))
        if typed_fields(fields) != fields:
            raise ServerRiskError("observation_fields_invalid", "类型字段只接受白名单标量")
        for key, expected in signal.get("field_types", {}).items():
            value = fields.get(key)
            if expected == "boolean" and not isinstance(value, bool) or expected == "string" and not isinstance(value, str) or expected == "integer" and (isinstance(value, bool) or not isinstance(value, int)):
                raise ServerRiskError("observation_fields_invalid", "登记信号字段类型不匹配")
        integer(item["source_sequence"], 0, 2**53 - 1)
        identifier(item["source_position_ref"])
        if "occurred_at" in item:
            if "interval_start" in item or "interval_end" in item:
                raise ServerRiskError("invalid_schema", "精确时间与区间不能混用")
            start = end = utc(item["occurred_at"])
        else:
            if "interval_start" not in item or "interval_end" not in item:
                raise ServerRiskError("invalid_event_time", "缺少可信事件区间")
            start, end = utc(item["interval_start"]), utc(item["interval_end"])
            if end <= start:
                raise ServerRiskError("invalid_event_time", "事件区间必须左闭右开且非空")
        if end > utc(now) + timedelta(seconds=source.get("max_clock_skew_seconds", 30)):
            raise ServerRiskError("observation_clock_skew", "来源事件时间超出允许时钟偏差")
        if batch["mode"] == "current" and (utc(now) - end).total_seconds() > source["allowed_lateness_seconds"]:
            raise ServerRiskError("observation_lateness", "当前观测超过登记允许迟到范围")
        if item["kind"] == "adverse":
            integer(item.get("count"), 1, 1000000000)
        elif "count" in item:
            raise ServerRiskError("invalid_schema", "仅异常观测允许count")
        if item["kind"] == "state_snapshot" and source["capability"] != "authoritative_state":
            raise ServerRiskError("observation_authority_required", "此来源不能提供权威状态")
        if "template_ref" in item:
            identifier(item["template_ref"])
            with self.repository.database.connect() as conn:
                found = conn.execute("SELECT 1 FROM drain_templates WHERE template_hash=?", (item["template_ref"],)).fetchone()
            if not found:
                raise ServerRiskError("observation_template_unknown", "模板引用未登记")
        refs = item.get("evidence_refs", [])
        if not isinstance(refs, list) or len(refs) > 100 or any(not isinstance(ref, str) or not ref.startswith("observation:") for ref in refs):
            raise ServerRiskError("observation_evidence_ref_invalid", "只接受可解析的脱敏观测引用")
        for ref in refs:
            with self.repository.database.connect() as conn:
                if not conn.execute("SELECT 1 FROM server_risk_observations WHERE observation_id=?", (ref.removeprefix("observation:"),)).fetchone():
                    raise ServerRiskError("observation_evidence_ref_invalid", "观测引用不存在")
        safe = dict(item, identity=dict(identity), typed_fields=dict(fields))
        if item["kind"] == "coverage":
            coverage = strict_object(item.get("coverage"), COVERAGE_FIELDS, COVERAGE_FIELDS)
            if source["capability"] != "complete_event_stream":
                raise ServerRiskError("observation_coverage_authority_required", "此来源不能证明完整事件流覆盖")
            covered_start, covered_end = utc(coverage["covered_start"]), utc(coverage["covered_end"])
            through = utc(coverage["complete_through"])
            if covered_start != start or covered_end != end or not start < through <= end:
                raise ServerRiskError("observation_coverage_invalid", "覆盖区间与完成前沿不一致")
            if coverage["input_frontier_ref"] != "batch:" + batch["batch_id"]:
                raise ServerRiskError("observation_frontier_unverified", "输入前沿不能解析为当前受控批次")
            if not isinstance(coverage["signal_codes"], list) or not coverage["signal_codes"] or any(code not in source["signals"] for code in coverage["signal_codes"]):
                raise ServerRiskError("observation_coverage_invalid", "覆盖信号未登记")
            if not isinstance(coverage["gaps"], list) or len(coverage["gaps"]) > 100:
                raise ServerRiskError("observation_coverage_invalid", "覆盖缺口结构无效")
            for gap in coverage["gaps"]:
                strict_object(gap, {"start", "end"}, {"start", "end"})
                if not start <= utc(gap["start"]) < utc(gap["end"]) <= end:
                    raise ServerRiskError("observation_coverage_invalid", "覆盖缺口越界")
            integer(coverage["dropped_count"], 0, 1000000000)
            integer(coverage["parse_failure_count"], 0, 1000000000)
            safe["coverage"] = dict(coverage)
        elif "coverage" in item:
            raise ServerRiskError("invalid_schema", "非coverage不能声明覆盖")
        subject = digest([source["environment"], identity["cluster"], identity.get("node_uid") or identity["node_id"], item["risk_type"], identity["object_kind"], identity["object_id"]])
        physical = digest([source["source_id"], source["source_epoch"], item["source_sequence"], item["source_position_ref"]])
        return {"observation_id": digest([physical, source["config_revision"]]), "source_fact_key": physical, "subject_key": subject, "start": start.isoformat(), "end": end.isoformat(), "payload": safe, "payload_digest": digest(safe)}
