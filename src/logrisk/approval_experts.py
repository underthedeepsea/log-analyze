from __future__ import annotations

import json
from typing import Any

from logrisk.database import Database


ROLES = (
    ("evidence_specialist", "证据专家", "证据核对"),
    ("rule_specialist", "规则专家", "规则匹配"),
    ("feature_specialist", "特征专家", "候选校验"),
)


def _object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        decoded = json.loads(value)
        return decoded if isinstance(decoded, dict) else {}
    except (TypeError, ValueError):
        return {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _text(value: Any, limit: int = 2000) -> str:
    return value[:limit] if isinstance(value, str) else ""


def _strings(value: Any) -> str:
    return "、".join(_text(item, 200) for item in _list(value)[:30] if isinstance(item, str))


def _field(label: str, value: Any) -> dict[str, str]:
    return {"label": label, "value": _text(value) or "未记录"}


class ApprovalExpertReader:
    """Read recorded opinions for one candidate, without calling any Provider.

    Dependency runs come from the producing run's immutable snapshot. Current
    workflow nodes may refer to later retries and are never used as substitutes.
    Only presentation fields are returned; runtime/connection snapshots stay private.
    """

    def __init__(self, database: Database) -> None:
        self.database = database

    def read(self, job_id: str, candidate_id: str) -> dict[str, Any] | None:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT entity_id, candidate_json FROM feature_candidates WHERE job_id=? AND candidate_id=?",
                (job_id, candidate_id),
            ).fetchone()
            if row is None:
                return None
            candidate = _object(row["candidate_json"])
            entity_id = str(row["entity_id"])
            run_id = _text(candidate.get("agent_run_id"), 128)
            run = self._run(connection, run_id, job_id, entity_id)
            snapshot = _object(run["locked_snapshot_json"]) if run else {}
            workflow_id = _text(snapshot.get("workflow_run_id"), 128)
            opinions = [self._missing(role, name, kind) for role, name, kind in ROLES]
            if run:
                artifacts = self._artifacts(connection, run_id)
                opinions[2] = self._feature(opinions[2], artifacts, candidate_id, run_id)
                # Check workflow scope before resolving fixed roles for dependency IDs.
                workflow = connection.execute(
                    "SELECT 1 FROM agent_workflow_runs WHERE workflow_run_id=? AND source_job_id=? AND entity_id=?",
                    (workflow_id, job_id, entity_id),
                ).fetchone() if workflow_id else None
                if workflow:
                    nodes = {item["node_id"]: item["role_id"] for item in connection.execute(
                        "SELECT node_id, role_id FROM agent_workflow_nodes WHERE workflow_run_id=?", (workflow_id,)
                    )}
                    for ref in _list(snapshot.get("dependency_artifact_refs"))[:3]:
                        if not isinstance(ref, dict):
                            continue
                        role = nodes.get(_text(ref.get("node_id")))
                        index = {"evidence_specialist": 0, "rule_specialist": 1}.get(role)
                        dependency_id = _text(ref.get("child_agent_run_id"), 128)
                        if index is None or not self._run(connection, dependency_id, job_id, entity_id):
                            continue
                        records = self._assessments(connection, dependency_id, job_id, entity_id)
                        reader = self._evidence if index == 0 else self._rules
                        opinions[index] = reader(opinions[index], records, dependency_id)
            available = sum(item["state"] != "missing" for item in opinions)
            return {
                "schema_version": "approval_expert_opinions_v1", "job_id": job_id,
                "candidate_id": candidate_id, "entity_id": entity_id,
                "agent_run_id": run_id if run else None,
                "workflow_run_id": workflow_id if run else None,
                "model": _text(candidate.get("model"), 200), "opinions": opinions,
                "overview": ("已读取三个专家的对应记录，请结合证据确认后审批。" if available == 3
                             else f"已读取 {available} / 3 位专家的记录；缺失记录不代表通过。"),
            }

    @staticmethod
    def _missing(role: str, name: str, kind: str) -> dict[str, Any]:
        return {"role_id": role, "name": name, "kind": kind, "state": "missing",
                "conclusion": "未记录该专家意见", "basis": "此候选没有可关联的专家记录。",
                "confirmation": "请结合下方候选证据独立判断。", "source": "无对应记录", "records": []}

    @staticmethod
    def _run(connection: Any, run_id: str, job_id: str, entity_id: str) -> Any:
        if not run_id:
            return None
        return connection.execute(
            "SELECT locked_snapshot_json FROM agent_runs WHERE run_id=? AND source_job_id=? AND entity_id=?",
            (run_id, job_id, entity_id),
        ).fetchone()

    @staticmethod
    def _artifacts(connection: Any, run_id: str) -> list[dict[str, Any]]:
        return [{**dict(row), "payload": _object(row["payload_json"])} for row in connection.execute(
            "SELECT artifact_id, artifact_type, payload_json, fingerprint, created_at FROM agent_artifacts "
            "WHERE run_id=? ORDER BY created_at, artifact_id LIMIT 100", (run_id,)
        )]

    def _assessments(self, connection: Any, run_id: str, job_id: str, entity_id: str) -> list[dict[str, Any]]:
        items = []
        for artifact in self._artifacts(connection, run_id):
            payload = artifact["payload"]
            if payload.get("producer_run_id") != run_id:
                continue
            refs = _list(payload.get("evidence_refs"))
            ref = next((item for item in refs if isinstance(item, dict)
                        and item.get("source_job_id") == job_id and item.get("entity_id") == entity_id), None)
            if ref is None:
                continue
            output = _object(payload.get("safe_payload"))
            if payload.get("requires_explicit_read") is True:
                call = connection.execute(
                    "SELECT result_summary_json FROM agent_tool_calls WHERE run_id=? AND tool_call_id=? "
                    "AND tool_name=? AND status='completed'",
                    (run_id, _text(ref.get("tool_call_id")), _text(payload.get("source_tool"))),
                ).fetchone()
                output = _object(call["result_summary_json"]) if call else {}
            items.append({**artifact, "output": output})
        return items

    @staticmethod
    def _record(artifact: dict[str, Any], run_id: str, title: str, fields: list[dict[str, str]]) -> dict[str, Any]:
        return {"artifact_id": artifact["artifact_id"], "run_id": run_id,
                "created_at": str(artifact["created_at"]), "title": title, "fields": fields}

    def _evidence(self, card: dict[str, Any], artifacts: list[dict[str, Any]], run_id: str) -> dict[str, Any]:
        item = next((a for a in reversed(artifacts) if a["artifact_type"] == "evidence_assessment_v1"
                     and a["payload"].get("source_tool") == "get_sanitized_evidence"), None)
        if not item or not isinstance(item["output"].get("templates"), list):
            return card
        templates = [t for t in item["output"]["templates"] if isinstance(t, dict)]
        counts = [t.get("count") for t in templates]
        count = str(sum(counts)) if all(type(n) is int and n >= 0 for n in counts) else "未记录"
        components = _strings(sorted({_text(t.get("component"), 100) for t in templates if t.get("component")}))
        severities = _strings(sorted({_text(t.get("severity"), 40) for t in templates if t.get("severity")}))
        window = _object(item["output"].get("window"))
        fields = [_field("时间范围", f"{_text(window.get('start')) or '未记录'} — {_text(window.get('end')) or '未记录'}")]
        fields.extend(_field(f"模板 {index + 1}", f"{_text(t.get('component'))} · {_text(t.get('severity'))} · "
                             f"{t.get('count') if type(t.get('count')) is int else '未知'} 次\n"
                             f"{_text(t.get('template'))}\nHash {_text(t.get('template_hash'))}")
                      for index, t in enumerate(templates[:50]))
        return {**card, "state": "recorded", "conclusion": f"已核对 {len(templates)} 个脱敏证据模板",
                "basis": f"组件：{components or '未记录'}；聚合日志命中 {count} 次。记录级别：{severities or '未记录'}。"
                         "各模板内容和时间范围可在对应记录中查看。",
                "confirmation": "确认这些模板描述的异常与候选标题、摘要一致，且适用范围合适。",
                "source": "聚合脱敏证据", "records": [self._record(item, run_id, "证据读取记录", fields)]}

    def _rules(self, card: dict[str, Any], artifacts: list[dict[str, Any]], run_id: str) -> dict[str, Any]:
        item = next((a for a in reversed(artifacts) if a["artifact_type"] == "rule_match_assessment_v1"
                     and a["payload"].get("source_tool") == "find_approved_rules"), None)
        if not item or type(item["output"].get("matched")) is not int or item["output"]["matched"] < 0:
            return card
        output = item["output"]
        matched = output["matched"]
        partial = output.get("truncated") is not False
        rules = [r for r in _list(output.get("items")) if isinstance(r, dict)]
        assets = next((a for a in reversed(artifacts) if a["payload"].get("source_tool") == "inspect_knowledge_assets"), None)
        basis = f"本次查询匹配 {matched} 条批准规则。"
        if partial:
            basis += "查询范围未完整覆盖，不能据此认定没有可复用规则。"
        if assets and isinstance(assets["output"].get("items"), list):
            basis += f"另读取 {len(assets['output']['items'])} 项已物化知识资产。"
        fields = [_field("匹配数量", str(matched)), _field("查询完整性", "部分结果" if partial else "完整")]
        fields.extend(_field("匹配规则", _text(r.get("rule_id")) + " · " + _text(r.get("title"))) for r in rules[:30])
        return {**card, "state": "attention" if partial or matched else "recorded",
                "conclusion": ("规则查询结果不完整" if partial else f"找到 {matched} 条可核对规则" if matched else "本次未匹配到批准规则"),
                "basis": basis, "confirmation": ("核对已有规则是否可复用，避免重复登记。" if matched or partial
                                                   else "确认新规则的组件、模板范围及重要性。"),
                "source": "批准规则查询与知识资产记录",
                "records": [self._record(item, run_id, "规则查询记录", fields)] +
                           ([self._record(assets, run_id, "知识资产查询", [_field("已物化资产数量", str(len(assets['output']['items'])))])]
                            if assets and isinstance(assets["output"].get("items"), list) else [])}

    def _feature(self, card: dict[str, Any], artifacts: list[dict[str, Any]], candidate_id: str, run_id: str) -> dict[str, Any]:
        registered = next((a for a in artifacts if a["artifact_type"] == "candidate"
                           and a["payload"].get("candidate_id") == candidate_id), None)
        if not registered:
            return card
        fingerprint = registered.get("fingerprint")
        evaluation = next((a for a in reversed(artifacts) if a["artifact_type"] == "evaluation"
                           and fingerprint and a.get("fingerprint") == fingerprint
                           and a["created_at"] <= registered["created_at"]), None)
        feature = registered["payload"]
        result = evaluation["payload"] if evaluation else {}
        passed = result.get("passed")
        resolution = _object(feature.get("problem_resolution"))
        semantic_safe = resolution.get("semantic_safe") is True and resolution.get("ambiguity") is not True
        conclusion = ("候选已通过结构校验" if passed is True else "候选校验未通过" if passed is False else "候选已登记，校验记录不完整")
        diagnostics = _strings(result.get("errors")) or _strings(result.get("warnings"))
        basis = ("模型给出的理由：" + _text(feature.get("selection_reason"), 180) if feature.get("selection_reason")
                 else _text(feature.get("summary"), 180) or "未记录候选摘要。")
        if diagnostics:
            basis += " 校验提示：" + diagnostics[:180]
        fields = [_field("登记标题", feature.get("title")), _field("登记摘要", feature.get("summary")),
                  _field("模型选择理由", feature.get("selection_reason")), _field("组件", _strings(feature.get("components"))),
                  _field("模板 Hash", _strings(feature.get("template_hashes"))),
                  _field("语义证据", "已记录为完整" if semantic_safe else "需人工核对")]
        records = [self._record(registered, run_id, "候选登记记录", fields)]
        if evaluation:
            checks = [_field("校验结果", "通过" if passed is True else "未通过" if passed is False else "未记录")]
            checks.extend(_field(_text(r.get("rule_name")) or _text(r.get("rule_id")),
                                 ("通过" if r.get("status") == "passed" else "未通过" if r.get("status") == "failed" else "未记录") + " " + _text(r.get("detail")))
                          for r in _list(result.get("rule_results"))[:30] if isinstance(r, dict))
            records.append(self._record(evaluation, run_id, "登记前确定性校验", checks))
        return {**card, "state": "recorded" if passed is True and semantic_safe else "attention",
                "conclusion": conclusion, "basis": basis,
                "confirmation": ("校验通过后仍需确认描述、重要性及规则适用范围，再作审批决定。" if passed is True and semantic_safe
                                 else "先核对校验提示与语义证据完整性，再作审批决定。"),
                "source": "模型候选与确定性校验", "records": records}
