from __future__ import annotations

import re
from collections import Counter
from datetime import date
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from logrisk.server_risk.contracts import ServerRiskError, day_bounds, decode, digest, identifier, integer, utc
from logrisk.server_risk.daily import covers_day
from logrisk.server_risk.repository import MAX_SCOPE_ROWS

LEVELS = {"high": 3, "medium": 2, "low": 1, "unknown": 0}
RECOVERY_STATES = ("unrecovered", "recovered", "unknown")
SYNTHETIC_ENVIRONMENTS = {"synthetic", "demo", "demonstration", "simulation"}
BUILTIN_EXPLANATION_VERSION = "node_risk_builtin_explanation_v1"
BUILTIN_EXPLANATIONS = {
    "runtime.cni_setup_failed": {
        "description": "CNI 网络配置步骤报告失败。",
        "consequence": "一般可能影响：相关 Pod 的创建或网络连通可能受阻；当前业务影响尚未确认。",
        "recommendation": "核对匹配 Pod 的 CNI 配置与节点网络状态，并补充同对象恢复观测。",
    },
    "k8s.pod.image_pull_backoff": {
        "description": "镜像拉取失败后处于重试退避状态。",
        "consequence": "一般可能影响：镜像拉取重试可能延迟或阻止相关 Pod 启动；当前业务影响尚未确认。",
        "recommendation": "核对镜像地址、仓库访问与认证配置、节点到镜像仓库的网络，并补充同对象恢复观测。",
    },
}


def http_overview_arguments(query: Mapping[str, Any], *, detail: bool = False) -> dict[str, Any]:
    allowed = {"from", "to", "timezone", "level", "recovery"}
    if not detail:
        allowed |= {"q", "page", "page_size"}
    if set(query) - allowed:
        raise ServerRiskError("invalid_schema", "概览查询字段不符合合同")
    values: dict[str, Any] = {}
    for key in query:
        value = query.getlist(key) if hasattr(query, "getlist") else query[key]
        if isinstance(value, (list, tuple)):
            if len(value) != 1:
                raise ServerRiskError("invalid_schema", "概览查询字段不能重复")
            value = value[0]
        if not isinstance(value, str):
            raise ServerRiskError("invalid_schema", "概览查询字段必须为文字")
        values[key] = value
    result = {"first": values.get("from"), "last": values.get("to"), "zone": values.get("timezone", "Asia/Shanghai"), "level": values.get("level", "all"), "recovery": values.get("recovery", "all")}
    if not detail:
        result["q"] = values.get("q", "")
        for key, default in (("page", "1"), ("page_size", "20")):
            value = values.get(key, default)
            if len(value) > 9 or not re.fullmatch(r"[0-9]+", value):
                raise ServerRiskError("invalid_limit", "分页参数必须为正整数")
            result[key] = int(value)
    return result


def normalized_query(first: Any, last: Any, zone: Any, now: str, *, level: str = "all", recovery: str = "all", q: str = "", page: int = 1, page_size: int = 20) -> tuple[dict[str, Any], Any, Any]:
    if zone != "Asia/Shanghai":
        raise ServerRiskError("invalid_date_or_timezone", "概览日期使用 Asia/Shanghai")
    today = utc(now).astimezone(ZoneInfo(zone)).date().isoformat()
    first = first if first is not None else last if last is not None else today
    last = last if last is not None else first
    start, _ = day_bounds(first, zone)
    _, end = day_bounds(last, zone)
    if not 0 <= (date.fromisoformat(last) - date.fromisoformat(first)).days < 90:
        raise ServerRiskError("invalid_date_range", "日期范围必须为1至90日")
    if not isinstance(level, str) or level not in {"all", *LEVELS} or not isinstance(recovery, str) or recovery not in {"all", *RECOVERY_STATES}:
        raise ServerRiskError("invalid_filter", "风险等级或恢复筛选值无效")
    if not isinstance(q, str) or len(q) > 256 or any(ord(char) < 32 for char in q):
        raise ServerRiskError("invalid_query_text", "主机查询最多256字符")
    integer(page, 1, 1000000)
    integer(page_size, 1, 100)
    return {"from": first, "to": last, "timezone": zone, "level": level, "recovery": recovery, "q": q.strip(), "page": page, "page_size": page_size}, start, end


def level_of(value: Any) -> str:
    return "high" if value == "critical" else value if isinstance(value, str) and value in LEVELS else "unknown"


def recovery_counts(groups: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(group["recovery_state"] for group in groups)
    return {state: counts[state] for state in RECOVERY_STATES}


def quality(groups: list[dict[str, Any]]) -> dict[str, Any]:
    reasons = sorted({reason for group in groups for reason in group["quality"]["missing_reasons"]})
    if not groups:
        reasons = ["source_coverage_unregistered"]
    return {"complete": bool(groups) and not reasons, "coverage": "complete" if groups and not reasons else "partial" if groups else "unknown", "missing_reasons": reasons, "source_mode": "real", "bounded": False}


class RiskOverview:
    """Aggregate a scoped immutable fact view without materializing business state."""

    def __init__(self, repository: Any, clock: Any) -> None:
        self.repository, self.clock = repository, clock

    def overview(self, first: str | None = None, last: str | None = None, zone: str = "Asia/Shanghai", **filters: Any) -> dict[str, Any]:
        query, start, end = normalized_query(first, last, zone, self.clock(), **filters)
        hosts, all_groups = [], []
        for node in self.repository.nodes():
            if query["q"] and query["q"].casefold() not in (node["cluster"] + " " + node["node_id"] + " " + node["label"]).casefold():
                continue
            groups = self._groups(node["cluster"], node["node_id"], query, start, end)
            if not groups:
                continue
            all_groups.extend(groups)
            if len(all_groups) > MAX_SCOPE_ROWS:
                raise ServerRiskError("query_resource_limit", "概览聚合超过资源上限")
            complete = all(group["occurrence_count"] is not None for group in groups)
            hosts.append({**node, "highest_level": max((g["level"] for g in groups), key=LEVELS.__getitem__), "risk_count": len(groups), "known_occurrence_count": sum(g["known_occurrence_count"] for g in groups), "occurrence_count": sum(g["occurrence_count"] for g in groups) if complete else None, "first_seen": min((g["first_seen"] for g in groups), key=utc), "last_seen": max((g["last_seen"] for g in groups), key=utc), "recovery_counts": recovery_counts(groups), "risk_types": sorted({g["risk_type"] for g in groups}), "analysis_status": "not_analyzed"})
            hosts[-1]["risk_summaries"] = [{"risk_type": risk_type, "title": title} for risk_type, title in sorted({(group["risk_type"], group["title"]) for group in groups})]
        hosts.sort(key=lambda item: (item["cluster"], item["node_id"]))
        hosts.sort(key=lambda item: utc(item["last_seen"]), reverse=True)
        hosts.sort(key=lambda item: LEVELS[item["highest_level"]], reverse=True)
        total, page_size = len(hosts), query["page_size"]
        summary = {"host_count": total, "high_host_count": sum(host["highest_level"] == "high" for host in hosts), "risk_count": len(all_groups), "known_occurrence_count": sum(g["known_occurrence_count"] for g in all_groups), "unknown_occurrence_groups": sum(g["occurrence_count"] is None for g in all_groups), "recovery_counts": recovery_counts(all_groups)}
        offset = (query["page"] - 1) * page_size
        return {"schema_version": "node_risk_overview_v1", "query": query, "items": hosts[offset:offset + page_size], "summary": summary, "pagination": {"page": query["page"], "page_size": page_size, "total": total, "total_pages": (total + page_size - 1) // page_size}, "revision": digest({"query": {key: value for key, value in query.items() if key not in {"page", "page_size"}}, "hosts": hosts, "groups": all_groups}), "quality": quality(all_groups), "analysis_status": "not_analyzed"}

    def detail(self, cluster: str, node: str, first: str | None = None, last: str | None = None, zone: str = "Asia/Shanghai", *, level: str = "all", recovery: str = "all") -> dict[str, Any]:
        identifier(cluster)
        identifier(node)
        query, start, end = normalized_query(first, last, zone, self.clock(), level=level, recovery=recovery)
        self.repository.require_node(cluster, node)
        groups = self._groups(cluster, node, query, start, end)
        result_query = {key: value for key, value in query.items() if key not in {"q", "page", "page_size"}}
        return {"schema_version": "node_risk_overview_detail_v1", "query": result_query, "host": {"cluster": cluster, "node_id": node, "label": node}, "groups": groups, "revision": digest({"query": result_query, "host": [cluster, node], "groups": groups}), "quality": quality(groups), "analysis_status": "not_analyzed"}

    def _metadata(self, snapshot: Any, cluster: str, node: str) -> dict[str, Any]:
        metadata: dict[str, Any] = {}
        with self.repository.database.connect() as conn:
            events = conn.execute("SELECT event_id,source_job_id,semantic_rule_id,semantic_rule_version FROM node_risk_events WHERE cluster=? AND node_id=? AND derivation_status='current' LIMIT 10001", (cluster, node)).fetchall()
            contributions = conn.execute("SELECT i.source_event_fingerprint,i.event_id,i.source_job_id FROM node_risk_ingestions i JOIN node_risk_events e ON e.event_id=i.event_id WHERE e.cluster=? AND e.node_id=? AND e.derivation_status='current' AND i.is_current=1 LIMIT 10001", (cluster, node)).fetchall()
        if len(events) > MAX_SCOPE_ROWS or len(contributions) > MAX_SCOPE_ROWS:
            raise ServerRiskError("query_resource_limit", "概览来源超过资源上限")
        by_event = {row["event_id"]: dict(row) for row in events}
        for key, value in by_event.items():
            metadata["legacy:" + key] = value
        for row in contributions:
            metadata["legacy:" + row["source_event_fingerprint"]] = {**by_event[row["event_id"]], "source_job_id": row["source_job_id"] or by_event[row["event_id"]]["source_job_id"]}
        for row, window, physical in snapshot.historical_projection(cluster, node)["windows"]:
            ref = "window:" + digest([row["task_id"], row["window_id"], row["item_index"], physical])
            semantic = window.get("risk_semantic") or {}
            metadata[ref] = {"source_job_id": row["task_id"], "semantic_rule_id": semantic.get("semantic_rule_id"), "semantic_rule_version": semantic.get("semantic_rule_version")}
        descriptors = {}
        with self.repository.database.connect() as conn:
            for task_id in {item.get("source_job_id") for item in metadata.values()} - {None, ""}:
                row = conn.execute("SELECT source_identity_json FROM streaming_tasks WHERE task_id=?", (task_id,)).fetchone()
                if row:
                    descriptors[task_id] = decode(row["source_identity_json"])
        for item in metadata.values():
            descriptor = descriptors.get(item.get("source_job_id"), {})
            item["environment"] = (descriptor.get("identity") or {}).get("environment")
        return metadata

    def _groups(self, cluster: str, node: str, query: dict[str, Any], start: Any, end: Any) -> list[dict[str, Any]]:
        snapshot = self.repository.snapshot(cluster, node)
        policies = snapshot.assessment.policies if snapshot.assessment is not None else None
        observations = {row["observation_id"]: row for row in snapshot.observations(cluster, node)}
        metadata = self._metadata(snapshot, cluster, node)
        bindings = [source for source in policies.sources.values() if source.get("cluster") == cluster and node in source.get("node_ids", [])] if policies is not None else []
        synthetic_only = bool(bindings) and all(str(source.get("environment", "")).casefold() in SYNTHETIC_ENVIRONMENTS for source in bindings)
        def is_real(row: dict[str, Any]) -> bool:
            source = policies.sources.get(row["source_id"]) if policies is not None else None
            return bool(source and str(source.get("environment", "")).casefold() not in SYNTHETIC_ENVIRONMENTS)
        safe_observations = {key: row for key, row in observations.items() if is_real(row)}
        grouped: dict[tuple[Any, ...], list[Any]] = {}
        for fact in snapshot.facts(cluster, node):
            if fact.get("cluster") != cluster or fact.get("node_id") != node:
                continue
            info = metadata.get(fact["fact_ref"], {})
            if fact["provenance"] == "registered_observation":
                if fact["fact_ref"] not in safe_observations:
                    continue
            elif str(info.get("environment", "")).casefold() in SYNTHETIC_ENVIRONMENTS or (not info.get("environment") and synthetic_only):
                continue
            try:
                left, right = utc(fact["first_seen"]), utc(fact["last_seen"])
            except ServerRiskError:
                continue
            exclusive = fact["provenance"] == "registered_observation" and left != right
            if right < left or not (left < end and (right > start if exclusive else right >= start)):
                continue
            bounded = left >= start and (right <= end if exclusive else right < end)
            key = (fact["risk_type"], fact["object_kind"], fact["object_id"])
            grouped.setdefault(key, []).append({**fact, "_bounded": bounded})
        assessment = snapshot.assessment.evaluate(cluster, node) if snapshot.assessment is not None else {"checks": []}
        checks = {check["subject_key"]: check for check in assessment["checks"]}
        groups = []
        for key, facts in sorted(grouped.items()):
            risk_type, kind, obj = key
            object_label = "未标识对象" if obj.startswith("legacy:") else obj
            refs = sorted({f["fact_ref"] for f in facts})
            matching_checks = [checks[subject] for subject in {f.get("subject_key") for f in facts} if subject in checks]
            matching_checks = [check for check in matching_checks if check["identity"]["cluster"] == cluster and check["identity"]["node_id"] == node and (check["identity"]["object_kind"], check["identity"]["object_id"]) == (kind, obj) and check["risk_type"] == risk_type]
            states = {check["status"] for check in matching_checks}
            state = "unrecovered" if states and states <= {"present", "recovering"} else "recovered" if states == {"clear"} else "unknown"
            if any(not f.get("subject_key") for f in facts):
                state = "unknown"
            recovery_refs = sorted({ref for check in matching_checks for ref in check["evidence_refs"] if ref in safe_observations and (safe_observations[ref]["payload"]["identity"]["object_kind"], safe_observations[ref]["payload"]["identity"]["object_id"]) == (kind, obj)})
            reasons = []
            if any(not fact["_bounded"] for fact in facts):
                reasons.append("boundary_ambiguous")
            if any(fact["provenance"] == "legacy_unverified" for fact in facts):
                reasons.append("legacy_unverified")
            if not snapshot.historical_projection(cluster, node)["projection_complete"]:
                reasons.append("projection_pending")
            if not self._covered(snapshot, facts, safe_observations, start, end):
                reasons.append("source_coverage_unregistered")
            known = sum(fact["count"] for fact in facts if fact["_bounded"])
            level = max((level_of(fact["severity"]) for fact in facts), key=LEVELS.__getitem__)
            narrative = self._narrative(cluster, node, key, facts, metadata, known)
            groups.append({"group_id": digest([cluster, node, *key]), "risk_type": risk_type, "title": narrative["title"], "level": level, "object": {"kind": kind, "id": obj, "label": obj}, "known_occurrence_count": known, "occurrence_count": known if not reasons else None, "first_seen": min((f["first_seen"] for f in facts), key=utc), "last_seen": max((f["last_seen"] for f in facts), key=utc), "recovery_state": state, "recovery_reason": "当前有效的同对象观测支持风险仍存在。" if state == "unrecovered" else "当前有效的同对象观测已满足登记的恢复条件。" if state == "recovered" else "缺少当前有效且匹配本对象的完整恢复判断。", "description": narrative["description"], "consequence": narrative["consequence"], "recommendation": narrative["recommendation"], "narrative_source": narrative["source"], "evidence_refs": refs, "recovery_evidence_refs": recovery_refs, "association": {"status": "unverified", "reason": "没有可验证的同对象关联证据。", "evidence_refs": []}, "analysis_status": "not_analyzed", "quality": {"complete": not reasons, "missing_reasons": sorted(set(reasons))}})
            groups[-1]["object"]["label"] = object_label
        # Only explicit object equality and overlapping observed intervals support co-occurrence.
        # The relation never asserts a cause and never borrows another object's evidence.
        for group in groups:
            def overlaps(other: dict[str, Any]) -> bool:
                left_facts = grouped[(group["risk_type"], group["object"]["kind"], group["object"]["id"])]
                right_facts = grouped[(other["risk_type"], other["object"]["kind"], other["object"]["id"])]
                for left in left_facts:
                    for right in right_facts:
                        a, b, c, d = utc(left["first_seen"]), utc(left["last_seen"]), utc(right["first_seen"]), utc(right["last_seen"])
                        if a > d or c > b or (b == c and a != b and left["provenance"] == "registered_observation") or (d == a and c != d and right["provenance"] == "registered_observation"):
                            continue
                        return True
                return False
            related = [other for other in groups if other["group_id"] != group["group_id"] and other["object"]["kind"] == group["object"]["kind"] and other["object"]["id"] == group["object"]["id"] and overlaps(other)]
            if related:
                group["association"] = {"status": "confirmed", "reason": "同一主机、同一对象存在时间重叠的异常记录；此关联不代表因果关系。", "evidence_refs": sorted(set(group["evidence_refs"]) | {ref for other in related for ref in other["evidence_refs"]})}
        return [group for group in groups if (query["level"] == "all" or group["level"] == query["level"]) and (query["recovery"] == "all" or group["recovery_state"] == query["recovery"])]

    def _covered(self, snapshot: Any, facts: list[Any], observations: dict[str, Any], start: Any, end: Any) -> bool:
        if snapshot.assessment is None or any(f["fact_ref"] not in observations for f in facts):
            return False
        source_ids = {observations[f["fact_ref"]]["source_id"] for f in facts}
        if len(source_ids) != 1:
            return False
        fact = facts[0]
        fact_rows = [observations[f["fact_ref"]] for f in facts]
        if not all(snapshot.assessment._frontier_complete(row) for row in fact_rows):
            return False
        boundaries = {(row["source_id"], row["source_epoch"], row["config_revision"], row["payload"]["identity"].get("boot_id")) for row in fact_rows}
        parts: dict[tuple[Any, ...], list[Any]] = {}
        for row in observations.values():
            payload = row["payload"]
            identity = payload["identity"]
            if payload["kind"] != "coverage" or row["source_id"] not in source_ids or payload["risk_type"] != fact["risk_type"] or (identity["object_kind"], identity["object_id"]) != (fact["object_kind"], fact["object_id"]):
                continue
            try:
                source = snapshot.assessment.policies.source(row["source_id"])
            except ServerRiskError:
                continue
            coverage = payload["coverage"]
            required = set(snapshot.assessment.policies.rules.get(fact["risk_type"], {}).get("required_signals", []))
            if source["capability"] != "complete_event_stream" or row["source_epoch"] != source["source_epoch"] or row["config_revision"] != source["config_revision"] or not required or not required.issubset(coverage["signal_codes"]) or coverage["gaps"] or coverage["dropped_count"] or coverage["parse_failure_count"] or not snapshot.assessment._frontier_complete(row):
                continue
            parts.setdefault((row["source_id"], row["source_epoch"], row["config_revision"], identity.get("boot_id")), []).append((utc(coverage["covered_start"]), utc(coverage["complete_through"])))
        return all(covers_day(parts.get(boundary, []), start, end) for boundary in boundaries)

    def _narrative(self, cluster: str, node: str, key: tuple[Any, ...], facts: list[Any], metadata: dict[str, Any], known: int) -> dict[str, Any]:
        risk_type, _, obj = key
        obj = "未标识对象" if obj.startswith("legacy:") else obj
        rules = []
        matched = {(metadata.get(f["fact_ref"], {}).get("semantic_rule_id"), metadata.get(f["fact_ref"], {}).get("semantic_rule_version")) for f in facts}
        with self.repository.database.connect() as conn:
            for rule_id, version in sorted(matched, key=str):
                if not rule_id or version is None:
                    continue
                row = conn.execute("SELECT content_json FROM risk_semantic_rule_versions WHERE rule_id=? AND version=?", (rule_id, version)).fetchone()
                if row:
                    rule = decode(row["content_json"])
                    if rule.get("risk_type") == risk_type:
                        rules.append((rule_id, version, rule))
        rule = rules[0][2] if rules else {}
        guide = BUILTIN_EXPLANATIONS.get(risk_type) if any(rule_id == "builtin." + risk_type and matched.get("source") == "builtin" for rule_id, _, matched in rules) else None
        def text(value: Any) -> str:
            return value[:2048] if isinstance(value, str) else ""
        title = text(rule.get("display_name")) or risk_type
        description = f"{cluster} / {node} 的对象 {obj} 记录了{title}，所选范围已知 {known} 次。"
        if guide:
            description += " 规则含义：" + guide["description"]
        elif text(rule.get("description")):
            description += " 规则说明：" + text(rule["description"])
        recommendation = rule.get("recommendation") or {}
        action = text(recommendation.get("text") or recommendation.get("description")) if isinstance(recommendation, dict) else text(recommendation)
        if guide and not action:
            action = ("先从本机异常记录定位匹配 Pod 与异常对象；" if obj == "未标识对象" else "针对对象 " + obj + " 的异常记录，先定位匹配 Pod；") + guide["recommendation"]
        source = {"kind": "builtin_rule_explanation" if guide else "matched_rule" if rules else "scoped_facts", "rule_refs": [{"rule_id": rule_id, "version": version} for rule_id, version, _ in rules], "evidence_refs": sorted(f["fact_ref"] for f in facts)}
        if guide:
            source["explanation_version"] = BUILTIN_EXPLANATION_VERSION
        consequence = text(rule.get("consequence") or rule.get("impact"))
        if not consequence:
            consequence = guide["consequence"] if guide else "已记录该对象的异常，尚无本实体业务影响的确认记录。"
        return {"title": title, "description": description, "consequence": consequence, "recommendation": action or "核对对象 " + obj + " 在该时间范围的状态与恢复观测。", "source": source}
