from __future__ import annotations

import re
from datetime import timedelta
from typing import Any
from zoneinfo import ZoneInfo

from logrisk.server_risk.contracts import ServerRiskError, identifier, integer, query_contract, strict_object, utc

QUERY_FIELDS = {"text", "cluster", "node_id", "date", "year", "timezone"}


def resolve_query(repository: Any, payload: Any, now: str) -> dict[str, Any]:
    strict_object(payload, QUERY_FIELDS, {"text"})
    text = payload["text"]
    if not isinstance(text, str) or not 1 <= len(text) <= 1024:
        raise ServerRiskError("invalid_query_text", "查询文字必须为1至1024字符")
    zone = payload.get("timezone", "Asia/Shanghai")
    try:
        today = utc(now).astimezone(ZoneInfo(zone)).date()
    except Exception as exc:
        raise ServerRiskError("invalid_date_or_timezone", "IANA时区无效") from exc
    year = integer(payload.get("year", today.year), 1, 9999)
    dates = re.findall(r"(?<![0-9])\d{4}-\d{2}-\d{2}(?![0-9])", text)
    short = re.findall(r"(\d{1,2})月(\d{1,2})日", text)
    dates += [f"{year:04d}-{int(m):02d}-{int(d):02d}" for m, d in short]
    if "今天" in text:
        dates.append(today.isoformat())
    if "昨天" in text:
        dates.append((today - timedelta(days=1)).isoformat())
    dates = sorted(set(dates))
    if not dates and payload.get("date"):
        dates = [payload["date"]]
    nodes = repository.nodes()
    selected = [n for n in nodes if n["node_id"] == payload.get("node_id") and (not payload.get("cluster") or n["cluster"] == payload["cluster"])]
    mentioned = [n for n in nodes if re.search(r"(?<![A-Za-z0-9_.-])" + re.escape(n["node_id"]) + r"(?![A-Za-z0-9_.-])", text) and (not payload.get("cluster") or n["cluster"] == payload["cluster"])]
    aliases = repository.assessment.policies.aliases if repository.assessment is not None else []
    # Chinese names can be immediately followed by a Chinese date expression.
    # ASCII aliases retain identifier boundaries so node-1 never matches node-10.
    alias_matches = [a for a in aliases if re.search((r"(?<![A-Za-z0-9_.-])" + re.escape(a["alias"]) + r"(?![A-Za-z0-9_.-])") if a["alias"].isascii() else re.escape(a["alias"]), text) and (not payload.get("cluster") or a["cluster"] == payload["cluster"])]
    for alias in alias_matches:
        mentioned += [n for n in nodes if n["cluster"] == alias["cluster"] and n["node_id"] == alias["node_id"]]
    mentioned = list({(n["cluster"], n["node_id"]): n for n in mentioned}.values())
    candidates = mentioned or selected
    if len(dates) != 1 or len(candidates) != 1:
        return {"schema_version": "server_risk_query_v1", "status": "needs_clarification", "fields": [key for key, condition in (("date", len(dates) != 1), ("node", len(candidates) != 1)) if condition], "candidates": candidates[:100], "input": text}
    remaining = text
    for alias in alias_matches:
        remaining = remaining.replace(alias["alias"], "")
    for n in candidates:
        remaining = remaining.replace(n["node_id"], "")
    remaining = re.sub(r"\d{4}-\d{2}-\d{2}|\d{1,2}月\d{1,2}日|今天|昨天", "", remaining)
    for phrase in sorted(("这个服务器", "这台服务器", "当前服务器", "这个节点", "这台", "服务器", "节点", "生成报告", "生成日报", "风险日报", "风险报告", "报告", "日报", "风险情况", "风险", "有什么", "有哪些", "是否有", "查看", "查询", "帮我", "请", "的", "在", "当天"), key=len, reverse=True):
        remaining = remaining.replace(phrase, "")
    if re.sub(r"[\s，。？！,:;?!.]", "", remaining):
        return {"schema_version": "server_risk_query_v1", "status": "unsupported_intent", "input": text}
    node = candidates[0]
    query = query_contract(node["cluster"], node["node_id"], dates[0], zone)
    return {"schema_version": "server_risk_query_v1", "status": "resolved", "query": query, "intent": "report" if "报告" in text or "日报" in text else "query", "input": text}
