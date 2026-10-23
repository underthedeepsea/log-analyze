from __future__ import annotations

from typing import Any

from logrisk.server_risk.contracts import cursor_encode, cursor_offset, digest, identifier, integer, query_contract
from logrisk.server_risk.daily import daily_series, day_evidence
from logrisk.server_risk.repository import ServerRiskRepository
from logrisk.server_risk.reports import ReportService
from logrisk.server_risk.query import resolve_query
from logrisk.server_risk.overview import RiskOverview


class ServerRiskService:
    def __init__(self, repository: ServerRiskRepository, reports: ReportService) -> None:
        self.repository, self.reports = repository, reports
        self.assessments = repository.assessment
        self.observations = None
        self.committed_adapter = self.source_worker = None

    def overview(self, first: str | None = None, last: str | None = None, zone: str = "Asia/Shanghai", **filters: Any) -> dict[str, Any]:
        self.reports.hit("query_validation")
        return RiskOverview(self.repository, self.reports.clock).overview(first, last, zone, **filters)

    def overview_detail(self, cluster: str, node: str, first: str | None = None, last: str | None = None, zone: str = "Asia/Shanghai", **filters: Any) -> dict[str, Any]:
        self.reports.hit("query_validation")
        return RiskOverview(self.repository, self.reports.clock).detail(cluster, node, first, last, zone, **filters)

    def search(self, q: str = "", cluster: str | None = None, cursor: str | None = None, limit: int = 50) -> dict[str, Any]:
        integer(limit, 1, 100)
        nodes = [n for n in self.repository.nodes() if (not cluster or n["cluster"] == cluster) and (not q or q.casefold() in n["node_id"].casefold())]
        scope = {"q": q, "cluster": cluster}
        revision = digest(nodes)
        offset = cursor_offset(cursor, scope, revision)
        latest_times = self.repository.latest_risk_times()
        page_nodes = []
        for node in nodes[offset:offset + limit]:
            latest = latest_times.get((node["cluster"], node["node_id"]))
            page_nodes.append({**node, "latest_risk_at": latest, "evidence_origin": "synthetic" if any(s.get("environment") == "synthetic" and s.get("cluster") == node["cluster"] and node["node_id"] in s.get("node_ids", []) for s in self.assessments.policies.sources.values()) else "historical"})
        return {"schema_version": "server_risk_node_page_v1", "items": page_nodes, "total": len(nodes), "next_cursor": cursor_encode(scope, offset + limit, revision) if offset + limit < len(nodes) else None}

    def day(self, cluster: str, node: str, day: str, zone: str, *, cursor: str | None = None, limit: int = 50) -> dict[str, Any]:
        self.reports.hit("query_validation")
        query = query_contract(cluster, node, day, zone)
        self.repository.require_node(cluster, node)
        self.reports.hit("evidence_snapshot")
        return day_evidence(self.repository, query, cursor=cursor, limit=limit)

    def daily(self, cluster: str, node: str, first: str, last: str, zone: str) -> dict[str, Any]:
        identifier(cluster)
        identifier(node)
        self.repository.require_node(cluster, node)
        return daily_series(self.repository, cluster, node, first, last, zone)

    def assessment(self, cluster: str, node: str) -> dict[str, Any]:
        identifier(cluster)
        identifier(node)
        self.repository.require_node(cluster, node)
        result = self.assessments.evaluate(cluster, node)
        result["latest_risk_at"] = max((fact["last_seen"] for fact in self.repository.facts(cluster, node)), default=None)
        return result

    def query(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = resolve_query(self.repository, payload, self.reports.clock())
        if result["status"] == "resolved":
            q = result["query"]
            page = self.day(q["cluster"], q["node_id"], q["date"], q["timezone"])
            result["answer"] = {"summary": page["summary"], "quality": page["quality"], "evidence_revision": page["evidence_revision"], "total": page["total"]}
        return result
