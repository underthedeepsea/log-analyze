from __future__ import annotations

import pytest
from logrisk.server_risk.contracts import ServerRiskError, tool_arguments
from tests.server_risk_fixtures import batch, domain_services, observation


def test_finite_query_dates_aliases_and_ambiguity(tmp_path):
    service, now, config, policies, _ = domain_services(tmp_path)
    service.observations.submit(batch("node", observation(1, now["value"])), "fixture-actor")
    policies.aliases = [{"alias": "计算服务器", "cluster": "prod-a", "node_id": "node-01"}]
    result = service.query({"text": "查询计算服务器8月31日风险", "year": 2026, "timezone": "Asia/Shanghai"})
    assert result["status"] == "resolved" and result["query"]["node_id"] == "node-01" and result["query"]["date"] == "2026-08-31"
    assert service.query({"text": "今天这个服务器风险", "cluster": "prod-a", "node_id": "node-01"})["query"]["date"] == "2026-08-31"
    assert service.query({"text": "请修复服务器", "cluster": "prod-a", "node_id": "node-01", "date": "2026-08-31"})["status"] == "unsupported_intent"
    assert service.query({"text": "8月31日风险", "year": 2026})["status"] == "needs_clarification"
    policies.aliases += [{"alias": "计算服务器", "cluster": "prod-b", "node_id": "node-01"}]
    original = service.repository.nodes
    service.repository.nodes = lambda: original() + [{"cluster": "prod-b", "node_id": "node-01", "label": "node-01"}]
    assert service.query({"text": "查询计算服务器8月31日风险", "year": 2026})["status"] == "needs_clarification"
    assert service.query({"text": "node-01 2026-08-31 风险"})["status"] == "needs_clarification"


def test_tool_fields_cannot_add_sql_paths_identity_or_urls():
    for extra in ("sql", "path", "actor", "roles", "api_base"):
        with pytest.raises(ServerRiskError):
            tool_arguments("logrisk_get_report", {"report_id": "a", extra: "anything"})
    with pytest.raises(ServerRiskError):
        tool_arguments("logrisk_daily_series", {"cluster": "c", "node_id": "n", "from": "2026-01-01", "to": "2026-06-01", "timezone": "Asia/Shanghai"})


def test_explicit_iso_date_adjacent_chinese_wins_and_invalid_never_falls_back(tmp_path):
    service, now, *_ = domain_services(tmp_path)
    service.observations.submit(batch("node", observation(1, now["value"])), "fixture-actor")
    selected = {"cluster": "prod-a", "node_id": "node-01", "date": "2026-08-31", "timezone": "Asia/Shanghai"}
    result = service.query(dict(selected, text="2026-09-01这个服务器有什么风险"))
    assert result["query"]["date"] == "2026-09-01"
    with pytest.raises(ServerRiskError):
        service.query(dict(selected, text="2026-02-30这个服务器有什么风险"))
    assert service.query(dict(selected, text="2026-09-01和2026-09-02服务器风险"))["status"] == "needs_clarification"
