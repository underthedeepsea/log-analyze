from __future__ import annotations

from logrisk.agentic.tools import build_agent_tool_registry


class Rules:
    def __init__(self):
        self.health_calls = 0

    def list_rules(self, **kwargs):
        self.health_calls += 1
        raise AssertionError("agent lookup must not build UI health")

    def find_active_rules_by_evidence(self, hashes, components, **kwargs):
        assert hashes == {"hash-a", "fp-a"}
        assert components == {"kernel"}
        return [{
            "rule_id": "r1", "title": "GPU", "feature_type": "risk",
            "status": "active", "components": ["kernel"],
            "template_signatures": [{"template_hash": "hash-a", "template_fingerprint": "fp-a"}],
        }]


class Dummy:
    pass


def test_agent_rule_lookup_uses_lightweight_repository_path_and_both_hash_aliases():
    rules = Rules()
    registry = build_agent_tool_registry(Dummy(), rules, Dummy())
    tool = registry.get("find_approved_rules")
    result = tool.handler({"template_hashes": ["hash-a", "fp-a"], "components": ["kernel"]}, Dummy())

    assert result["matched"] == 1
    assert rules.health_calls == 0
