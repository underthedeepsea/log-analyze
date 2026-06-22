from logrisk.risk_engine import score_risk_entities


def test_risk_score_basic():
    rules = {
        "severity_weight": {"ERROR": 1.2, "UNKNOWN": 0.6},
        "component_weight": {"kernel": 1.5, "unknown": 0.6},
        "template_rules": [{
            "name": "oom",
            "match": {"component": "kernel", "template_regex": ".*out of memory.*"},
            "risk_weight": 95,
            "category": "node_memory_pressure",
            "rca_hint": "oom",
        }],
        "scoring": {"count_norm_divisor": 50, "max_score": 100},
    }
    windows = [{
        "window_start": "2026-06-22T10:00:00+08:00",
        "window_end": "2026-06-22T10:05:00+08:00",
        "cluster": "prod-a",
        "entity_type": "node",
        "entity_id": "node-a",
        "component": "kernel",
        "severity": "ERROR",
        "template_hash": "x",
        "template": "Memory cgroup out of memory",
        "count": 3,
        "affected_pods": [],
    }]
    entities = score_risk_entities(windows, rules)
    assert entities[0]["risk_score"] > 70
