from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from logrisk.semantic.extractor import SemanticExtractor
from logrisk.semantic.schema import SemanticValidationError, validate_dictionary


DICTIONARY_ROOT = Path("configs/semantic_dictionary")


def extractor() -> SemanticExtractor:
    dictionaries = []
    for path in sorted(DICTIONARY_ROOT.glob("*.yaml")):
        dictionaries.append(validate_dictionary(yaml.safe_load(path.read_text(encoding="utf-8"))))
    return SemanticExtractor.from_snapshot({
        "schema_version": "semantic_snapshot_v1",
        "extractor_version": "1.0.0",
        "dictionaries": dictionaries,
    })


@pytest.mark.parametrize(("message", "component", "field", "value", "mask"), [
    ("HTTP request failed with status 503", "nginx", "http_status", 503, "<HTTP_STATUS>"),
    ("open failed errno=28", "kernel", "errno", 28, "<ERRNO>"),
    ("container exited with code 137", "containerd", "exit_code", 137, "<EXIT_CODE>"),
    ("terminated by signal 9", "containerd", "signal", 9, "<SIGNAL>"),
    ("NVRM: Xid 79, GPU has fallen off the bus", "kernel", "xid_code", 79, "<XID_CODE>"),
    ("pod status changed Reason=Evicted", "kubelet", "k8s_reason", "Evicted", "<K8S_REASON>"),
])
def test_extracts_typed_semantic_fields(message, component, field, value, mask):
    result = extractor().extract(message, source_type="system", component=component)

    assert result["semantic_fields"][field] == value
    assert any(item["typed_mask"] == mask for item in result["typed_parameters"])
    assert result["matched_rule_ids"]


def test_enrich_keeps_message_and_adds_versioned_semantics():
    record = {"message_core": "HTTP status 500", "source_type": "access", "component": "nginx"}

    enriched = extractor().enrich(record)

    assert enriched["message_core"] == record["message_core"]
    assert enriched["semantic_fields"] == {"http_status": 500, "http_status_class": "5xx"}
    assert enriched["semantic_extractor_version"] == "1.0.0"
    assert enriched["semantic_dictionary_versions"]


def test_normal_info_message_has_no_semantic_signal():
    result = extractor().extract("driver registered successfully", source_type="syslog", component="kernel")

    assert result["semantic_fields"] == {}
    assert result["semantic_tags"] == []
    assert result["typed_parameters"] == []


@pytest.mark.parametrize("change, error", [
    ({"field": "root_cause"}, "field"),
    ({"pattern": "["}, "正则"),
    ({"group": "missing"}, "命名组"),
])
def test_rejects_invalid_rule_changes(change, error):
    payload = {
        "schema_version": "semantic_dictionary_v1",
        "dictionary_id": "test",
        "version": 1,
        "rules": [{
            "rule_id": "test-http",
            "field": "http_status",
            "pattern": r"status (?P<value>\d{3})",
            "group": "value",
            "value_type": "integer",
            "typed_mask": "HTTP_STATUS",
            "tags": ["HTTP", "状态码"],
            "priority": 100,
            "source_types": [],
            "components": [],
        }],
    }
    payload["rules"][0].update(change)

    with pytest.raises(SemanticValidationError, match=error):
        validate_dictionary(payload)


def test_rejects_duplicate_ids_and_same_priority_field_conflict():
    rule = {
        "rule_id": "duplicate",
        "field": "errno",
        "pattern": r"errno=(?P<value>\d+)",
        "group": "value",
        "value_type": "integer",
        "typed_mask": "ERRNO",
        "tags": ["系统错误"],
        "priority": 100,
        "source_types": [],
        "components": [],
    }
    payload = {
        "schema_version": "semantic_dictionary_v1",
        "dictionary_id": "test",
        "version": 1,
        "rules": [rule, dict(rule)],
    }

    with pytest.raises(SemanticValidationError, match="rule_id"):
        validate_dictionary(payload)

    payload["rules"][1] = dict(rule, rule_id="other")
    with pytest.raises(SemanticValidationError, match="优先级冲突"):
        validate_dictionary(payload)
