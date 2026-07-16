from __future__ import annotations

import re
from copy import deepcopy
from typing import Any

from logrisk.semantic.schema import validate_dictionary


class SemanticExtractor:
    MAX_FIELDS = 32

    def __init__(self, snapshot: dict[str, Any]):
        self.extractor_version = str(snapshot.get("extractor_version") or "1.0.0")
        self.dictionaries = [validate_dictionary(item) for item in snapshot.get("dictionaries") or []]
        self.rules = sorted(
            (dict(rule, dictionary_id=item["dictionary_id"]) for item in self.dictionaries for rule in item["rules"]),
            key=lambda item: (-int(item["priority"]), item["rule_id"]),
        )
        self.dictionary_versions = {
            item["dictionary_id"]: {"version": item["version"], "content_hash": item["content_hash"]}
            for item in self.dictionaries
        }

    @classmethod
    def from_snapshot(cls, snapshot: dict[str, Any]) -> "SemanticExtractor":
        return cls(snapshot)

    @staticmethod
    def _applies(rule: dict[str, Any], source_type: str, component: str) -> bool:
        sources = {item.lower() for item in rule["source_types"]}
        components = {item.lower() for item in rule["components"]}
        return (not sources or source_type.lower() in sources) and (not components or component.lower() in components)

    @staticmethod
    def _value(rule: dict[str, Any], text: str) -> Any:
        return int(text) if rule["value_type"] == "integer" else text

    def extract(self, message_core: str, *, source_type: str, component: str) -> dict[str, Any]:
        fields: dict[str, Any] = {}
        tags: list[str] = []
        parameters: list[dict[str, Any]] = []
        matched_rule_ids: list[str] = []
        spans: list[tuple[int, int, str]] = []
        for rule in self.rules:
            if len(fields) >= self.MAX_FIELDS or rule["field"] in fields or not self._applies(rule, source_type, component):
                continue
            match = re.search(rule["pattern"], message_core, re.IGNORECASE)
            if not match:
                continue
            start, end = match.span(rule["group"])
            raw_value = match.group(rule["group"])
            value = self._value(rule, raw_value)
            fields[rule["field"]] = value
            if rule["field"] == "http_status":
                fields["http_status_class"] = f"{int(value) // 100}xx"
            for tag in rule["tags"]:
                if tag not in tags:
                    tags.append(tag)
            typed_mask = f"<{rule['typed_mask']}>"
            parameters.append({
                "field": rule["field"],
                "value": value,
                "typed_mask": typed_mask,
                "rule_id": rule["rule_id"],
                "dictionary_id": rule["dictionary_id"],
            })
            matched_rule_ids.append(rule["rule_id"])
            spans.append((start, end, typed_mask))
        typed_message = message_core
        for start, end, mask in sorted(spans, reverse=True):
            typed_message = typed_message[:start] + mask + typed_message[end:]
        return {
            "semantic_fields": fields,
            "semantic_tags": tags[:16],
            "typed_parameters": parameters,
            "typed_message": typed_message,
            "matched_rule_ids": matched_rule_ids,
            "semantic_extractor_version": self.extractor_version,
            "semantic_dictionary_versions": deepcopy(self.dictionary_versions),
        }

    def enrich(self, record: dict[str, Any]) -> dict[str, Any]:
        result = dict(record)
        result.update(self.extract(
            str(record.get("message_core") or ""),
            source_type=str(record.get("source_type") or "unknown"),
            component=str(record.get("component") or "unknown"),
        ))
        return result
