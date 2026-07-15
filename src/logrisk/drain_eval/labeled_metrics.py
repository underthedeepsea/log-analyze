from __future__ import annotations

import re
from itertools import combinations
from typing import Any


def _ratio(numerator: int | float, denominator: int | float, empty: float = 1.0) -> float:
    return round(numerator / denominator, 6) if denominator else empty


def _tokens(value: str) -> list[str]:
    return re.findall(r"<[^>]+>|[A-Za-z0-9_]+|[^\s]", value)


def evaluate_labeled(rows: list[dict[str, Any]]) -> dict[str, float]:
    true_positive = false_positive = false_negative = 0
    for left, right in combinations(rows, 2):
        gold_same = left.get("gold_group_id") == right.get("gold_group_id")
        predicted_same = left.get("predicted_group_id") == right.get("predicted_group_id")
        true_positive += int(gold_same and predicted_same)
        false_positive += int(not gold_same and predicted_same)
        false_negative += int(gold_same and not predicted_same)
    precision = _ratio(true_positive, true_positive + false_positive)
    recall = _ratio(true_positive, true_positive + false_negative)
    grouping_f1 = _ratio(2 * precision * recall, precision + recall)

    exact = sum(row.get("gold_template") == row.get("predicted_template") for row in rows)
    token_scores: list[float] = []
    semantic_expected = semantic_found = 0
    protected_expected = protected_found = 0
    for row in rows:
        gold_tokens = _tokens(str(row.get("gold_template") or ""))
        predicted_tokens = _tokens(str(row.get("predicted_template") or ""))
        common = sum(min(gold_tokens.count(token), predicted_tokens.count(token)) for token in set(gold_tokens))
        token_precision = _ratio(common, len(predicted_tokens))
        token_recall = _ratio(common, len(gold_tokens))
        token_scores.append(_ratio(2 * token_precision * token_recall, token_precision + token_recall))
        expected_fields = row.get("semantic_fields") or {}
        predicted_fields = row.get("predicted_semantic_fields") or {}
        semantic_expected += len(expected_fields)
        semantic_found += sum(predicted_fields.get(key) == value for key, value in expected_fields.items())
        template = str(row.get("predicted_template") or "")
        protected = row.get("protected_tokens") or []
        protected_expected += len(protected)
        protected_found += sum(str(token) in template for token in protected)

    return {
        "pairwise_grouping_precision": precision,
        "pairwise_grouping_recall": recall,
        "pairwise_grouping_f1": grouping_f1,
        "exact_template_accuracy": _ratio(exact, len(rows)),
        "token_template_f1": round(sum(token_scores) / len(token_scores), 6) if token_scores else 1.0,
        "over_merge_rate": _ratio(false_positive, true_positive + false_positive, 0.0),
        "over_split_rate": _ratio(false_negative, true_positive + false_negative, 0.0),
        "semantic_field_recall": _ratio(semantic_found, semantic_expected),
        "protected_token_preservation": _ratio(protected_found, protected_expected),
    }
