from __future__ import annotations

from logrisk.drain_eval.labeled_metrics import evaluate_labeled
from logrisk.drain_eval.unlabeled_metrics import evaluate_unlabeled


def _rows(predicted: list[str]) -> list[dict[str, object]]:
    gold = ["a", "a", "b", "b"]
    return [
        {
            "record_id": f"log-{index}",
            "gold_group_id": gold[index],
            "gold_template": "error <NUM>",
            "predicted_group_id": predicted[index],
            "predicted_template": "error <NUM>",
            "semantic_fields": {"code": index},
            "predicted_semantic_fields": {"code": index},
            "protected_tokens": ["error"],
        }
        for index in range(4)
    ]


def test_labeled_metrics_distinguish_perfect_over_merge_and_over_split():
    perfect = evaluate_labeled(_rows(["x", "x", "y", "y"]))
    merged = evaluate_labeled(_rows(["x", "x", "x", "x"]))
    split = evaluate_labeled(_rows(["w", "x", "y", "z"]))

    assert perfect["pairwise_grouping_f1"] == 1.0
    assert perfect["over_merge_rate"] == 0.0
    assert perfect["over_split_rate"] == 0.0
    assert perfect["semantic_field_recall"] == 1.0
    assert perfect["protected_token_preservation"] == 1.0
    assert merged["pairwise_grouping_precision"] < 1.0
    assert merged["over_merge_rate"] > 0.0
    assert split["pairwise_grouping_recall"] == 0.0
    assert split["over_split_rate"] == 1.0


def test_unlabeled_metrics_report_singletons_and_wildcards():
    metrics = evaluate_unlabeled([
        {"template": "failed <*> on <NUM>", "count": 1},
        {"template": "failed <*> on <NUM>", "count": 3},
        {"template": "ready", "count": 1},
    ])

    assert metrics["singleton_ratio"] == 0.666667
    assert metrics["wildcard_ratio"] == 0.444444
    assert metrics["cluster_count"] == 3
