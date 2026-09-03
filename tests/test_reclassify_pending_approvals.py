from __future__ import annotations

import csv
import json
from pathlib import Path

from logrisk.tools.reclassify_pending_approvals import main, reclassify_candidates


def _candidate(candidate_id: str, template: str, *, old_key: str) -> dict:
    return {
        "candidate_id": candidate_id,
        "status": "pending",
        "feature_type": "runtime_failure",
        "problem_code": "unknown_problem_code",
        "components": ["kubelet"],
        "source_templates": [{
            "template_fingerprint": f"fixture-{candidate_id}",
            "category": "runtime",
            "component": "kubelet",
            "template": template,
        }],
        "old_review_key": old_key,
        "old_match_mode": "template_set",
    }


def test_reclassify_candidates_returns_auditable_semantic_and_fallback_rows():
    rows, summary = reclassify_candidates([
        _candidate("oom", "Out of memory: Killed process <*> ", old_key="old-oom"),
        _candidate("unknown", "opaque vendor condition", old_key="old-unknown"),
    ])

    by_id = {row["candidate_id"]: row for row in rows}
    assert by_id["oom"]["new_problem_code"] == "linux.memory.oom"
    assert by_id["oom"]["new_review_key"] == "semantic:linux.memory.oom"
    assert by_id["oom"]["semantic_safe"] is True
    assert by_id["unknown"]["new_problem_code"].startswith("logrisk.runtime_failure.")
    assert by_id["unknown"]["new_match_mode"] == "template_set"
    assert by_id["unknown"]["semantic_safe"] is False
    assert summary["candidate_count"] == 2
    assert summary["old_group_count"] == 2
    assert summary["new_group_count"] == 2
    assert summary["semantic_group_count"] == 1
    assert summary["fallback_group_count"] == 1
    assert summary["changed_candidate_count"] == 2


def test_reclassify_cli_writes_only_sanitized_audit_outputs(tmp_path):
    source = tmp_path / "pending.csv"
    with source.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=[
            "review_key",
            "candidate_candidate_id",
            "candidate_status",
            "candidate_feature_type",
            "candidate_problem_code",
            "candidate_components",
            "candidate_source_templates",
        ])
        writer.writeheader()
        writer.writerow({
            "review_key": "old-review",
            "candidate_candidate_id": "candidate-a",
            "candidate_status": "pending",
            "candidate_feature_type": "runtime_failure",
            "candidate_problem_code": "unknown_problem_code",
            "candidate_components": json.dumps(["kubelet"]),
            "candidate_source_templates": json.dumps([{
                "template_fingerprint": "fixture-oom",
                "category": "runtime",
                "component": "kubelet",
                "template": "Out of memory: Killed process <*> ",
            }]),
        })

    output = tmp_path / "reclassified.csv"
    summary = tmp_path / "summary.json"
    assert main([
        "--dry-run",
        "--input", str(source),
        "--output", str(output),
        "--summary", str(summary),
    ]) == 0

    with output.open("r", encoding="utf-8", newline="") as handle:
        report = list(csv.DictReader(handle))
    report_summary = json.loads(summary.read_text(encoding="utf-8"))

    assert report[0]["candidate_id"] == "candidate-a"
    assert report[0]["new_problem_code"] == "linux.memory.oom"
    assert report[0]["new_review_key"] == "semantic:linux.memory.oom"
    assert report[0]["subtype"] == ""
    assert report_summary["candidate_count"] == 1
    assert report_summary["semantic_safe_candidate_count"] == 1
    assert report_summary["logrisk_approval_candidates_total"] == 1
    assert report_summary["logrisk_approval_semantic_safe_candidates"] == 1
    assert "raw" not in output.read_text(encoding="utf-8").lower()
