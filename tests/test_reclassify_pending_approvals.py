from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from logrisk.approval_dedup import approval_identity
from logrisk.tools.reclassify_pending_approvals import load_pending_candidates, main, reclassify_candidates


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
    assert by_id["unknown"]["new_problem_code"].startswith("logrisk.unclassified.")
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


CHINESE_FIELDS = [
    "候选ID",
    "候选状态",
    "候选问题码",
    "审批键",
    "审批组ID",
    "特征类型",
    "特征标题",
    "特征摘要",
    "重要性",
    "匹配模式",
    "选择理由",
    "标签",
    "组件",
    "组件范围",
    "模板Hash",
    "锚点签名",
    "评估结果",
]


def _write_chinese_csv(path: Path, rows: list[dict[str, str]], *, fields: list[str] | None = None):
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields or CHINESE_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def _chinese_row(candidate_id: str = "cn-1", *, status: str = "pending", **overrides: str) -> dict[str, str]:
    row = {
        "审批组ID": "physical-group-1",
        "候选ID": candidate_id,
        "候选状态": status,
        "候选问题码": "legacy.problem",
        "审批键": "appr_legacy",
        "特征类型": "runtime_failure",
        "特征标题": "脱敏标题",
        "特征摘要": "脱敏摘要",
        "重要性": "high",
        "匹配模式": "template_set",
        "选择理由": "脱敏理由",
        "标签": json.dumps(["runtime"], ensure_ascii=False),
        "组件": json.dumps(["kubelet"], ensure_ascii=False),
        "组件范围": json.dumps(["kubelet"], ensure_ascii=False),
        "模板Hash": json.dumps(["fixture-oom"], ensure_ascii=False),
        "锚点签名": "",
        "评估结果": json.dumps({"passed": True}, ensure_ascii=False),
    }
    row.update(overrides)
    return row


def test_load_pending_candidates_maps_bom_chinese_headers_and_physical_identity(tmp_path):
    source = tmp_path / "中文-bom.csv"
    _write_chinese_csv(source, [_chinese_row()])

    candidates = load_pending_candidates(source)

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate["candidate_id"] == "cn-1"
    assert candidate["feature_type"] == "runtime_failure"
    assert candidate["template_hashes"] == ["fixture-oom"]
    assert candidate["components"] == ["kubelet"]
    assert candidate["old_problem_code"] == "legacy.problem"
    assert candidate["old_match_mode"] == "template_set"
    assert candidate["old_approval_key"] == "appr_legacy"
    assert candidate["old_approval_group_id"] == "physical-group-1"


@pytest.mark.parametrize(
    "fieldnames",
    [
        [
            "candidate_id",
            "candidate_status",
            "queue_status",
            "candidate_problem_code",
            "group_problem_code",
            "candidate_match_mode",
            "group_match_mode",
        ],
        [
            "candidate_id",
            "group_match_mode",
            "group_problem_code",
            "queue_status",
            "candidate_match_mode",
            "candidate_problem_code",
            "candidate_status",
        ],
    ],
)
def test_loader_candidate_aliases_win_over_group_queue_regardless_of_column_order(
    tmp_path, fieldnames: list[str],
):
    source = tmp_path / "candidate-precedence.csv"
    row = {
        "candidate_id": "candidate-1",
        "candidate_status": "pending",
        "queue_status": "approved",
        "candidate_problem_code": "candidate.problem",
        "group_problem_code": "group.problem",
        "candidate_match_mode": "candidate_mode",
        "group_match_mode": "group_mode",
    }
    with source.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(row)

    candidates = load_pending_candidates(source)

    assert len(candidates) == 1
    assert candidates[0]["status"] == "pending"
    assert candidates[0]["problem_code"] == "candidate.problem"
    assert candidates[0]["old_problem_code"] == "candidate.problem"
    assert candidates[0]["old_match_mode"] == "candidate_mode"


@pytest.mark.parametrize(
    "fieldnames",
    [
        ["候选ID", "候选状态", "审批组状态", "候选问题码", "审批组问题码", "匹配模式", "组匹配模式"],
        ["候选ID", "组匹配模式", "审批组问题码", "审批组状态", "匹配模式", "候选问题码", "候选状态"],
    ],
)
def test_loader_chinese_candidate_aliases_win_over_group_queue_regardless_of_column_order(
    tmp_path, fieldnames: list[str],
):
    source = tmp_path / "candidate-precedence-zh.csv"
    row = {
        "候选ID": "candidate-1",
        "候选状态": "pending",
        "审批组状态": "approved",
        "候选问题码": "candidate.problem",
        "审批组问题码": "group.problem",
        "匹配模式": "candidate_mode",
        "组匹配模式": "group_mode",
    }
    with source.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(row)

    candidates = load_pending_candidates(source)

    assert len(candidates) == 1
    assert candidates[0]["status"] == "pending"
    assert candidates[0]["old_problem_code"] == "candidate.problem"
    assert candidates[0]["old_match_mode"] == "candidate_mode"


def test_loader_uses_group_queue_aliases_only_when_candidate_values_are_blank(tmp_path):
    source = tmp_path / "group-queue-fallback.csv"
    source.write_text(
        "candidate_id,candidate_status,queue_status,candidate_problem_code,group_problem_code,"
        "candidate_match_mode,group_match_mode\n"
        "candidate-1,,pending,,group.problem,,group_mode\n",
        encoding="utf-8",
    )

    candidates = load_pending_candidates(source)

    assert len(candidates) == 1
    assert candidates[0]["status"] == "pending"
    assert candidates[0]["old_problem_code"] == "group.problem"
    assert candidates[0]["old_match_mode"] == "group_mode"


def test_loader_rejects_conflicting_candidate_id_aliases(tmp_path):
    source = tmp_path / "conflicting-ids.csv"
    source.write_text(
        "candidate_id,candidate_candidate_id,candidate_status\nleft,right,pending\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="候选 ID.*冲突"):
        load_pending_candidates(source)


@pytest.mark.parametrize(
    ("left_alias", "right_alias"),
    [
        ("candidate_id", "candidate_candidate_id"),
        ("candidate_id", "候选ID"),
        ("candidate_candidate_id", "候选ID"),
    ],
)
def test_direct_reclassification_rejects_conflicting_candidate_id_aliases(
    left_alias: str, right_alias: str,
):
    with pytest.raises(ValueError, match="候选 ID.*冲突"):
        reclassify_candidates([{
            left_alias: "left",
            right_alias: "right",
            "status": "pending",
        }])


@pytest.mark.parametrize(
    ("candidate_status_alias", "group_status_alias"),
    [
        ("candidate_status", "queue_status"),
        ("候选状态", "queue_status"),
        ("候选状态", "审批组状态"),
    ],
)
def test_direct_reclassification_candidate_status_alias_wins_over_group_status_alias(
    candidate_status_alias: str, group_status_alias: str,
):
    rows, summary = reclassify_candidates([{
        "candidate_id": "candidate-1",
        candidate_status_alias: "pending",
        group_status_alias: "approved",
    }])

    assert [row["candidate_id"] for row in rows] == ["candidate-1"]
    assert summary["pending_row_count"] == 1
    assert summary["skipped_non_pending_count"] == 0


@pytest.mark.parametrize(
    ("name", "contents", "message"),
    [
        ("empty.csv", "", "为空"),
        ("unknown.csv", "not_a_candidate,other\nvalue,value\n", "无法识别"),
        ("missing-id.csv", "候选状态\n pending\n", "候选 ID"),
        ("null-id.csv", "候选ID,候选状态\nnull,pending\n", "候选 ID"),
        ("array-id.csv", "候选ID,候选状态\n[],pending\n", "候选 ID"),
        ("nonempty-array-id.csv", "候选ID,候选状态\n[\"cn-1\"],pending\n", "候选 ID"),
        ("object-id.csv", "候选ID,候选状态\n{},pending\n", "候选 ID"),
        ("nonempty-object-id.csv", "候选ID,候选状态\n{\"id\":\"cn-1\"},pending\n", "候选 ID"),
        ("broken-json.csv", "候选ID,候选状态,模板Hash\ncn-1,pending,[broken\n", "JSON"),
        ("malformed-csv.csv", "candidate_id,candidate_status\n\"cn-1,pending\n", "CSV"),
    ],
)
def test_loader_rejects_invalid_csv_inputs(tmp_path, name, contents, message):
    source = tmp_path / name
    source.write_text(contents, encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_pending_candidates(source)


def test_cli_reports_duplicate_and_non_pending_filters(tmp_path):
    source = tmp_path / "filters.csv"
    _write_chinese_csv(source, [
        _chinese_row("keep"),
        _chinese_row("keep"),
        _chinese_row("approved", status="approved"),
    ])
    original = source.read_bytes()
    output = tmp_path / "report.csv"
    summary = tmp_path / "summary.json"

    assert main([
        "--dry-run",
        "--input", str(source),
        "--output", str(output),
        "--summary", str(summary),
    ]) == 0

    report_summary = json.loads(summary.read_text(encoding="utf-8"))
    assert report_summary["input_row_count"] == 3
    assert report_summary["unique_pending_candidate_count"] == 1
    assert report_summary["skipped_duplicate_count"] == 1
    assert report_summary["skipped_non_pending_count"] == 1
    assert report_summary["candidate_count"] == 1
    assert source.read_bytes() == original


def test_missing_template_bodies_are_undecided_and_do_not_count_as_changed(tmp_path):
    source = tmp_path / "insufficient.csv"
    _write_chinese_csv(source, [_chinese_row()])
    candidates = load_pending_candidates(source)

    rows, summary = reclassify_candidates(candidates)

    row = rows[0]
    assert row["old_problem_code"] == "legacy.problem"
    assert row["old_match_mode"] == "template_set"
    assert row["old_approval_key"] == "appr_legacy"
    assert row["old_approval_group_id"] == "physical-group-1"
    assert row["classification_status"] == "insufficient_evidence"
    assert row["comparison_status"] == "undecided"
    assert row["new_problem_code"] in (None, "")
    assert row["new_review_key"] in (None, "")
    assert row["semantic_safe"] is False
    assert summary["evaluated_count"] == 0
    assert summary["evidence_insufficient_count"] == 1
    assert summary["changed_candidate_count"] == 0
    assert summary["approval_compression_ratio"] is None


def test_complete_template_fixture_is_classified_and_compares_logical_key():
    candidate = _candidate(
        "oom",
        "Out of memory: Killed process <*> ",
        old_key="semantic:linux.memory.oom",
    )

    rows, summary = reclassify_candidates([candidate])

    row = rows[0]
    assert row["classification_status"] == "classified"
    assert row["comparison_status"] == "unchanged"
    assert row["new_review_key"] == "semantic:linux.memory.oom"
    assert summary["evaluated_count"] == 1
    assert summary["evidence_insufficient_count"] == 0
    assert summary["changed_candidate_count"] == 0
    assert summary["input_row_count"] == 1
    assert summary["unique_pending_candidate_count"] == 1


def test_physical_and_logical_keys_are_not_compared_across_namespaces():
    candidate = _candidate(
        "oom",
        "Out of memory: Killed process <*> ",
        old_key="",
    )
    candidate["old_approval_key"] = "appr_legacy"
    candidate["old_approval_group_id"] = "physical-group-1"

    rows, summary = reclassify_candidates([candidate])

    row = rows[0]
    assert row["old_review_key"] == ""
    assert row["old_approval_key"] == "appr_legacy"
    assert row["comparison_status"] == "changed"
    assert row["comparison_basis"] == "physical_approval_key"
    assert row["new_approval_key"].startswith("appr_")
    assert summary["changed_candidate_count"] == 1


def test_old_problem_code_cannot_supply_a_new_classification_without_template_support():
    candidate = _candidate(
        "stale-code",
        "opaque vendor condition",
        old_key="semantic:linux.memory.oom",
    )
    candidate["problem_code"] = "linux.memory.oom"
    candidate["old_problem_code"] = "linux.memory.oom"

    rows, _summary = reclassify_candidates([candidate])

    row = rows[0]
    assert row["semantic_safe"] is False
    assert row["new_problem_code"].startswith("logrisk.unclassified.")


def test_cli_rejects_input_output_and_summary_path_collisions(tmp_path):
    source = tmp_path / "collision-source.csv"
    _write_chinese_csv(source, [_chinese_row()])
    original = source.read_bytes()

    assert main([
        "--dry-run",
        "--input", str(source),
        "--output", str(source.parent / "alias" / ".." / source.name),
        "--summary", str(tmp_path / "collision-summary.json"),
    ]) == 2
    assert source.read_bytes() == original

    output = tmp_path / "collision-output.csv"
    assert main([
        "--dry-run",
        "--input", str(source),
        "--output", str(output),
        "--summary", str(source.parent / "." / source.name),
    ]) == 2
    assert source.read_bytes() == original

    assert main([
        "--dry-run",
        "--input", str(source),
        "--output", str(output),
        "--summary", str(output),
    ]) == 2
    assert not output.exists()
    assert source.read_bytes() == original


@pytest.mark.parametrize(
    ("name", "contents", "message"),
    [
        ("header-only.csv", "candidate_id,candidate_status\n", "数据行"),
        (
            "short-row.csv",
            "candidate_id,candidate_status,candidate_components\nid,pending\n",
            "列数",
        ),
    ],
)
def test_loader_rejects_header_only_and_short_rows(tmp_path, name, contents, message):
    source = tmp_path / name
    source.write_text(contents, encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_pending_candidates(source)


def test_direct_reclassification_summary_counts_duplicate_and_non_pending_rows():
    pending = _candidate("keep", "opaque vendor condition", old_key="old")
    duplicate = dict(pending)
    non_pending = dict(_candidate("done", "opaque vendor condition", old_key="done-old"))
    non_pending["status"] = "approved"

    rows, summary = reclassify_candidates([pending, duplicate, non_pending])

    assert [row["candidate_id"] for row in rows] == ["keep"]
    assert summary["input_row_count"] == 3
    assert summary["pending_row_count"] == 2
    assert summary["unique_pending_candidate_count"] == 1
    assert summary["skipped_duplicate_count"] == 1
    assert summary["skipped_non_pending_count"] == 1
    assert summary["skip_reasons"] == {
        "duplicate_candidate_id": 1,
        "non_pending": 1,
    }


def test_pattern_source_body_is_usable_resolver_evidence():
    candidate = _candidate(
        "pattern-oom",
        "Out of memory: Killed process <*> ",
        old_key="semantic:linux.memory.oom",
    )
    source = candidate["source_templates"][0]
    source["pattern"] = source.pop("template")

    rows, summary = reclassify_candidates([candidate])

    assert rows[0]["classification_status"] == "classified"
    assert rows[0]["new_problem_code"] == "linux.memory.oom"
    assert summary["evaluated_count"] == 1


def test_template_signature_body_uses_resolver_source_precedence():
    candidate = _candidate(
        "signature-oom",
        "Out of memory: Killed process <*> ",
        old_key="semantic:linux.memory.oom",
    )
    source = candidate.pop("source_templates")[0]
    source["pattern"] = source.pop("template")
    candidate["template_signatures"] = [source]

    rows, summary = reclassify_candidates([candidate])

    assert rows[0]["classification_status"] == "classified"
    assert rows[0]["new_problem_code"] == "linux.memory.oom"
    assert summary["evaluated_count"] == 1


def test_selected_source_matches_when_hash_and_fingerprint_both_are_present():
    candidate = _candidate(
        "dual-source-id",
        "Out of memory: Killed process <*> ",
        old_key="semantic:linux.memory.oom",
    )
    candidate["template_hashes"] = ["hash2"]
    source = candidate["source_templates"][0]
    source["template_fingerprint"] = "hash1"
    source["template_hash"] = "hash2"

    rows, summary = reclassify_candidates([candidate])

    assert rows[0]["classification_status"] == "classified"
    assert rows[0]["new_problem_code"] == "linux.memory.oom"
    assert summary["evaluated_count"] == 1


def test_unselected_source_without_body_does_not_block_selected_evidence():
    candidate = _candidate(
        "unselected-source",
        "Out of memory: Killed process <*> ",
        old_key="semantic:linux.memory.oom",
    )
    candidate["template_hashes"] = ["selected"]
    candidate["source_templates"][0]["template_fingerprint"] = "selected"
    candidate["source_templates"].append({
        "template_fingerprint": "unselected",
        "category": "runtime",
    })

    rows, summary = reclassify_candidates([candidate])

    assert rows[0]["classification_status"] == "classified"
    assert rows[0]["new_problem_code"] == "linux.memory.oom"
    assert summary["evaluated_count"] == 1


def test_original_feature_type_is_used_for_template_set_identity():
    candidate = _candidate(
        "cni-identity",
        "CNI network: no enough IPs",
        old_key="",
    )
    candidate["feature_type"] = "cni_ip_exhaustion"
    candidate["evaluator_result"] = {"passed": False}
    candidate["old_approval_key"] = approval_identity(candidate)["approval_key"]

    rows, _summary = reclassify_candidates([candidate])

    assert rows[0]["comparison_status"] == "unchanged"
    assert rows[0]["comparison_basis"] == "physical_approval_key"


def test_rollback_problem_code_is_sanitized_only_in_emitted_report(monkeypatch):
    monkeypatch.setenv("LOGRISK_SEMANTIC_RESOLVER_ENABLED", "0")
    candidate = _candidate("rollback-secret", "opaque vendor condition", old_key="")
    candidate["source_templates"][0]["problem_code"] = "secret.customer_password"

    rows, _summary = reclassify_candidates([candidate])

    emitted_code = rows[0]["new_problem_code"]
    assert emitted_code.startswith("logrisk.unclassified.")
    assert "secret" not in emitted_code
    assert "customer" not in emitted_code
    assert "password" not in emitted_code


def test_unsupported_template_body_is_not_treated_as_resolver_evidence():
    candidate = _candidate("unsupported-body", "Out of memory: Killed process <*> ", old_key="old")
    source = candidate["source_templates"][0]
    source["template_body"] = source.pop("template")

    rows, summary = reclassify_candidates([candidate])

    assert rows[0]["classification_status"] == "insufficient_evidence"
    assert rows[0]["new_problem_code"] in (None, "")
    assert summary["evaluated_count"] == 0
    assert summary["evidence_insufficient_count"] == 1


def test_untrusted_feature_type_cannot_echo_arbitrary_text_in_fallback_identifier():
    candidate = _candidate("unsafe-type", "opaque vendor condition", old_key="old")
    untrusted_type = "MY secret token/ABCD-1234-" + ("x" * 200)
    candidate["feature_type"] = untrusted_type

    rows, _summary = reclassify_candidates([candidate])

    new_problem_code = rows[0]["new_problem_code"]
    assert new_problem_code.startswith("logrisk.unclassified.")
    assert untrusted_type not in new_problem_code
    digest = new_problem_code.rsplit(".", 1)[1]
    assert len(digest) == 16
    assert all(character in "0123456789abcdef" for character in digest)
