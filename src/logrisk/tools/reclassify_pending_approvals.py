from __future__ import annotations

import argparse
import csv
import json
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from logrisk.approval_dedup import approval_identity


REPORT_FIELDS = (
    "candidate_id",
    "old_problem_code",
    "new_problem_code",
    "old_match_mode",
    "new_match_mode",
    "old_review_key",
    "new_review_key",
    "confidence",
    "semantic_safe",
    "ambiguity",
    "evidence_source",
    "matched_rule",
    "subtype",
    "reason",
)
_CANDIDATE_FIELDS = frozenset({
    "feature_type",
    "title",
    "summary",
    "importance",
    "template_hashes",
    "components",
    "tags",
    "selection_reason",
    "source_templates",
    "template_signatures",
    "problem_code",
    "problemCode",
    "risk_type",
    "cause",
    "risk_semantic",
    "semantic_fields",
    "anchor_signatures",
    "component_scope",
})


def _decode_cell(value: str) -> Any:
    if value == "":
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def load_pending_candidates(path: str | Path) -> list[dict[str, Any]]:
    """Load only allowlisted candidate fields from a queue CSV export."""

    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            candidate = {
                key[len("candidate_"):]: _decode_cell(value)
                for key, value in row.items()
                if key.startswith("candidate_") and key[len("candidate_"):] in _CANDIDATE_FIELDS | {"candidate_id", "status"}
            }
            candidate_id = str(candidate.get("candidate_id") or row.get("candidate_candidate_id") or "").strip()
            status = str(candidate.get("status") or row.get("queue_status") or "pending").strip().lower()
            if not candidate_id or status != "pending" or candidate_id in seen:
                continue
            seen.add(candidate_id)
            candidate["candidate_id"] = candidate_id
            candidate["old_review_key"] = str(row.get("review_key") or "")
            candidate["old_problem_code"] = str(
                candidate.get("problem_code")
                or row.get("group_problem_code")
                or ""
            )
            candidate["old_match_mode"] = str(
                candidate.get("match_mode")
                or row.get("group_match_mode")
                or ""
            )
            candidates.append(candidate)
    return candidates


def _reason(identity: Mapping[str, Any]) -> str:
    if identity["ambiguity"]:
        return "multiple_concrete_semantics"
    if identity["semantic_safe"]:
        return str(identity["matched_rule"] or "deterministic_semantic_match")
    if identity["resolution_source"] == "fallback":
        return "no_reliable_concrete_match"
    return "generic_or_non_concrete_semantics"


def reclassify_candidates(
    candidates: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Resolve pending candidates without mutating storage or historical groups."""

    rows: list[dict[str, Any]] = []
    old_groups: set[str] = set()
    new_groups: set[str] = set()
    seen: set[str] = set()
    semantic_safe_count = 0
    ambiguous_count = 0

    for raw_candidate in candidates:
        if not isinstance(raw_candidate, Mapping):
            continue
        candidate_id = str(raw_candidate.get("candidate_id") or "").strip()
        if not candidate_id or candidate_id in seen:
            continue
        if str(raw_candidate.get("status") or "pending").strip().lower() != "pending":
            continue
        seen.add(candidate_id)
        identity = approval_identity(raw_candidate)
        new_problem_code = str(identity["problem_code"])
        new_match_mode = str(identity["match_mode"])
        new_review_key = (
            f"semantic:{new_problem_code}"
            if identity["semantic_safe"]
            else f"approval:{identity['approval_key']}"
        )
        old_review_key = str(raw_candidate.get("old_review_key") or raw_candidate.get("review_key") or "")
        old_problem_code = str(raw_candidate.get("old_problem_code") or raw_candidate.get("problem_code") or "")
        old_match_mode = str(raw_candidate.get("old_match_mode") or raw_candidate.get("match_mode") or "")
        if old_review_key:
            old_groups.add(old_review_key)
        new_groups.add(new_review_key)
        semantic_safe_count += int(bool(identity["semantic_safe"]))
        ambiguous_count += int(bool(identity["ambiguity"]))
        rows.append({
            "candidate_id": candidate_id,
            "old_problem_code": old_problem_code,
            "new_problem_code": new_problem_code,
            "old_match_mode": old_match_mode,
            "new_match_mode": new_match_mode,
            "old_review_key": old_review_key,
            "new_review_key": new_review_key,
            "confidence": identity["resolution_confidence"],
            "semantic_safe": bool(identity["semantic_safe"]),
            "ambiguity": bool(identity["ambiguity"]),
            "evidence_source": identity["resolution_source"],
            "matched_rule": identity["matched_rule"],
            "subtype": identity["subtype"],
            "reason": _reason(identity),
        })

    candidate_count = len(rows)
    semantic_group_count = sum(key.startswith("semantic:") for key in new_groups)
    fallback_group_count = len(new_groups) - semantic_group_count
    fallback_count = candidate_count - semantic_safe_count
    changed_count = sum(row["old_review_key"] != row["new_review_key"] for row in rows)
    summary = {
        "schema_version": "approval_reclassification_dry_run_v1",
        "candidate_count": candidate_count,
        "old_group_count": len(old_groups),
        "new_group_count": len(new_groups),
        "semantic_group_count": semantic_group_count,
        "fallback_group_count": fallback_group_count,
        "semantic_safe_candidate_count": semantic_safe_count,
        "fallback_candidate_count": fallback_count,
        "ambiguous_candidate_count": ambiguous_count,
        "changed_candidate_count": changed_count,
        "canonical_problem_code_coverage": semantic_safe_count / candidate_count if candidate_count else 0.0,
        "fallback_problem_code_ratio": fallback_count / candidate_count if candidate_count else 0.0,
        "approval_compression_ratio": 1 - len(new_groups) / candidate_count if candidate_count else 0.0,
        "semantic_ambiguity_ratio": ambiguous_count / candidate_count if candidate_count else 0.0,
        "logrisk_approval_candidates_total": candidate_count,
        "logrisk_approval_review_groups": len(new_groups),
        "logrisk_approval_canonical_candidates": semantic_safe_count,
        "logrisk_approval_fallback_candidates": fallback_count,
        "logrisk_approval_ambiguous_candidates": ambiguous_count,
        "logrisk_approval_semantic_safe_candidates": semantic_safe_count,
    }
    return rows, summary


def _write_report(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="对 pending Approval Candidate 做只读语义重分类")
    parser.add_argument("--dry-run", action="store_true", help="只生成审计报告，不修改 Candidate 或 Approval Group")
    parser.add_argument("--input", required=True, type=Path, help="审批队列 CSV 导出")
    parser.add_argument("--output", required=True, type=Path, help="逐 Candidate 重分类 CSV 输出路径")
    parser.add_argument("--summary", required=True, type=Path, help="汇总 JSON 输出路径")
    args = parser.parse_args(argv)
    if not args.dry_run:
        parser.error("必须显式指定 --dry-run；该工具不会执行物理重挂载")
    rows, summary = reclassify_candidates(load_pending_candidates(args.input))
    _write_report(args.output, rows)
    _write_summary(args.summary, summary)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
