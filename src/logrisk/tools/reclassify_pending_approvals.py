from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from logrisk.approval_dedup import approval_identity
from logrisk.problem_resolver import concrete_problem_codes, selected_evidence_sources


class InputError(ValueError):
    """Raised when an audit CSV cannot be interpreted safely."""


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
    "old_approval_key",
    "old_approval_group_id",
    "new_approval_key",
    "classification_status",
    "evidence_status",
    "comparison_status",
    "comparison_basis",
)

_HEADER_TO_FIELD = {
    "candidate_id": "candidate_id",
    "candidate_candidate_id": "candidate_id",
    "candidate_status": "candidate_status",
    "candidate_feature_type": "feature_type",
    "candidate_title": "title",
    "candidate_summary": "summary",
    "candidate_importance": "importance",
    "candidate_template_hashes": "template_hashes",
    "candidate_components": "components",
    "candidate_tags": "tags",
    "candidate_selection_reason": "selection_reason",
    "candidate_source_templates": "source_templates",
    "candidate_template_signatures": "template_signatures",
    "candidate_problem_code": "candidate_problem_code",
    "candidate_problemCode": "candidate_problem_code",
    "candidate_match_mode": "candidate_match_mode",
    "candidate_risk_type": "risk_type",
    "candidate_cause": "cause",
    "candidate_risk_semantic": "risk_semantic",
    "candidate_semantic_fields": "semantic_fields",
    "candidate_anchor_signatures": "anchor_signatures",
    "candidate_component_scope": "component_scope",
    "candidate_evaluator_result": "evaluator_result",
    "status": "queue_status",
    "queue_status": "queue_status",
    "source_templates": "source_templates",
    "review_key": "old_review_key",
    "old_review_key": "old_review_key",
    "approval_key": "old_approval_key",
    "old_approval_key": "old_approval_key",
    "approval_group_id": "old_approval_group_id",
    "old_approval_group_id": "old_approval_group_id",
    "group_problem_code": "group_problem_code",
    "old_problem_code": "group_problem_code",
    "group_match_mode": "group_match_mode",
    "old_match_mode": "group_match_mode",
    "候选ID": "candidate_id",
    "候选状态": "candidate_status",
    "特征类型": "feature_type",
    "特征标题": "title",
    "特征摘要": "summary",
    "重要性": "importance",
    "模板Hash": "template_hashes",
    "组件": "components",
    "组件范围": "component_scope",
    "标签": "tags",
    "选择理由": "selection_reason",
    "候选问题码": "candidate_problem_code",
    "匹配模式": "candidate_match_mode",
    "审批键": "old_approval_key",
    "审批组ID": "old_approval_group_id",
    "审批组状态": "queue_status",
    "审批组问题码": "group_problem_code",
    "组问题码": "group_problem_code",
    "组匹配模式": "group_match_mode",
    "锚点签名": "anchor_signatures",
    # This is the explicit, opt-in spelling allowed for a future sanitized export.
    "来源模板": "source_templates",
    "评估结果": "evaluator_result",
}

_ID_HEADERS = frozenset(
    header for header, field in _HEADER_TO_FIELD.items() if field == "candidate_id"
)
_CANDIDATE_ID_ALIASES = ("candidate_id", "candidate_candidate_id", "候选ID")
_CANDIDATE_STATUS_ALIASES = (
    "candidate_status", "status", "候选状态", "queue_status", "审批组状态",
)
_JSON_ARRAY_FIELDS = frozenset({
    "template_hashes",
    "components",
    "tags",
    "source_templates",
    "template_signatures",
    "anchor_signatures",
    "component_scope",
})
_JSON_OBJECT_FIELDS = frozenset({"evaluator_result"})
_JSON_FIELDS = _JSON_ARRAY_FIELDS | _JSON_OBJECT_FIELDS
_SOURCE_BODY_FIELDS = ("template", "pattern")
_CANONICAL_PROBLEM_CODES = frozenset(concrete_problem_codes())


def _decode_cell(value: str | None, field: str | None = None) -> Any:
    if value is None or value == "":
        return None
    if field == "candidate_id":
        return value
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as exc:
        if field in _JSON_FIELDS:
            raise InputError(f"列 {field} 包含无效 JSON") from exc
        return value
    if field in _JSON_ARRAY_FIELDS and not isinstance(decoded, list):
        raise InputError(f"列 {field} 必须是 JSON 数组")
    if field in _JSON_OBJECT_FIELDS and not isinstance(decoded, Mapping):
        raise InputError(f"列 {field} 必须是 JSON 对象")
    return decoded


def _header_name(value: str | None) -> str:
    return str(value or "").strip().lstrip("\ufeff").strip()


def _candidate_field_for_header(header: str) -> str | None:
    return _HEADER_TO_FIELD.get(header)


def _is_known_header(header: str) -> bool:
    return header in _HEADER_TO_FIELD


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _first_text(values: Mapping[str, Any], *fields: str) -> str:
    return next((_text(values.get(field)) for field in fields if _text(values.get(field))), "")


def _candidate_id_text(value: Any) -> str:
    """Accept only scalar, non-sentinel candidate identifiers."""

    if value is None or isinstance(value, (Mapping, list, tuple, set)):
        return ""
    text = _text(value)
    if text.casefold() in {"null", "true", "false"}:
        return ""
    if text[:1] in {"[", "{"}:
        try:
            decoded = json.loads(text)
        except json.JSONDecodeError:
            decoded = None
        if isinstance(decoded, (Mapping, list)) or text[-1:] in {"]", "}"}:
            return ""
    return text


def _candidate_identity_and_status(candidate: Mapping[str, Any]) -> tuple[str, str]:
    """Normalize candidate aliases for both CSV and direct reclassification input."""

    candidate_ids = {
        candidate_id
        for field in _CANDIDATE_ID_ALIASES
        if (candidate_id := _candidate_id_text(candidate.get(field)))
    }
    if len(candidate_ids) > 1:
        raise InputError("候选 ID 列冲突")
    return next(iter(candidate_ids), ""), (
        _first_text(candidate, *_CANDIDATE_STATUS_ALIASES) or "pending"
    ).lower()


def _read_candidate_rows(path: str | Path) -> list[dict[str, Any]]:
    """Read and normalize every CSV row without applying queue filtering."""

    try:
        handle = Path(path).open("r", encoding="utf-8-sig", newline="")
    except OSError as exc:
        raise InputError(f"无法读取输入 CSV: {exc}") from exc

    parsed: list[dict[str, Any]] = []
    with handle:
        reader = csv.DictReader(handle, strict=True)
        raw_headers = reader.fieldnames
        headers = [_header_name(header) for header in raw_headers or []]
        if not headers or not any(headers):
            raise InputError("输入 CSV 为空或缺少表头")
        if len(headers) != len(set(headers)):
            raise InputError("输入 CSV 表头重复")
        if not any(_is_known_header(header) for header in headers):
            raise InputError("无法识别 CSV 表头；需要 candidate_* 或受支持的中文表头")
        if not any(header in _ID_HEADERS for header in headers):
            raise InputError("CSV 缺少候选 ID 列")

        saw_data_row = False
        for line_number, raw_row in enumerate(reader, start=2):
            saw_data_row = True
            if None in raw_row or any(value is None for value in raw_row.values()):
                raise InputError(f"第 {line_number} 行列数不匹配")
            candidate: dict[str, Any] = {}
            for raw_header, value in raw_row.items():
                field = _candidate_field_for_header(_header_name(raw_header))
                if field is not None:
                    decoded = _decode_cell(value, field)
                    if field == "candidate_id":
                        candidate[_header_name(raw_header)] = decoded
                    else:
                        candidate[field] = decoded

            try:
                candidate_id, status = _candidate_identity_and_status(candidate)
            except InputError as exc:
                raise InputError(f"第 {line_number} 行{exc}") from exc
            if not candidate_id:
                raise InputError(f"第 {line_number} 行缺少候选 ID")

            candidate["candidate_id"] = candidate_id
            candidate["status"] = status
            candidate["problem_code"] = _first_text(candidate, "candidate_problem_code")
            candidate["match_mode"] = _first_text(candidate, "candidate_match_mode")
            candidate["old_review_key"] = _text(candidate.get("old_review_key"))
            candidate["old_approval_key"] = _text(candidate.get("old_approval_key"))
            candidate["old_approval_group_id"] = _text(candidate.get("old_approval_group_id"))
            candidate["approval_key"] = candidate["old_approval_key"]
            candidate["approval_group_id"] = candidate["old_approval_group_id"]
            candidate["old_problem_code"] = _first_text(
                candidate, "candidate_problem_code", "group_problem_code"
            )
            candidate["old_match_mode"] = _first_text(
                candidate, "candidate_match_mode", "group_match_mode"
            )
            parsed.append(candidate)

        if not saw_data_row:
            raise InputError("输入 CSV 缺少数据行")
    return parsed


def _new_filter_stats() -> dict[str, Any]:
    return {
        "input_row_count": 0,
        "pending_row_count": 0,
        "unique_pending_candidate_count": 0,
        "skipped_duplicate_count": 0,
        "skipped_non_pending_count": 0,
        "skipped_missing_candidate_id_count": 0,
        "skip_reasons": {
            "duplicate_candidate_id": 0,
            "non_pending": 0,
        },
    }


def _filter_pending_candidates(
    candidates: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Filter pending candidates and calculate all input/skip counters."""

    pending: list[dict[str, Any]] = []
    seen: set[str] = set()
    stats = _new_filter_stats()
    for raw_candidate in candidates:
        stats["input_row_count"] += 1
        if not isinstance(raw_candidate, Mapping):
            raise InputError("候选记录必须是对象")
        candidate = dict(raw_candidate)
        candidate_id, status = _candidate_identity_and_status(candidate)
        if not candidate_id:
            stats["skipped_missing_candidate_id_count"] += 1
            raise InputError("候选记录缺少候选 ID")
        candidate["candidate_id"] = candidate_id
        candidate["status"] = status
        if status != "pending":
            stats["skipped_non_pending_count"] += 1
            stats["skip_reasons"]["non_pending"] += 1
            continue
        stats["pending_row_count"] += 1
        if candidate_id in seen:
            stats["skipped_duplicate_count"] += 1
            stats["skip_reasons"]["duplicate_candidate_id"] += 1
            continue
        seen.add(candidate_id)
        pending.append(candidate)

    stats["unique_pending_candidate_count"] = len(pending)
    return pending, stats


def _load_csv_candidates(path: str | Path) -> list[dict[str, Any]]:
    try:
        return _read_candidate_rows(path)
    except csv.Error as exc:
        raise InputError(f"CSV 格式错误: {exc}") from exc
    except UnicodeError as exc:
        raise InputError(f"CSV 编码错误: {exc}") from exc


def _load_pending_candidates(path: str | Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    return _filter_pending_candidates(_load_csv_candidates(path))


def load_pending_candidates(path: str | Path) -> list[dict[str, Any]]:
    """Load only allowlisted pending candidate fields from a queue CSV export."""

    return _load_pending_candidates(path)[0]


def _source_body(source: Mapping[str, Any]) -> str:
    for field in _SOURCE_BODY_FIELDS:
        value = source.get(field)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _has_template_bodies(candidate: Mapping[str, Any]) -> bool:
    sources, selected_hashes, source_ids = selected_evidence_sources(candidate)
    if not sources:
        return False

    for source in sources:
        if not isinstance(source, Mapping):
            return False
        if not _source_body(source):
            return False

    if selected_hashes and not selected_hashes.issubset(source_ids):
        return False
    return True


def _safe_problem_code(value: Any) -> str:
    """Emit only resolver-registered codes; hash every other code."""

    code = _text(value)
    if code in _CANONICAL_PROBLEM_CODES:
        return code
    digest = hashlib.sha256(code.encode("utf-8")).hexdigest()[:16] if code else "unknown"
    return f"logrisk.unclassified.{digest}"


def _reason(identity: Mapping[str, Any]) -> str:
    if identity["ambiguity"]:
        return "multiple_concrete_semantics"
    if identity["semantic_safe"]:
        return str(identity["matched_rule"] or "deterministic_semantic_match")
    if identity["resolution_source"] == "fallback":
        return "no_reliable_concrete_match"
    return "generic_or_non_concrete_semantics"


def _old_identity(candidate: Mapping[str, Any]) -> dict[str, str]:
    candidate_problem_code = _first_text(
        candidate, "candidate_problem_code", "problem_code", "problemCode", "候选问题码",
    )
    group_problem_code = _first_text(
        candidate, "group_problem_code", "old_problem_code", "审批组问题码", "组问题码",
    )
    candidate_match_mode = _first_text(
        candidate, "candidate_match_mode", "match_mode", "匹配模式",
    )
    group_match_mode = _first_text(
        candidate, "group_match_mode", "old_match_mode", "组匹配模式",
    )
    return {
        "problem_code": candidate_problem_code or group_problem_code,
        "match_mode": candidate_match_mode or group_match_mode,
        "review_key": _text(candidate.get("old_review_key") or candidate.get("review_key")),
        "approval_key": _text(candidate.get("old_approval_key") or candidate.get("approval_key")),
        "approval_group_id": _text(
            candidate.get("old_approval_group_id") or candidate.get("approval_group_id")
        ),
    }


def _comparison(
    old: Mapping[str, str],
    new_review_key: str | None,
    new_approval_key: str | None,
    *,
    evidence_complete: bool,
) -> tuple[str, str]:
    if not evidence_complete:
        return "undecided", ""
    if old["review_key"] and new_review_key:
        return (
            "unchanged" if old["review_key"] == new_review_key else "changed",
            "logical_review_key",
        )
    if old["approval_key"] and new_approval_key:
        return (
            "unchanged" if old["approval_key"] == new_approval_key else "changed",
            "physical_approval_key",
        )
    return "incomparable", ""


def _report_row(
    candidate_id: str,
    old: Mapping[str, str],
    **updates: Any,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "candidate_id": candidate_id,
        "old_problem_code": old["problem_code"],
        "new_problem_code": None,
        "old_match_mode": old["match_mode"],
        "new_match_mode": None,
        "old_review_key": old["review_key"],
        "new_review_key": None,
        "confidence": None,
        "semantic_safe": False,
        "ambiguity": False,
        "evidence_source": "insufficient_evidence",
        "matched_rule": None,
        "subtype": None,
        "reason": "insufficient_evidence",
        "old_approval_key": old["approval_key"],
        "old_approval_group_id": old["approval_group_id"],
        "new_approval_key": None,
        "classification_status": "insufficient_evidence",
        "evidence_status": "insufficient_evidence",
        "comparison_status": "undecided",
        "comparison_basis": "",
    }
    row.update(updates)
    return row


def _resolver_input(candidate: Mapping[str, Any]) -> dict[str, Any]:
    """Resolve from current template evidence, never from historical labels."""

    resolved = dict(candidate)
    for field in (
        "old_problem_code",
        "old_match_mode",
        "old_review_key",
        "old_approval_key",
        "old_approval_group_id",
        "problem_code",
        "problemCode",
        "risk_type",
        "cause",
        "risk_semantic",
        "semantic_fields",
    ):
        resolved.pop(field, None)
    return resolved


@dataclass
class _ReclassificationMetrics:
    filter_stats: dict[str, Any]
    rows: list[dict[str, Any]] = field(default_factory=list)
    old_groups: set[str] = field(default_factory=set)
    old_logical_groups: set[str] = field(default_factory=set)
    old_physical_groups: set[str] = field(default_factory=set)
    old_physical_keys: set[str] = field(default_factory=set)
    new_groups: set[str] = field(default_factory=set)
    comparable_new_groups: set[str] = field(default_factory=set)
    insufficient_evidence_ids: list[str] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=lambda: {
        "semantic_safe": 0,
        "ambiguous": 0,
        "evaluated": 0,
        "evidence_insufficient": 0,
        "changed": 0,
        "incomparable": 0,
        "undecided": 0,
    })

    def record_old_identity(self, old: Mapping[str, str]) -> None:
        if old["approval_key"]:
            self.old_physical_keys.add(old["approval_key"])
        if old["review_key"]:
            self.old_logical_groups.add(old["review_key"])
            self.old_groups.add(old["review_key"])
        if old["approval_group_id"]:
            self.old_physical_groups.add(old["approval_group_id"])
            if not old["review_key"]:
                self.old_groups.add(old["approval_group_id"])
        elif old["approval_key"] and not old["review_key"]:
            self.old_groups.add(old["approval_key"])

    def record_insufficient_evidence(self, candidate_id: str) -> None:
        self.counts["evidence_insufficient"] += 1
        self.counts["undecided"] += 1
        self.insufficient_evidence_ids.append(candidate_id)

    def record_classification(
        self,
        identity: Mapping[str, Any],
        new_review_key: str,
        comparison_status: str,
    ) -> None:
        self.counts["evaluated"] += 1
        self.counts["semantic_safe"] += int(bool(identity["semantic_safe"]))
        self.counts["ambiguous"] += int(bool(identity["ambiguity"]))
        self.new_groups.add(new_review_key)
        if comparison_status == "changed":
            self.counts["changed"] += 1
        elif comparison_status == "incomparable":
            self.counts["incomparable"] += 1
        elif comparison_status == "undecided":
            self.counts["undecided"] += 1
        if comparison_status in {"changed", "unchanged"}:
            self.comparable_new_groups.add(new_review_key)

    def summary(self) -> dict[str, Any]:
        candidate_count = len(self.rows)
        evaluated_count = self.counts["evaluated"]
        semantic_safe_count = self.counts["semantic_safe"]
        ambiguous_count = self.counts["ambiguous"]
        incomparable_count = self.counts["incomparable"]
        undecided_count = self.counts["undecided"]
        semantic_group_count = sum(
            key.startswith("semantic:") for key in self.new_groups
        )
        fallback_group_count = len(self.new_groups) - semantic_group_count
        fallback_count = evaluated_count - semantic_safe_count
        comparable_count = candidate_count - incomparable_count - undecided_count
        compression_ratio = (
            1 - len(self.comparable_new_groups) / comparable_count
            if comparable_count
            else None
        )
        summary: dict[str, Any] = {
            "schema_version": "approval_reclassification_dry_run_v1",
            **self.filter_stats,
            "candidate_count": candidate_count,
            "evaluated_count": evaluated_count,
            "evidence_insufficient_count": self.counts["evidence_insufficient"],
            "insufficient_evidence_candidate_ids": self.insufficient_evidence_ids,
            "incomparable_candidate_count": incomparable_count,
            "undecided_candidate_count": undecided_count,
            "old_group_count": len(self.old_groups),
            "old_logical_group_count": len(self.old_logical_groups),
            "old_physical_group_count": len(self.old_physical_groups),
            "old_physical_approval_key_count": len(self.old_physical_keys),
            "new_group_count": len(self.new_groups),
            "comparable_candidate_count": comparable_count,
            "comparable_new_group_count": len(self.comparable_new_groups),
            "semantic_group_count": semantic_group_count,
            "fallback_group_count": fallback_group_count,
            "semantic_safe_candidate_count": semantic_safe_count,
            "fallback_candidate_count": fallback_count,
            "ambiguous_candidate_count": ambiguous_count,
            "changed_candidate_count": self.counts["changed"],
            "canonical_problem_code_coverage": (
                semantic_safe_count / evaluated_count if evaluated_count else 0.0
            ),
            "fallback_problem_code_ratio": (
                fallback_count / evaluated_count if evaluated_count else 0.0
            ),
            "approval_compression_ratio": compression_ratio,
            "approval_compression_status": "computed" if comparable_count else "undecided",
            "semantic_ambiguity_ratio": (
                ambiguous_count / evaluated_count if evaluated_count else 0.0
            ),
        }
        summary.update(_compatibility_aliases(summary))
        return summary


def _compatibility_aliases(summary: Mapping[str, Any]) -> dict[str, Any]:
    """Derive legacy metric names once at the report boundary."""

    return {
        "logrisk_approval_candidates_total": summary["candidate_count"],
        "logrisk_approval_review_groups": summary["new_group_count"],
        "logrisk_approval_canonical_candidates": summary["semantic_safe_candidate_count"],
        "logrisk_approval_fallback_candidates": summary["fallback_candidate_count"],
        "logrisk_approval_ambiguous_candidates": summary["ambiguous_candidate_count"],
        "logrisk_approval_semantic_safe_candidates": summary["semantic_safe_candidate_count"],
    }


def reclassify_candidates(
    candidates: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Resolve pending candidates without mutating storage or historical groups."""

    raw_candidates = list(candidates)
    pending_candidates, filter_stats = _filter_pending_candidates(raw_candidates)
    metrics = _ReclassificationMetrics(filter_stats)

    for raw_candidate in pending_candidates:
        candidate_id = _text(raw_candidate.get("candidate_id"))
        old = _old_identity(raw_candidate)
        metrics.record_old_identity(old)

        evidence_complete = _has_template_bodies(raw_candidate)
        if not evidence_complete:
            comparison_status, comparison_basis = _comparison(
                old,
                None,
                None,
                evidence_complete=False,
            )
            metrics.rows.append(_report_row(
                candidate_id,
                old,
                comparison_status=comparison_status,
                comparison_basis=comparison_basis,
            ))
            metrics.record_insufficient_evidence(candidate_id)
            continue

        identity = approval_identity(_resolver_input(raw_candidate))
        semantic_safe = bool(identity["semantic_safe"])
        resolved_problem_code = _text(identity.get("problem_code"))
        new_problem_code = _safe_problem_code(resolved_problem_code)
        new_match_mode = _text(identity.get("match_mode"))
        new_approval_key = _text(identity.get("approval_key"))
        new_review_key = (
            f"semantic:{resolved_problem_code}"
            if semantic_safe
            else f"approval:{new_approval_key}"
        )
        comparison_status, comparison_basis = _comparison(
            old,
            new_review_key,
            new_approval_key,
            evidence_complete=True,
        )
        metrics.record_classification(identity, new_review_key, comparison_status)
        metrics.rows.append(_report_row(
            candidate_id,
            old,
            new_problem_code=new_problem_code,
            new_match_mode=new_match_mode,
            new_review_key=new_review_key,
            confidence=identity["resolution_confidence"],
            semantic_safe=semantic_safe,
            ambiguity=bool(identity["ambiguity"]),
            evidence_source=identity["resolution_source"],
            matched_rule=identity["matched_rule"],
            subtype=identity["subtype"],
            reason=_reason(identity),
            new_approval_key=new_approval_key,
            classification_status="classified",
            evidence_status="complete",
            comparison_status=comparison_status,
            comparison_basis=comparison_basis,
        ))

    return metrics.rows, metrics.summary()


def _write_report(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _validate_output_paths(input_path: Path, output_path: Path, summary_path: Path) -> None:
    """Reject aliases or existing file identities that could overwrite the source."""

    try:
        resolved = {
            "input": input_path.resolve(),
            "output": output_path.resolve(),
            "summary": summary_path.resolve(),
        }
    except OSError as exc:
        raise InputError(f"无法解析输入/输出路径: {exc}") from exc

    if len(set(resolved.values())) != len(resolved):
        raise InputError("输入 CSV、报告 CSV 和汇总 JSON 必须使用互不相同的路径")

    existing_paths = {
        name: path
        for name, path in (
            ("input", input_path),
            ("output", output_path),
            ("summary", summary_path),
        )
        if path.exists()
    }
    for name, path in existing_paths.items():
        if name == "input":
            continue
        try:
            if os.path.samefile(input_path, path):
                raise InputError(
                    f"{name} 输出路径与输入 CSV 指向同一文件，拒绝覆盖"
                )
        except FileNotFoundError:
            continue
        except OSError:
            # The resolved-path check above still protects ordinary aliases.
            continue


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="对 pending Approval Candidate 做只读语义重分类")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只生成审计报告，不修改 Candidate 或 Approval Group",
    )
    parser.add_argument("--input", required=True, type=Path, help="审批队列 CSV 导出")
    parser.add_argument("--output", required=True, type=Path, help="逐 Candidate 重分类 CSV 输出路径")
    parser.add_argument("--summary", required=True, type=Path, help="汇总 JSON 输出路径")
    args = parser.parse_args(argv)
    if not args.dry_run:
        parser.error("必须显式指定 --dry-run；该工具不会执行物理重挂载")
    try:
        _validate_output_paths(args.input, args.output, args.summary)
        candidates = _load_csv_candidates(args.input)
        rows, summary = reclassify_candidates(candidates)
        _write_report(args.output, rows)
        _write_summary(args.summary, summary)
    except InputError as exc:
        print(f"输入错误: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
