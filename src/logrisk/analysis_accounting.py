from __future__ import annotations

import copy
import json
from typing import Any, Mapping

from logrisk.feature_jobs import FeatureJobError, validate_result_document
from logrisk.operational_ledgers import (
    MAX_INT64,
    OperationalLedgerError,
    canonical_ranges,
    source_identity,
)


class AnalysisAccountingError(ValueError):
    """Raised when an analysis root cannot be assigned safe provenance."""


def _text(value: Any, field: str) -> str:
    if isinstance(value, bool) or not isinstance(value, str) or not value.strip():
        raise AnalysisAccountingError(f"{field} 必须是非空字符串")
    if "\x00" in value:
        raise AnalysisAccountingError(f"{field} 含有非法字符")
    return value.strip()


def _count(value: Any, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise AnalysisAccountingError(f"{field} 必须是非负整数")
    if isinstance(value, int):
        result = value
    elif isinstance(value, str) and value.strip().isdigit():
        result = int(value.strip())
    else:
        raise AnalysisAccountingError(f"{field} 必须是非负整数")
    if result < 0 or result > MAX_INT64:
        raise AnalysisAccountingError(f"{field} 超出 BIGINT 非负范围")
    return result


def _range_count(ranges: list[Mapping[str, Any]]) -> int:
    total = 0
    for item in ranges:
        total += int(item["end"]) - int(item["start"])
        if total > MAX_INT64:
            raise AnalysisAccountingError("区间总长度超出 BIGINT 范围")
    return total


def _same_json(left: Any, right: Any) -> bool:
    return json.dumps(left, ensure_ascii=False, sort_keys=True, separators=(",", ":")) == json.dumps(
        right, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


class AnalysisAccounting:
    """Small coordinator for server-owned input provenance and root members."""

    def __init__(self, ledger_repository: Any, input_job_store: Any) -> None:
        if ledger_repository is None:
            raise AnalysisAccountingError("缺少 operational ledger repository")
        if input_job_store is None:
            raise AnalysisAccountingError("缺少 input job store")
        self.ledger_repository = ledger_repository
        self.input_job_store = input_job_store

    def begin(
        self,
        *,
        input_job_id: str | None = None,
        document: Mapping[str, Any] | None = None,
        request_key: str,
        environment: str = "production",
        scope_key: str = "default",
        expected_members: int = 1,
        reanalysis_of: str | None = None,
    ) -> dict[str, Any]:
        env = _text(environment, "environment")
        scope = _text(scope_key, "scope_key")
        request = _text(request_key, "request_key")
        members = _count(expected_members, "expected_members")
        if members is None or members < 1:
            raise AnalysisAccountingError("expected_members 必须大于零")

        parent: dict[str, Any] | None = None
        if reanalysis_of is not None:
            parent_id = _text(reanalysis_of, "reanalysis_of")
            if input_job_id is not None:
                raise AnalysisAccountingError("reanalysis 不接受新的 input_job_id")
            parent = self._get_root(parent_id)
            if str(parent.get("environment")) != env or str(parent.get("scope_key")) != scope:
                raise AnalysisAccountingError("parent root 与新 root 环境或作用域不一致")

        if parent is not None:
            resolved = self._resolve_parent(parent, document=document, environment=env, scope_key=scope)
        elif input_job_id is not None:
            resolved = self._resolve_input_job(_text(input_job_id, "input_job_id"), env, scope)
            if document is not None and not _same_json(document, resolved["document"]):
                raise AnalysisAccountingError("提供的 result 与服务端 input result 不一致")
        else:
            if document is None:
                raise AnalysisAccountingError("必须提供 input_job_id 或 result document")
            resolved = self._reported_document(document, env, scope)

        root = self._create_root_and_members(
            request_key=request,
            environment=env,
            scope_key=scope,
            expected_members=members,
            input_job_id=resolved.get("input_job_id"),
            source_id=resolved.get("source_id"),
            ranges=resolved.get("ranges") or [],
            input_count=resolved.get("input_count"),
            reported_input_count=resolved.get("reported_input_count"),
            provenance=resolved["provenance"],
            parent_run_id=str(parent["analysis_run_id"]) if parent is not None else None,
        )
        root_id = str(root["analysis_run_id"])
        member_ids = [f"{root_id}:{index}" for index in range(int(members))]
        root = self.ledger_repository.get_analysis_run(root_id)
        return {
            "analysis_run_id": root_id,
            "member_ids": member_ids,
            "document": copy.deepcopy(resolved.get("document")),
            "provenance": str(root.get("provenance") or resolved["provenance"]),
            "input_count": root.get("input_count"),
            "reported_input_count": root.get("reported_input_count"),
            "input_job_id": root.get("input_job_id"),
            "source_id": root.get("source_id"),
            "ranges": copy.deepcopy(root.get("ranges") or []),
            "root": root,
        }

    def attach(
        self,
        *,
        analysis_run_id: str,
        member_id: str,
        input_job_id: str | None = None,
        document: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        root_id = _text(analysis_run_id, "analysis_run_id")
        member_key = _text(member_id, "member_id")
        root = self._get_root(root_id)
        if str(root.get("status")) in {"completed", "failed", "cancelled"}:
            raise AnalysisAccountingError("终态分析根不可附加成员")
        member = next(
            (
                item
                for item in (root.get("members") or [])
                if isinstance(item, Mapping) and str(item.get("member_id")) == member_key
            ),
            None,
        )
        if member is None:
            raise AnalysisAccountingError("分析成员不存在")

        root_input_job = root.get("input_job_id")
        if input_job_id is not None and str(input_job_id) != str(root_input_job or ""):
            raise AnalysisAccountingError("input_job_id 与分析根冻结来源不一致")
        resolved_document: Mapping[str, Any] | None = None
        if root_input_job:
            resolved = self._resolve_input_job(str(root_input_job), str(root["environment"]), str(root["scope_key"]))
            self._assert_manifest_matches(root, resolved)
            resolved_document = resolved["document"]
            if document is not None and not _same_json(document, resolved_document):
                raise AnalysisAccountingError("提供的 result 与服务端 input result 不一致")
        elif document is not None:
            try:
                resolved_document = validate_result_document(copy.deepcopy(dict(document)))
            except FeatureJobError as exc:
                raise AnalysisAccountingError(str(exc)) from exc
            if str(root.get("provenance")) == "unverified-input":
                reported_count = _reported_input_count(resolved_document)
                if reported_count != root.get("reported_input_count"):
                    raise AnalysisAccountingError("result 与分析根申报输入计数不一致")

        return {
            "analysis_run_id": root_id,
            "member_id": member_key,
            "document": copy.deepcopy(resolved_document),
            "provenance": root.get("provenance"),
            "input_count": root.get("input_count"),
            "reported_input_count": root.get("reported_input_count"),
            "input_job_id": root_input_job,
            "source_id": root.get("source_id"),
            "ranges": copy.deepcopy(root.get("ranges") or []),
            "root": root,
            "member": copy.deepcopy(dict(member)),
        }

    def _get_root(self, analysis_run_id: str) -> dict[str, Any]:
        try:
            root = self.ledger_repository.get_analysis_run(analysis_run_id)
        except KeyError as exc:
            raise AnalysisAccountingError("分析根不存在") from exc
        if not isinstance(root, dict):
            raise AnalysisAccountingError("分析根读取结果无效")
        return root

    def _create_root_and_members(self, **kwargs: Any) -> dict[str, Any]:
        """Persist a root and all reserved members in one repository transaction."""

        member_count = int(kwargs["expected_members"])
        database = getattr(self.ledger_repository, "database", None)
        if database is not None and callable(getattr(database, "transaction", None)):
            with database.transaction() as connection:
                root = self.ledger_repository.create_analysis_run(
                    connection=connection, **kwargs
                )
                root_id = _text(root.get("analysis_run_id"), "analysis_run_id")
                member_ids = [f"{root_id}:{index}" for index in range(member_count)]
                for member_id in member_ids:
                    self.ledger_repository.register_analysis_member(
                        root_id, member_id, connection=connection
                    )
                return root
        root = self.ledger_repository.create_analysis_run(**kwargs)
        root_id = _text(root.get("analysis_run_id"), "analysis_run_id")
        member_ids = [f"{root_id}:{index}" for index in range(member_count)]
        for member_id in member_ids:
            self.ledger_repository.register_analysis_member(root_id, member_id)
        return root

    def _resolve_input_job(self, input_job_id: str, environment: str, scope_key: str) -> dict[str, Any]:
        try:
            job = self.input_job_store.get_job(input_job_id)
        except Exception as exc:
            raise AnalysisAccountingError("无法读取 input job") from exc
        if not isinstance(job, Mapping) or str(job.get("status")) != "completed":
            raise AnalysisAccountingError("input job 尚未完成，无法冻结来源")
        try:
            document = self.input_job_store.get_result(input_job_id)
        except Exception as exc:
            raise AnalysisAccountingError("input job 缺少已保存 result") from exc
        try:
            document = validate_result_document(copy.deepcopy(document))
        except FeatureJobError as exc:
            raise AnalysisAccountingError(str(exc)) from exc

        receipts: list[dict[str, Any]] = []
        after: str | None = None
        while True:
            try:
                page = self.ledger_repository.list_ingestion_batches(
                    input_job_id,
                    after_batch_id=after,
                    limit=500,
                )
            except Exception as exc:
                raise AnalysisAccountingError("无法读取 input ingestion receipts") from exc
            if not isinstance(page, Mapping) or not isinstance(page.get("items"), list):
                raise AnalysisAccountingError("input ingestion receipts 格式无效")
            if any(not isinstance(item, Mapping) for item in page["items"]):
                raise AnalysisAccountingError("input ingestion receipt 格式无效")
            receipts.extend(page["items"])
            if not bool(page.get("has_more")):
                break
            next_after = page.get("next_after_batch_id")
            if not isinstance(next_after, str) or not next_after or next_after == after:
                raise AnalysisAccountingError("input ingestion receipts 分页游标无效")
            after = next_after
        if not receipts:
            raise AnalysisAccountingError("input job 缺少完整 verified ingestion receipt")

        root_ranges: list[dict[str, Any]] = []
        source_ids: set[str] = set()
        for index, receipt in enumerate(receipts):
            if str(receipt.get("input_job_id")) != input_job_id:
                raise AnalysisAccountingError(f"ingestion receipt[{index}] input_job_id 不一致")
            if str(receipt.get("provenance")) != "verified":
                raise AnalysisAccountingError(f"ingestion receipt[{index}] provenance 未验证")
            source = receipt.get("source")
            if not isinstance(source, Mapping):
                raise AnalysisAccountingError(f"ingestion receipt[{index}] 缺少 source")
            source_id = _text(source.get("source_id"), f"ingestion receipt[{index}].source_id")
            source_environment = _text(source.get("environment"), "source.environment")
            source_scope = _text(source.get("scope_key"), "source.scope_key")
            source_kind = _text(source.get("source_kind"), "source.source_kind")
            identity_digest = _text(source.get("identity_digest"), "source.identity_digest")
            if source_environment != environment or source_scope != scope_key:
                raise AnalysisAccountingError("ingestion receipt 与请求环境或作用域不一致")
            try:
                if source_identity(
                    environment=source_environment,
                    scope_key=source_scope,
                    source_kind=source_kind,
                    identity_digest=identity_digest,
                ) != source_id:
                    raise AnalysisAccountingError("ingestion receipt source_id 无效")
            except OperationalLedgerError as exc:
                raise AnalysisAccountingError(str(exc)) from exc
            raw_ranges = receipt.get("ranges")
            try:
                normalized = canonical_ranges(raw_ranges if isinstance(raw_ranges, list) else [])
            except OperationalLedgerError as exc:
                raise AnalysisAccountingError(f"ingestion receipt[{index}] ranges 无效") from exc
            actual_count = _count(receipt.get("actual_count"), f"ingestion receipt[{index}].actual_count")
            if actual_count is None or actual_count != _range_count(normalized):
                raise AnalysisAccountingError(f"ingestion receipt[{index}] actual_count 与区间不一致")
            source_ids.add(source_id)
            root_ranges.extend({**item, "source_id": source_id} for item in normalized)

        root_ranges = canonical_ranges(root_ranges)
        source_id = next(iter(source_ids)) if len(source_ids) == 1 else None
        return {
            "input_job_id": input_job_id,
            "document": document,
            "provenance": "verified",
            "input_count": _range_count(root_ranges),
            "reported_input_count": None,
            "source_id": source_id,
            "ranges": root_ranges,
        }

    def _reported_document(self, document: Mapping[str, Any], environment: str, scope_key: str) -> dict[str, Any]:
        try:
            validated = validate_result_document(copy.deepcopy(dict(document)))
        except FeatureJobError as exc:
            raise AnalysisAccountingError(str(exc)) from exc
        return {
            "document": validated,
            "provenance": "unverified-input",
            "input_count": None,
            "reported_input_count": _reported_input_count(validated),
            "input_job_id": None,
            "source_id": None,
            "ranges": [],
            "environment": environment,
            "scope_key": scope_key,
        }

    def _resolve_parent(
        self,
        parent: Mapping[str, Any],
        *,
        document: Mapping[str, Any] | None,
        environment: str,
        scope_key: str,
    ) -> dict[str, Any]:
        parent_input_job = parent.get("input_job_id")
        if parent_input_job:
            resolved = self._resolve_input_job(str(parent_input_job), environment, scope_key)
            self._assert_manifest_matches(parent, resolved)
            if document is not None and not _same_json(document, resolved["document"]):
                raise AnalysisAccountingError("提供的 result 与 parent input result 不一致")
            return resolved
        if document is not None:
            resolved = self._reported_document(document, environment, scope_key)
            if str(parent.get("provenance")) != resolved["provenance"]:
                raise AnalysisAccountingError("reanalysis provenance 与 parent root 不一致")
            if resolved.get("reported_input_count") != parent.get("reported_input_count"):
                raise AnalysisAccountingError("reanalysis document 与 parent root 不一致")
            return {
                **resolved,
                "input_job_id": None,
                "source_id": parent.get("source_id"),
                "ranges": copy.deepcopy(parent.get("ranges") or []),
                "input_count": parent.get("input_count"),
                "reported_input_count": parent.get("reported_input_count"),
                "provenance": parent.get("provenance"),
            }
        if str(parent.get("provenance")) == "verified":
            return {
                "document": None,
                "provenance": "verified",
                "input_count": parent.get("input_count"),
                "reported_input_count": None,
                "input_job_id": None,
                "source_id": parent.get("source_id"),
                "ranges": copy.deepcopy(parent.get("ranges") or []),
            }
        raise AnalysisAccountingError("reported parent root 需要原始 validated result 才能 reanalysis")

    def _assert_manifest_matches(self, root: Mapping[str, Any], resolved: Mapping[str, Any]) -> None:
        if str(root.get("provenance")) != "verified" or resolved.get("provenance") != "verified":
            raise AnalysisAccountingError("分析根来源 provenance 不可升级")
        if root.get("input_job_id") and str(root.get("input_job_id")) != str(resolved.get("input_job_id")):
            raise AnalysisAccountingError("input_job_id 与分析根不一致")
        if root.get("input_count") != resolved.get("input_count"):
            raise AnalysisAccountingError("服务端 input manifest 计数已变化")
        if root.get("source_id") != resolved.get("source_id"):
            raise AnalysisAccountingError("服务端 input manifest 来源已变化")
        if not _same_json(root.get("ranges") or [], resolved.get("ranges") or []):
            raise AnalysisAccountingError("服务端 input manifest 区间已变化")


def _reported_input_count(document: Mapping[str, Any]) -> int | None:
    summary = document.get("summary")
    if not isinstance(summary, Mapping):
        return None
    for field in ("records_parsed", "total_raw_logs", "lines_read"):
        if field not in summary or summary.get(field) is None:
            continue
        return _count(summary.get(field), f"summary.{field}")
    return None
