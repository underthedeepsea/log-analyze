from __future__ import annotations

import contextvars
import hashlib
import uuid
import threading
from functools import wraps
from types import MappingProxyType
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import Any, Callable, Iterator, Mapping, TypeVar

from logrisk.operational_ledgers import OperationalLedgerConflict, normalize_usage


T = TypeVar("T")

_TOKEN_FIELDS = (
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "cached_input_tokens",
    "reasoning_tokens",
)
_USAGE_KEYS = frozenset(
    {
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cached_input_tokens",
        "reasoning_tokens",
        "prompt_tokens",
        "completion_tokens",
        "prompt_eval_count",
        "eval_count",
        "cached_tokens",
        "reasoning_tokens",
        "prompt_tokens_details",
        "completion_tokens_details",
    }
)
_DETAIL_KEYS = frozenset({"cached_tokens", "reasoning_tokens"})


@dataclass(frozen=True)
class UsageContext:
    """Optional context inherited by legacy model-client call signatures."""

    ledger_repository: Any | None = None
    analysis_run_id: str | None = None
    environment: str | None = None
    scope_key: str | None = None
    call_kind: str | None = None
    provider: str | None = None
    caller_kind: str | None = None
    caller_id: str | None = None
    tool_name: str | None = None
    logical_call_id: str | None = None
    metadata_contract: str | None = None


@dataclass(frozen=True)
class ModelCallOutcome:
    attempt_id: str
    transport_status: str
    usage: Mapping[str, Any]
    usage_quality: str
    validation_status: str


_CONTEXT: contextvars.ContextVar[UsageContext | None] = contextvars.ContextVar(
    "logrisk_usage_context", default=None
)
_LAST_OUTCOME: contextvars.ContextVar[ModelCallOutcome | None] = contextvars.ContextVar(
    "logrisk_model_call_outcome", default=None
)
_OUTCOME_CLIENT: contextvars.ContextVar[int | None] = contextvars.ContextVar("logrisk_outcome_client", default=None)
# Bounded stripes also support extension clients that cannot be weak-referenced.
_CLIENT_LOCKS = tuple(threading.RLock() for _ in range(64))


def _isolated_attempt(function: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(function)
    def wrapped(client: Any, *args: Any, **kwargs: Any) -> Any:
        with _CLIENT_LOCKS[(id(client) >> 4) % len(_CLIENT_LOCKS)]:
            _LAST_OUTCOME.set(None)
            _OUTCOME_CLIENT.set(None)
            return function(client, *args, **kwargs)
    return wrapped


@contextmanager
def usage_context(**kwargs: Any) -> Iterator[UsageContext]:
    """Temporarily set physical-attempt context and always restore it."""

    current = _CONTEXT.get() or UsageContext()
    values = {field: getattr(current, field) for field in UsageContext.__dataclass_fields__}
    for field, value in kwargs.items():
        if field not in values:
            raise TypeError(f"未知 usage context 字段: {field}")
        if value is not None:
            values[field] = value
    resolved = UsageContext(**values)
    token = _CONTEXT.set(resolved)
    try:
        yield resolved
    finally:
        _CONTEXT.reset(token)


def current_usage_context() -> UsageContext | None:
    """Return the current task-local context without exposing mutable state."""

    return _CONTEXT.get()


def current_model_call_outcome() -> ModelCallOutcome | None:
    return _LAST_OUTCOME.get()


def mark_model_validation(status: str) -> None:
    if status not in {"valid", "invalid", "evaluator_failed", "not_validated"}:
        raise ValueError("模型校验状态无效")
    current = _LAST_OUTCOME.get()
    if current is not None:
        _LAST_OUTCOME.set(replace(current, validation_status=status))


def _publish_outcome(client: Any, attempt_id: str, *, succeeded: bool, error: Exception | None = None) -> None:
    metadata = getattr(client, "last_metadata", {})
    metadata = metadata if isinstance(metadata, Mapping) else {}
    usage = public_usage(client)
    response_received = bool(metadata)
    status = str(getattr(error, "status", "") or "") if error is not None else ""
    _LAST_OUTCOME.set(ModelCallOutcome(
        attempt_id=attempt_id,
        transport_status="response" if response_received else ("unknown" if error is not None else "response"),
        usage=MappingProxyType(dict(usage)),
        usage_quality=str(metadata.get("usage_quality") or ("known" if usage else "unknown")),
        validation_status="valid" if succeeded else ("invalid" if status == "parse_failed" else "not_validated"),
    ))
    _OUTCOME_CLIENT.set(id(client))


def _sanitize_usage(value: Mapping[str, Any] | None) -> dict[str, Any]:
    """Keep supplier usage aliases only; arbitrary response fields are dropped."""

    if not isinstance(value, Mapping):
        return {}
    result: dict[str, Any] = {}
    for key, item in value.items():
        name = str(key)
        if name in {"prompt_tokens_details", "completion_tokens_details"}:
            if isinstance(item, Mapping):
                result[name] = {
                    detail_key: detail_value
                    for detail_key, detail_value in item.items()
                    if str(detail_key) in _DETAIL_KEYS
                }
            continue
        if name in _USAGE_KEYS:
            result[name] = item
    return result


def usage_metadata(value: Mapping[str, Any] | None) -> dict[str, Any]:
    """Build the backwards-compatible ``last_metadata`` usage shape.

    ``usage`` contains only known canonical values, while ``raw_usage`` keeps
    whitelisted aliases and invalid values available to the ledger normalizer.
    Missing values remain absent/unknown instead of becoming zero.
    """

    raw = _sanitize_usage(value)
    normalized = normalize_usage(raw if isinstance(value, Mapping) or value is None else value)  # type: ignore[arg-type]
    public_usage = {
        field: normalized[field]
        for field in _TOKEN_FIELDS
        if normalized[field] is not None
    }
    return {
        "usage": public_usage,
        "raw_usage": raw,
        "usage_quality": normalized["usage_quality"],
        "invalid_usage": bool(normalized["invalid_usage"]),
    }


def attempt_usage(client: Any) -> Mapping[str, Any] | None:
    """Read the current client's sanitized raw usage after an invocation."""

    metadata = getattr(client, "last_metadata", {})
    if not isinstance(metadata, Mapping):
        return None
    raw = metadata.get("raw_usage")
    if isinstance(raw, Mapping):
        return dict(raw)
    usage = metadata.get("usage")
    return dict(usage) if isinstance(usage, Mapping) else None


def public_usage(client: Any) -> dict[str, Any]:
    """Return the canonical, non-null usage fields for traces and callers."""

    outcome = _LAST_OUTCOME.get()
    if outcome is not None and _OUTCOME_CLIENT.get() == id(client):
        return dict(outcome.usage)
    metadata = getattr(client, "last_metadata", {})
    if not isinstance(metadata, Mapping) or not isinstance(metadata.get("usage"), Mapping):
        return {}
    return dict(metadata["usage"])


def reset_client_metadata(client: Any) -> None:
    """Clear stale supplier metadata before every physical attempt."""

    _LAST_OUTCOME.set(None)
    _OUTCOME_CLIENT.set(None)
    try:
        setattr(client, "last_metadata", {})
    except Exception:
        # A client may expose a read-only metadata property; it still gets the
        # invocation, and the ledger correctly records unknown usage.
        pass


def _client_is_fake(client: Any, provider: str | None) -> bool:
    material = " ".join(
        (
            str(provider or ""),
            str(getattr(client, "__class__", type(client)).__name__),
            str(getattr(client, "__class__", type(client)).__module__),
        )
    ).lower()
    return any(marker in material for marker in ("mock", "fake", "test"))


def _context_value(explicit: Any, inherited: Any, default: Any = None) -> Any:
    return explicit if explicit is not None else inherited if inherited is not None else default


def _next_attempt_index(repository: Any, environment: str, logical_call_id: str) -> int:
    database = getattr(repository, "database", None)
    if database is None or not callable(getattr(database, "connect", None)):
        return 0
    with database.connect() as connection:
        row = connection.execute(
            "SELECT MAX(attempt_index) AS attempt_index FROM operational_physical_calls "
            "WHERE environment=? AND logical_call_id=?",
            (environment, logical_call_id),
        ).fetchone()
    return int(row["attempt_index"] if row and row["attempt_index"] is not None else -1) + 1


def _logical_id(
    *,
    call_kind: str,
    caller_kind: str,
    caller_id: str | None,
    analysis_run_id: str | None,
    provider: str | None,
    model: str,
) -> str:
    material = "|".join(
        (
            call_kind,
            caller_kind,
            str(caller_id or ""),
            str(analysis_run_id or ""),
            str(provider or ""),
            model,
        )
    )
    return f"{caller_kind}:{hashlib.sha256(material.encode('utf-8')).hexdigest()}"


def _error_code(exc: Exception, *, default: str = "model_failed") -> str:
    status = str(getattr(exc, "status", "") or "")
    if status == "parse_failed":
        return "parse_failed"
    if status in {"model_failed", "connection_failed"}:
        return status
    return default


def _call_context(
    client: Any,
    *,
    analysis_run_id: str | None,
    environment: str | None,
    scope_key: str | None,
    call_kind: str | None,
    provider: str | None,
    caller_kind: str | None,
    caller_id: str | None,
    tool_name: str | None,
    logical_call_id: str | None,
    metadata_contract: str | None,
    model: str,
) -> UsageContext:
    inherited = _CONTEXT.get() or UsageContext()
    resolved_provider = _context_value(provider, inherited.provider, getattr(client, "provider", None))
    resolved_caller_kind = _context_value(caller_kind, inherited.caller_kind, "model_client")
    resolved_caller_id = _context_value(caller_id, inherited.caller_id)
    resolved_kind = _context_value(call_kind, inherited.call_kind, "provider")
    resolved_environment = _context_value(environment, inherited.environment)
    if resolved_environment is None:
        resolved_environment = "local-test" if _client_is_fake(client, resolved_provider) else "production"
    resolved_scope = _context_value(scope_key, inherited.scope_key, "default")
    resolved_root = _context_value(analysis_run_id, inherited.analysis_run_id)
    resolved_logical = _context_value(logical_call_id, inherited.logical_call_id)
    if resolved_logical is None:
        resolved_logical = _logical_id(
            call_kind=str(resolved_kind),
            caller_kind=str(resolved_caller_kind),
            caller_id=resolved_caller_id,
            analysis_run_id=resolved_root,
            provider=resolved_provider,
            model=model,
        )
    resolved_contract = _context_value(
        metadata_contract,
        inherited.metadata_contract,
        getattr(client, "metadata_contract", "v1"),
    )
    return UsageContext(
        ledger_repository=_context_value(None, inherited.ledger_repository),
        analysis_run_id=str(resolved_root) if resolved_root is not None else None,
        environment=str(resolved_environment),
        scope_key=str(resolved_scope),
        call_kind=str(resolved_kind),
        provider=str(resolved_provider) if resolved_provider is not None else None,
        caller_kind=str(resolved_caller_kind),
        caller_id=str(resolved_caller_id) if resolved_caller_id is not None else None,
        tool_name=str(tool_name) if tool_name is not None else inherited.tool_name,
        logical_call_id=str(resolved_logical),
        metadata_contract=str(resolved_contract),
    )


@_isolated_attempt
def run_model_attempt(
    client: Any,
    messages: list[dict[str, Any]],
    schema: dict[str, Any],
    *,
    model: str,
    timeout: float,
    options: dict[str, Any] | None = None,
    ledger_repository: Any | None = None,
    analysis_run_id: str | None = None,
    environment: str | None = None,
    scope_key: str | None = None,
    call_kind: str | None = "provider",
    provider: str | None = None,
    caller_kind: str | None = None,
    caller_id: str | None = None,
    tool_name: str | None = None,
    logical_call_id: str | None = None,
    attempt_index: int | None = None,
    metadata_contract: str | None = None,
) -> Any:
    """Execute exactly one model invocation and record one physical attempt."""

    context = _call_context(
        client,
        analysis_run_id=analysis_run_id,
        environment=environment,
        scope_key=scope_key,
        call_kind=call_kind,
        provider=provider,
        caller_kind=caller_kind,
        caller_id=caller_id,
        tool_name=tool_name,
        logical_call_id=logical_call_id,
        metadata_contract=metadata_contract,
        model=model,
    )
    repository = ledger_repository if ledger_repository is not None else (_CONTEXT.get() or UsageContext()).ledger_repository
    reset_client_metadata(client)
    attempt_id = uuid.uuid4().hex
    if repository is None:
        try:
            result = client.generate_json(messages, schema, model=model, timeout=timeout, options=options)
        except Exception as exc:
            _publish_outcome(client, attempt_id, succeeded=False, error=exc)
            raise
        _publish_outcome(client, attempt_id, succeeded=True)
        return result

    assert context.environment is not None and context.scope_key is not None
    assert context.call_kind is not None and context.caller_kind is not None
    assert context.logical_call_id is not None and context.metadata_contract is not None
    next_index = int(attempt_index) if attempt_index is not None else _next_attempt_index(
        repository, context.environment, context.logical_call_id
    )
    call_id = attempt_id
    repository.prepare_call(
        call_id=call_id,
        logical_call_id=context.logical_call_id,
        attempt_index=next_index,
        analysis_run_id=context.analysis_run_id,
        environment=context.environment,
        scope_key=context.scope_key,
        call_kind=context.call_kind,
        provider=context.provider,
        model=model,
        tool_name=context.tool_name,
        caller_kind=context.caller_kind,
        caller_id=context.caller_id,
        metadata_contract=context.metadata_contract,
    )
    repository.start_call(call_id)
    try:
        result = client.generate_json(messages, schema, model=model, timeout=timeout, options=options)
    except Exception as exc:
        repository.finish_call(
            call_id,
            status="failed",
            usage=attempt_usage(client),
            error_code=_error_code(exc),
        )
        _publish_outcome(client, call_id, succeeded=False, error=exc)
        raise
    repository.finish_call(call_id, status="succeeded", usage=attempt_usage(client))
    _publish_outcome(client, call_id, succeeded=True)
    return result


def run_agent_tool_attempt(
    operation: Callable[[], T],
    *,
    ledger_repository: Any | None = None,
    analysis_run_id: str | None = None,
    environment: str | None = None,
    scope_key: str | None = None,
    logical_call_id: str | None = None,
    attempt_index: int | None = None,
    tool_name: str,
    caller_kind: str = "agent_runtime",
    caller_id: str | None = None,
    metadata_contract: str = "deterministic-tool",
) -> T:
    """Record one deterministic Agent tool dispatch separately from Providers."""

    inherited = _CONTEXT.get() or UsageContext()
    repository = ledger_repository if ledger_repository is not None else inherited.ledger_repository
    if repository is None:
        return operation()
    env = str(_context_value(environment, inherited.environment, "production"))
    scope = str(_context_value(scope_key, inherited.scope_key, "default"))
    root_id = _context_value(analysis_run_id, inherited.analysis_run_id)
    logical = logical_call_id or _logical_id(
        call_kind="agent_tool",
        caller_kind=caller_kind,
        caller_id=caller_id,
        analysis_run_id=str(root_id) if root_id is not None else None,
        provider=None,
        model=tool_name,
    )
    index = int(attempt_index) if attempt_index is not None else _next_attempt_index(repository, env, logical)
    call_id = uuid.uuid4().hex
    repository.prepare_call(
        call_id=call_id,
        logical_call_id=logical,
        attempt_index=index,
        analysis_run_id=str(root_id) if root_id is not None else None,
        environment=env,
        scope_key=scope,
        call_kind="agent_tool",
        tool_name=tool_name,
        caller_kind=caller_kind,
        caller_id=caller_id,
        metadata_contract=metadata_contract,
    )
    repository.start_call(call_id)
    try:
        output = operation()
    except Exception as exc:
        repository.finish_call(call_id, status="failed", error_code=_error_code(exc, default="tool_failed"))
        raise
    repository.finish_call(call_id, status="succeeded")
    return output


# Descriptive aliases keep callers readable while retaining one implementation.
physical_model_attempt = run_model_attempt
physical_tool_attempt = run_agent_tool_attempt


__all__ = [
    "UsageContext",
    "ModelCallOutcome",
    "attempt_usage",
    "current_usage_context",
    "current_model_call_outcome",
    "mark_model_validation",
    "physical_model_attempt",
    "physical_tool_attempt",
    "public_usage",
    "reset_client_metadata",
    "run_agent_tool_attempt",
    "run_model_attempt",
    "usage_context",
    "usage_metadata",
]
