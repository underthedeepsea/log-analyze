from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo


SYSLOG_RE = re.compile(
    r"^(?P<month>\w{3})\s+(?P<day>\d{1,2})\s+"
    r"(?P<time>\d{2}:\d{2}:\d{2})\s+"
    r"(?P<node>\S+)\s+"
    r"(?P<process>[a-zA-Z0-9_.-]+)(?:\[(?P<pid>\d+)\])?:\s*"
    r"(?P<message>.*)$"
)

KLOG_RE = re.compile(
    r"^(?P<level>[IWEF])(?P<date>\d{4})\s+"
    r"(?P<time>\d{2}:\d{2}:\d{2}\.\d{6})\s+"
    r"(?:(?P<thread>\d+)\s+)?"
    r"(?P<src>[a-zA-Z0-9_-]+\.go:\d+)]\s*"
    r"(?P<message>.*)$"
)

LEVEL_MAP = {
    "I": "INFO",
    "W": "WARN",
    "E": "ERROR",
    "F": "FATAL",
}

LOG_CLOUD_PROFILE = "log-cloud-shanghai-v1"
CRI_RE = re.compile(r"^(?P<timestamp>\S+)\s+(?:stdout|stderr)\s+[FP]\s+(?P<body>.*)$")


class ConfiguredSourceText(str):
    """Transient source-owned hint; JSON producer strings cannot supply this type."""


def validate_source_profile(profile: Any) -> str | None:
    if profile is not None and (not isinstance(profile, str) or profile != LOG_CLOUD_PROFILE):
        raise ValueError("未登记的来源解析profile")
    return profile


def _identity_text(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip() or len(value) > 256 or any(ord(char) < 32 for char in value):
        return None
    return value.strip()


@dataclass
class NormalizedLog:
    raw_log_id: Optional[str]
    timestamp: Optional[str]
    cluster: str
    node: Optional[str]
    namespace: Optional[str]
    pod: Optional[str]
    container: Optional[str]
    device: Optional[str]
    source_type: str
    component: str
    severity: Optional[str]
    message_core: str
    raw_log: str
    labels: Dict[str, Any]


def extract_message(record: Dict[str, Any]) -> str:
    for key in ("message", "msg", "log", "content", "text", "raw", "line"):
        if key in record:
            return str(record[key])
    return str(record)


def normalize_record(record: Dict[str, Any], default_cluster: str = "default", *, source_profile: str | None = None) -> Dict[str, Any]:
    hint = record.get("_source_profile")
    profile = validate_source_profile(source_profile if source_profile is not None else hint if isinstance(hint, ConfiguredSourceText) else None)
    raw = extract_message(record)
    envelope = CRI_RE.match(raw) if profile else None
    raw_core = envelope["body"] if envelope else raw
    source_type = str(record.get("source_type") or "unknown")
    component = str(record.get("component") or record.get("process") or "unknown")
    node = record.get("node") or record.get("host") or record.get("hostname")
    if profile:
        node = next((value for key in ("node", "host", "hostname") if (value := _identity_text(record.get(key))) is not None), None)
    severity = record.get("level") or record.get("severity")
    message_core = raw_core

    syslog_match = SYSLOG_RE.match(raw_core)
    if syslog_match:
        gd = syslog_match.groupdict()
        node = node or (_identity_text(gd.get("node")) if profile else gd.get("node"))
        component = gd.get("process") or component
        source_type = "syslog"
        message_core = gd.get("message") or raw_core

    # K8s klog prefix inside syslog or component logs.
    klog_match = KLOG_RE.match(message_core)
    if klog_match:
        kgd = klog_match.groupdict()
        severity = severity or LEVEL_MAP.get(kgd.get("level"), "INFO")
        message_core = kgd.get("message") or message_core

    # kernel syslog does not have klog level; keep severity if user provided it.
    if component == "kernel" and not severity:
        severity = "ERROR" if any(x in message_core.lower() for x in ["error", "fail", "out of memory", "killed process"]) else "INFO"

    # Pod logs usually have component in metadata.
    if source_type == "podlog" and component == "unknown":
        component = str(record.get("container") or "podlog")

    if profile and node is None:
        metadata = record.get("source_metadata")
        if isinstance(metadata, dict):
            node = _identity_text(metadata.get("node_id")) or _identity_text(metadata.get("hostname"))
    timestamp = record.get("timestamp") or record.get("@timestamp")
    if profile and timestamp:
        try:
            parsed = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
            timestamp = (parsed if parsed.tzinfo else parsed.replace(tzinfo=ZoneInfo("Asia/Shanghai"))).astimezone(timezone.utc).isoformat()
        except (ValueError, TypeError, OverflowError):
            # A malformed discovery time never borrows the unrelated CRI body time.
            pass
    cluster = record.get("cluster")
    cluster_hint = record.get("_source_cluster")
    if profile or isinstance(cluster_hint, ConfiguredSourceText):
        # A producer's explicit logical cluster remains distinct from the source
        # cluster default. Neither is inferred from message text or a peer address.
        candidate = cluster if cluster is not None else cluster_hint if isinstance(cluster_hint, ConfiguredSourceText) else default_cluster
        cluster = _identity_text(candidate)
        if cluster is None:
            raise ValueError("显式日志cluster必须是有效的非空标识")
    else:
        cluster = str(cluster or default_cluster)

    normalized = NormalizedLog(
        raw_log_id=record.get("raw_log_id") or record.get("_id"),
        timestamp=timestamp,
        cluster=cluster,
        node=node,
        namespace=record.get("namespace"),
        pod=record.get("pod"),
        container=record.get("container"),
        device=record.get("device") or (record.get("labels") or {}).get("device"),
        source_type=source_type,
        component=component,
        severity=str(severity).upper() if severity else None,
        message_core=message_core,
        raw_log=raw,
        labels=record.get("labels") or {},
    )
    return asdict(normalized)


def normalize_records(records: list[Dict[str, Any]], default_cluster: str = "default") -> list[Dict[str, Any]]:
    return [normalize_record(r, default_cluster=default_cluster) for r in records]
