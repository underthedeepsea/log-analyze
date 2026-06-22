from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional


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
    r"(?P<thread>\d+)\s+"
    r"(?P<src>[a-zA-Z0-9_-]+\.go:\d+)]\s*"
    r"(?P<message>.*)$"
)

LEVEL_MAP = {
    "I": "INFO",
    "W": "WARN",
    "E": "ERROR",
    "F": "FATAL",
}


@dataclass
class NormalizedLog:
    raw_log_id: Optional[str]
    timestamp: Optional[str]
    cluster: str
    node: Optional[str]
    namespace: Optional[str]
    pod: Optional[str]
    container: Optional[str]
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


def normalize_record(record: Dict[str, Any], default_cluster: str = "default") -> Dict[str, Any]:
    raw = extract_message(record)
    source_type = str(record.get("source_type") or "unknown")
    component = str(record.get("component") or record.get("process") or "unknown")
    node = record.get("node") or record.get("host") or record.get("hostname")
    severity = record.get("level") or record.get("severity")
    message_core = raw

    syslog_match = SYSLOG_RE.match(raw)
    if syslog_match:
        gd = syslog_match.groupdict()
        node = node or gd.get("node")
        component = gd.get("process") or component
        source_type = "syslog"
        message_core = gd.get("message") or raw

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

    normalized = NormalizedLog(
        raw_log_id=record.get("raw_log_id") or record.get("_id"),
        timestamp=record.get("timestamp") or record.get("@timestamp"),
        cluster=str(record.get("cluster") or default_cluster),
        node=node,
        namespace=record.get("namespace"),
        pod=record.get("pod"),
        container=record.get("container"),
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
