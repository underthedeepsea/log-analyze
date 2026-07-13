from __future__ import annotations

import hashlib
import re


HASH_VERSION = "v2"
_PLACEHOLDER = re.compile(r"<[^<>]+>")
_WHITESPACE = re.compile(r"\s+")


def canonical_template(template: str) -> str:
    """Normalize formatting and Drain placeholders without changing semantics."""
    normalized = _PLACEHOLDER.sub("<*>", str(template).strip())
    return _WHITESPACE.sub(" ", normalized)


def _digest(*parts: str) -> str:
    value = "\x1f".join(parts)
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]


def template_fingerprint(source_type: str, component: str, template: str) -> str:
    return _digest(source_type, component, canonical_template(template))


def template_instance_hash(
    cluster: str,
    node: str,
    source_type: str,
    component: str,
    template: str,
) -> str:
    return _digest(cluster, node, source_type, component, canonical_template(template))
