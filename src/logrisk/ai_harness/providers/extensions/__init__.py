from __future__ import annotations

"""Committed extension-adapter boundary for internally adapted model providers."""

from logrisk.ai_harness.providers.extensions.registry import (
    get_extension_adapter,
    list_extension_descriptors,
)

__all__ = ["get_extension_adapter", "list_extension_descriptors"]
