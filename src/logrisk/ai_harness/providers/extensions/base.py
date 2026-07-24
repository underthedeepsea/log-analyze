from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol


@dataclass(frozen=True)
class ExtensionDescriptor:
    """Public, non-secret metadata for one explicitly registered extension."""

    adapter_id: str
    display_name: str
    supported_output_modes: tuple[str, ...]
    credential_fields: Mapping[str, str]
    config_help: str

    def public_dict(self) -> dict[str, Any]:
        return {
            "adapter_id": self.adapter_id,
            "display_name": self.display_name,
            "supported_output_modes": list(self.supported_output_modes),
            "credential_fields": dict(self.credential_fields),
            "config_help": self.config_help,
        }


@dataclass(frozen=True)
class ExtensionRequest:
    """Sanitized model request supplied to a private extension implementation."""

    connection: Mapping[str, Any]
    messages: list[dict[str, Any]]
    schema: dict[str, Any]
    model: str
    timeout: float
    options: dict[str, Any]


class ExtensionAdapter(Protocol):
    """Stable contract; private protocol/authentication code stays behind it."""

    descriptor: ExtensionDescriptor

    def validate_connection(self, connection: Mapping[str, Any]) -> None:
        ...

    def check_connection(self, connection: Mapping[str, Any]) -> dict[str, Any]:
        ...

    def generate_content(self, request: ExtensionRequest) -> str:
        ...
