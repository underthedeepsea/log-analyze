from __future__ import annotations


class AgenticError(ValueError):
    """Stable error that never includes model content or credentials."""

    def __init__(self, message: str, *, code: str = "agentic_invalid", status_code: int = 422) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code
