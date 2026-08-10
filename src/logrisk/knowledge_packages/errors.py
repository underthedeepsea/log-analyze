from __future__ import annotations


class KnowledgePackageError(ValueError):
    """Stable, safe-to-expose validation or lifecycle error."""

    def __init__(self, message: str, *, code: str = "knowledge_package_invalid") -> None:
        super().__init__(message)
        self.code = code
