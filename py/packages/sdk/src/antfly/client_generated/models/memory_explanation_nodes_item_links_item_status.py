from enum import StrEnum


class MemoryExplanationNodesItemLinksItemStatus(StrEnum):
    AVAILABLE = "available"
    CHANGED = "changed"
    MISSING_OR_DENIED = "missing_or_denied"
    TRUNCATED = "truncated"

    def __str__(self) -> str:
        return str(self.value)
