from enum import StrEnum


class MemorySupersessionCandidatePageSemanticDiscovery(StrEnum):
    DISABLED = "disabled"
    INDEXED = "indexed"
    NOT_READY = "not_ready"

    def __str__(self) -> str:
        return str(self.value)
