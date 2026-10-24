from enum import StrEnum


class MemoryEntityCandidatePageSemanticDiscovery(StrEnum):
    INDEXED = "indexed"
    NOT_INDEXED = "not_indexed"
    NOT_READY = "not_ready"

    def __str__(self) -> str:
        return str(self.value)
