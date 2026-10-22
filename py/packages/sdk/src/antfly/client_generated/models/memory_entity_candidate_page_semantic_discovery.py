from enum import StrEnum


class MemoryEntityCandidatePageSemanticDiscovery(StrEnum):
    NOT_INDEXED = "not_indexed"

    def __str__(self) -> str:
        return str(self.value)
