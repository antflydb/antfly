from enum import StrEnum


class MemorySupersessionCandidatePageRetrieval(StrEnum):
    INDEXED = "indexed"
    NOT_READY = "not_ready"

    def __str__(self) -> str:
        return str(self.value)
