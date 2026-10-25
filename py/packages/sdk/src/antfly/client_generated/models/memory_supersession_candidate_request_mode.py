from enum import StrEnum


class MemorySupersessionCandidateRequestMode(StrEnum):
    LEXICAL = "lexical"
    SEMANTIC = "semantic"
    UNION_SEARCH = "union_search"

    def __str__(self) -> str:
        return str(self.value)
