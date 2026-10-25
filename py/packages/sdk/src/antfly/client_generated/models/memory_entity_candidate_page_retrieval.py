from enum import StrEnum


class MemoryEntityCandidatePageRetrieval(StrEnum):
    BOUNDED_AUDIENCE_FALLBACK = "bounded_audience_fallback"
    BOUNDED_PRIMARY_FALLBACK = "bounded_primary_fallback"
    INDEXED_LEXICAL = "indexed_lexical"

    def __str__(self) -> str:
        return str(self.value)
