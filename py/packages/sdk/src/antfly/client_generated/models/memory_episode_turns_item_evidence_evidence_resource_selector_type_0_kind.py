from enum import StrEnum


class MemoryEpisodeTurnsItemEvidenceEvidenceResourceSelectorType0Kind(StrEnum):
    RECORD = "record"
    RELATIONSHIP = "relationship"
    TRACE = "trace"

    def __str__(self) -> str:
        return str(self.value)
