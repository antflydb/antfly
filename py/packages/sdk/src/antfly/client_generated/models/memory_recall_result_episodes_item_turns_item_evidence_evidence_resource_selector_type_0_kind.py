from enum import StrEnum


class MemoryRecallResultEpisodesItemTurnsItemEvidenceEvidenceResourceSelectorType0Kind(StrEnum):
    RECORD = "record"
    RELATIONSHIP = "relationship"
    TRACE = "trace"

    def __str__(self) -> str:
        return str(self.value)
