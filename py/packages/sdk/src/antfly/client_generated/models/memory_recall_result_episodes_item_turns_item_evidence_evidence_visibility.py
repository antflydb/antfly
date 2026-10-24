from enum import StrEnum


class MemoryRecallResultEpisodesItemTurnsItemEvidenceEvidenceVisibility(StrEnum):
    PRIVATE = "private"
    TEAM = "team"

    def __str__(self) -> str:
        return str(self.value)
