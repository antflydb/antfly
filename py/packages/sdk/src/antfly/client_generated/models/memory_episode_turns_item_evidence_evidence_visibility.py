from enum import StrEnum


class MemoryEpisodeTurnsItemEvidenceEvidenceVisibility(StrEnum):
    PRIVATE = "private"
    TEAM = "team"

    def __str__(self) -> str:
        return str(self.value)
