from enum import StrEnum


class MemoryEpisodePageItemsItemTurnsItemEvidenceEvidenceVisibility(StrEnum):
    PRIVATE = "private"
    TEAM = "team"

    def __str__(self) -> str:
        return str(self.value)
