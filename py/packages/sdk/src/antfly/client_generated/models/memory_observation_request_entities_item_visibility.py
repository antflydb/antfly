from enum import StrEnum


class MemoryObservationRequestEntitiesItemVisibility(StrEnum):
    PRIVATE = "private"
    TEAM = "team"

    def __str__(self) -> str:
        return str(self.value)
