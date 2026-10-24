from enum import StrEnum


class MemoryRecallResultEntitiesItemEntityVisibility(StrEnum):
    PRIVATE = "private"
    TEAM = "team"

    def __str__(self) -> str:
        return str(self.value)
