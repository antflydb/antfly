from enum import StrEnum


class MemoryEntityViewEntityVisibility(StrEnum):
    PRIVATE = "private"
    TEAM = "team"

    def __str__(self) -> str:
        return str(self.value)
