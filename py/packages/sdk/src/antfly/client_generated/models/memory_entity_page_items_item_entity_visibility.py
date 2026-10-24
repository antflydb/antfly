from enum import StrEnum


class MemoryEntityPageItemsItemEntityVisibility(StrEnum):
    PRIVATE = "private"
    TEAM = "team"

    def __str__(self) -> str:
        return str(self.value)
