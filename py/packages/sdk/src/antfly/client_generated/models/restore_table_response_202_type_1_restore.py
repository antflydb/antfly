from enum import Enum


class RestoreTableResponse202Type1Restore(str, Enum):
    COMMITTED = "committed"

    def __str__(self) -> str:
        return str(self.value)
