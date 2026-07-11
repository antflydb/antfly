from enum import Enum


class RestoreTableResponse200Restore(str, Enum):
    COMMITTED = "committed"

    def __str__(self) -> str:
        return str(self.value)
