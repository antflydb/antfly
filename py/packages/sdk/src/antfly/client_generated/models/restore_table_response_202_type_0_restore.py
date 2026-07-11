from enum import Enum


class RestoreTableResponse202Type0Restore(str, Enum):
    TRIGGERED = "triggered"

    def __str__(self) -> str:
        return str(self.value)
