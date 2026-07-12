from enum import Enum


class RestoreTriggeredResponseRestore(str, Enum):
    TRIGGERED = "triggered"

    def __str__(self) -> str:
        return str(self.value)
