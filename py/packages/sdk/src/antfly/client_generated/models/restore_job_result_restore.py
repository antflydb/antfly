from enum import Enum


class RestoreJobResultRestore(str, Enum):
    TRIGGERED = "triggered"

    def __str__(self) -> str:
        return str(self.value)
