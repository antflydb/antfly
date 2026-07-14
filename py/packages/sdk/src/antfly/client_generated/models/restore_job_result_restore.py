from enum import Enum


class RestoreJobResultRestore(str, Enum):
    COMMITTED = "committed"
    TRIGGERED = "triggered"

    def __str__(self) -> str:
        return str(self.value)
