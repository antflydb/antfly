from enum import StrEnum


class RestoreJobResultRestore(StrEnum):
    COMMITTED = "committed"
    TRIGGERED = "triggered"

    def __str__(self) -> str:
        return str(self.value)
