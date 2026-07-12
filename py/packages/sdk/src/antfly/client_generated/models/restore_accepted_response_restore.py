from enum import Enum


class RestoreAcceptedResponseRestore(str, Enum):
    COMMITTED = "committed"
    TRIGGERED = "triggered"

    def __str__(self) -> str:
        return str(self.value)
