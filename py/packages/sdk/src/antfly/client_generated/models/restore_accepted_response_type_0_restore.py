from enum import Enum


class RestoreAcceptedResponseType0Restore(str, Enum):
    TRIGGERED = "triggered"

    def __str__(self) -> str:
        return str(self.value)
