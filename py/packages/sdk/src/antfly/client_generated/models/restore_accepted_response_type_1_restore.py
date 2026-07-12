from enum import Enum


class RestoreAcceptedResponseType1Restore(str, Enum):
    COMMITTED = "committed"

    def __str__(self) -> str:
        return str(self.value)
