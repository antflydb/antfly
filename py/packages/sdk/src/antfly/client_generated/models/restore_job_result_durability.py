from enum import Enum


class RestoreJobResultDurability(str, Enum):
    PENDING = "pending"

    def __str__(self) -> str:
        return str(self.value)
