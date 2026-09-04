from enum import StrEnum


class RestoreJobResultDurability(StrEnum):
    PENDING = "pending"

    def __str__(self) -> str:
        return str(self.value)
