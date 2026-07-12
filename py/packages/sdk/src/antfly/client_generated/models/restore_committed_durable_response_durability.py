from enum import Enum


class RestoreCommittedDurableResponseDurability(str, Enum):
    DURABLE = "durable"

    def __str__(self) -> str:
        return str(self.value)
