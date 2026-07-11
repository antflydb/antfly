from enum import Enum


class RestoreTableResponse202Type1Durability(str, Enum):
    PENDING = "pending"

    def __str__(self) -> str:
        return str(self.value)
