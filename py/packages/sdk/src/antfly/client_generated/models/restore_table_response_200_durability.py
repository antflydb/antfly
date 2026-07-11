from enum import Enum


class RestoreTableResponse200Durability(str, Enum):
    DURABLE = "durable"

    def __str__(self) -> str:
        return str(self.value)
