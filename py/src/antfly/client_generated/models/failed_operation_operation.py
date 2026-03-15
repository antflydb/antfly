from enum import Enum


class FailedOperationOperation(str, Enum):
    DELETE = "delete"
    UPSERT = "upsert"

    def __str__(self) -> str:
        return str(self.value)
