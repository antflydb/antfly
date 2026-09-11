from enum import StrEnum


class FailedOperationOperation(StrEnum):
    DELETE = "delete"
    UPSERT = "upsert"

    def __str__(self) -> str:
        return str(self.value)
