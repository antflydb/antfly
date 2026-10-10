from enum import StrEnum


class IngestLakeChangesBodyChangesItemOp(StrEnum):
    DELETE = "delete"
    UPSERT = "upsert"

    def __str__(self) -> str:
        return str(self.value)
