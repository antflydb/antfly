from enum import StrEnum


class TableStorageMode(StrEnum):
    DOCUMENT = "document"
    RELATIONAL = "relational"

    def __str__(self) -> str:
        return str(self.value)
