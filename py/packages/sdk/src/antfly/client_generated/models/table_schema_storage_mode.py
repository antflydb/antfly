from enum import Enum


class TableSchemaStorageMode(str, Enum):
    DOCUMENT = "document"
    RELATIONAL = "relational"

    def __str__(self) -> str:
        return str(self.value)
