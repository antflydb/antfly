from enum import Enum


class RowsInsertSourceRequestOp(str, Enum):
    INSERT = "insert"

    def __str__(self) -> str:
        return str(self.value)
