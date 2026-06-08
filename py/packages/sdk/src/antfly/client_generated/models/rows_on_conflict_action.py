from enum import Enum


class RowsOnConflictAction(str, Enum):
    NOTHING = "nothing"
    UPDATE = "update"

    def __str__(self) -> str:
        return str(self.value)
