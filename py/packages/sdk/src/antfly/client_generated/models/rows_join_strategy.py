from enum import Enum


class RowsJoinStrategy(str, Enum):
    AUTO = "auto"
    HASH = "hash"
    LOOKUP = "lookup"
    MERGE = "merge"

    def __str__(self) -> str:
        return str(self.value)
