from enum import Enum


class RelationalIndexKeyNulls(str, Enum):
    DEFAULT = "default"
    FIRST = "first"
    LAST = "last"

    def __str__(self) -> str:
        return str(self.value)
