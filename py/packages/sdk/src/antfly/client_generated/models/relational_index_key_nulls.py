from enum import StrEnum


class RelationalIndexKeyNulls(StrEnum):
    DEFAULT = "default"
    FIRST = "first"
    LAST = "last"

    def __str__(self) -> str:
        return str(self.value)
