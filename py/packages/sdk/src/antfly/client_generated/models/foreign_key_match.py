from enum import Enum


class ForeignKeyMatch(str, Enum):
    FULL = "full"
    SIMPLE = "simple"

    def __str__(self) -> str:
        return str(self.value)
