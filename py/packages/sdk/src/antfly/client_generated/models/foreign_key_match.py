from enum import StrEnum


class ForeignKeyMatch(StrEnum):
    FULL = "full"
    PARTIAL = "partial"
    SIMPLE = "simple"

    def __str__(self) -> str:
        return str(self.value)
