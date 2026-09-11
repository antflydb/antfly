from enum import StrEnum


class LinkupSearchConfigDepth(StrEnum):
    DEEP = "deep"
    STANDARD = "standard"

    def __str__(self) -> str:
        return str(self.value)
