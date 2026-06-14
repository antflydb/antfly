from enum import Enum


class LinkupSearchConfigDepth(str, Enum):
    DEEP = "deep"
    STANDARD = "standard"

    def __str__(self) -> str:
        return str(self.value)
