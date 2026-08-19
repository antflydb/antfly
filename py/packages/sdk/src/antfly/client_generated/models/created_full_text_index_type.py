from enum import Enum


class CreatedFullTextIndexType(str, Enum):
    FULL_TEXT = "full_text"

    def __str__(self) -> str:
        return str(self.value)
