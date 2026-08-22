from enum import Enum


class CreateFullTextIndexRequestType(str, Enum):
    FULL_TEXT = "full_text"

    def __str__(self) -> str:
        return str(self.value)
