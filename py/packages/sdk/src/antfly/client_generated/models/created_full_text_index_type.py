from enum import StrEnum


class CreatedFullTextIndexType(StrEnum):
    FULL_TEXT = "full_text"

    def __str__(self) -> str:
        return str(self.value)
