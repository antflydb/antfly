from enum import StrEnum


class ExtractionOptionsWordSplitter(StrEnum):
    CHAR = "char"
    WHITESPACE = "whitespace"

    def __str__(self) -> str:
        return str(self.value)
