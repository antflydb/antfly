from enum import Enum


class ExtractionResponseObject(str, Enum):
    EXTRACTION = "extraction"

    def __str__(self) -> str:
        return str(self.value)
