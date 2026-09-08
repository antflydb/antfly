from enum import StrEnum


class ExtractionResponseObject(StrEnum):
    EXTRACTION = "extraction"

    def __str__(self) -> str:
        return str(self.value)
