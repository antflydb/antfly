from enum import StrEnum


class ExtractionRegexValidatorMode(StrEnum):
    FULL = "full"
    PARTIAL = "partial"

    def __str__(self) -> str:
        return str(self.value)
