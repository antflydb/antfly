from enum import StrEnum


class ExtractionRegexValidatorType(StrEnum):
    REGEX = "regex"

    def __str__(self) -> str:
        return str(self.value)
