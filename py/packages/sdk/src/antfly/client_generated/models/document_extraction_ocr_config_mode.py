from enum import StrEnum


class DocumentExtractionOcrConfigMode(StrEnum):
    ALWAYS = "always"
    AUTO = "auto"

    def __str__(self) -> str:
        return str(self.value)
