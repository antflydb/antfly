from enum import StrEnum


class DocumentExtractionReaderConfigRecognitionLevel(StrEnum):
    ACCURATE = "accurate"
    FAST = "fast"

    def __str__(self) -> str:
        return str(self.value)
