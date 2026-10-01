from enum import StrEnum


class ExtractionLongDocumentOptionsMode(StrEnum):
    REJECT = "reject"
    WINDOW = "window"

    def __str__(self) -> str:
        return str(self.value)
