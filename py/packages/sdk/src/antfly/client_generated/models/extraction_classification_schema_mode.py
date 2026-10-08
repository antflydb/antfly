from enum import StrEnum


class ExtractionClassificationSchemaMode(StrEnum):
    MULTI = "multi"
    SINGLE = "single"

    def __str__(self) -> str:
        return str(self.value)
