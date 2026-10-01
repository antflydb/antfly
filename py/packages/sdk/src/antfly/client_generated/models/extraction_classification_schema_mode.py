from enum import StrEnum


class ExtractionClassificationSchemaMode(StrEnum):
    BOOLEAN = "boolean"
    MULTI = "multi"
    ORDINAL = "ordinal"
    SINGLE = "single"

    def __str__(self) -> str:
        return str(self.value)
