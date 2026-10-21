from enum import StrEnum


class GeneratedExtractionRequestMethod(StrEnum):
    GENERATED = "generated"

    def __str__(self) -> str:
        return str(self.value)
