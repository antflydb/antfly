from enum import StrEnum


class GeneratedExtractorConfigMethod(StrEnum):
    GENERATED = "generated"

    def __str__(self) -> str:
        return str(self.value)
