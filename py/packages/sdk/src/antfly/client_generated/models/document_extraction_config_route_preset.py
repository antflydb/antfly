from enum import StrEnum


class DocumentExtractionConfigRoutePreset(StrEnum):
    EXPLICIT_ONLY = "explicit_only"
    MIXED_FILES = "mixed_files"

    def __str__(self) -> str:
        return str(self.value)
