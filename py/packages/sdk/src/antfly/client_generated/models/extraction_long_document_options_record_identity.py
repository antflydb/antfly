from enum import StrEnum


class ExtractionLongDocumentOptionsRecordIdentity(StrEnum):
    OCCURRENCE = "occurrence"
    SEMANTIC = "semantic"

    def __str__(self) -> str:
        return str(self.value)
