from enum import StrEnum


class ExtractionLongDocumentMetadataOtherRecordIdentity(StrEnum):
    OCCURRENCE = "occurrence"
    SEMANTIC = "semantic"

    def __str__(self) -> str:
        return str(self.value)
