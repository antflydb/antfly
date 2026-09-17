from enum import StrEnum


class ExtractionLongDocumentMetadataNaturalRecordIdentity(StrEnum):
    EXACT_SOURCE_ANCHOR = "exact_source_anchor"

    def __str__(self) -> str:
        return str(self.value)
