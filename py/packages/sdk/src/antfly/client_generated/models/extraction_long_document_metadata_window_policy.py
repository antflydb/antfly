from enum import StrEnum


class ExtractionLongDocumentMetadataWindowPolicy(StrEnum):
    SOURCE_WORDS_MIDPOINT_OWNERSHIP = "source_words_midpoint_ownership"

    def __str__(self) -> str:
        return str(self.value)
