from enum import StrEnum


class ExtractionLongDocumentMetadataDuplicateScore(StrEnum):
    MAXIMUM_CALIBRATED_SCORE = "maximum_calibrated_score"

    def __str__(self) -> str:
        return str(self.value)
