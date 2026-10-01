from enum import StrEnum


class ExtractionLongDocumentMetadataClassificationAggregation(StrEnum):
    OWNED_WORD_WEIGHTED_MEAN_RAW_LOGITS = "owned_word_weighted_mean_raw_logits"

    def __str__(self) -> str:
        return str(self.value)
