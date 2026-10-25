from enum import StrEnum


class EnrichmentProducerConfigType(StrEnum):
    COPY = "copy"
    DECISION = "decision"
    DOCUMENT_EXTRACTION = "document_extraction"
    EXTRACTOR = "extractor"
    GENERATOR = "generator"
    MEDIA_OUTPUT = "media_output"
    READER = "reader"
    TRANSCRIBER = "transcriber"
    VECTOR_LABELS = "vector_labels"

    def __str__(self) -> str:
        return str(self.value)
