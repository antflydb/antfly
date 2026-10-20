from enum import StrEnum


class EnrichmentProducerConfigType(StrEnum):
    COPY = "copy"
    DECISION = "decision"
    DOCUMENT_EXTRACTION = "document_extraction"
    EXTRACTOR = "extractor"
    GENERATOR = "generator"
    READER = "reader"
    TRANSCRIBER = "transcriber"

    def __str__(self) -> str:
        return str(self.value)
