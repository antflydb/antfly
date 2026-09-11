from enum import StrEnum


class ConnectedModelType(StrEnum):
    CHUNKER = "chunker"
    CLASSIFIER = "classifier"
    EMBEDDER = "embedder"
    EXTRACTOR = "extractor"
    GENERATOR = "generator"
    OTHER = "other"
    READER = "reader"
    RERANKER = "reranker"
    REWRITER = "rewriter"
    TRANSCRIBER = "transcriber"

    def __str__(self) -> str:
        return str(self.value)
