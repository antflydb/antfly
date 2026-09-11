from enum import StrEnum


class InferenceModelKind(StrEnum):
    CHUNKER = "chunker"
    CLASSIFIER = "classifier"
    EMBEDDER = "embedder"
    EXTRACTOR = "extractor"
    GENERATOR = "generator"
    READER = "reader"
    RERANKER = "reranker"
    REWRITER = "rewriter"
    TRANSCRIBER = "transcriber"

    def __str__(self) -> str:
        return str(self.value)
