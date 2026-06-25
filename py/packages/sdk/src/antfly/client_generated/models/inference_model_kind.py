from enum import Enum


class InferenceModelKind(str, Enum):
    CHUNKER = "chunker"
    CLASSIFIER = "classifier"
    EMBEDDER = "embedder"
    EXTRACTOR = "extractor"
    GENERATOR = "generator"
    READER = "reader"
    RECOGNIZER = "recognizer"
    RERANKER = "reranker"
    REWRITER = "rewriter"
    TRANSCRIBER = "transcriber"

    def __str__(self) -> str:
        return str(self.value)
