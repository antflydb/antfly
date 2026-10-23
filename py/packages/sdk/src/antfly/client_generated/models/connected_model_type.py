from enum import StrEnum


class ConnectedModelType(StrEnum):
    CHUNKER = "chunker"
    CLASSIFIER = "classifier"
    DECIDER = "decider"
    EMBEDDER = "embedder"
    EXTRACTOR = "extractor"
    GENERATOR = "generator"
    OTHER = "other"
    PREDICTOR = "predictor"
    READER = "reader"
    RERANKER = "reranker"
    REWRITER = "rewriter"
    SYNTHESIZER = "synthesizer"
    TRANSCRIBER = "transcriber"

    def __str__(self) -> str:
        return str(self.value)
