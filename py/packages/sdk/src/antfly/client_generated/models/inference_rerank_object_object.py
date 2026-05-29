from enum import Enum


class InferenceRerankObjectObject(str, Enum):
    RERANK_SCORE = "rerank.score"

    def __str__(self) -> str:
        return str(self.value)
