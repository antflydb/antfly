from enum import StrEnum


class InferenceRerankObjectObject(StrEnum):
    RERANK_SCORE = "rerank.score"

    def __str__(self) -> str:
        return str(self.value)
