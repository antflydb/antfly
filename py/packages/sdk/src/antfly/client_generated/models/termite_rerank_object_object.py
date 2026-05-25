from enum import Enum


class TermiteRerankObjectObject(str, Enum):
    RERANK_SCORE = "rerank.score"

    def __str__(self) -> str:
        return str(self.value)
