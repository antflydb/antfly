from enum import StrEnum


class QueryEvaluationScope(StrEnum):
    CANDIDATES = "candidates"
    MATCHES = "matches"

    def __str__(self) -> str:
        return str(self.value)
