from enum import StrEnum


class RerankerCandidateLimitExceededErrorError(StrEnum):
    RERANKER_CANDIDATE_LIMIT_EXCEEDED = "reranker_candidate_limit_exceeded"

    def __str__(self) -> str:
        return str(self.value)
