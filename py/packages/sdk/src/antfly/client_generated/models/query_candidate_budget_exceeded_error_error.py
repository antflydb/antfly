from enum import StrEnum


class QueryCandidateBudgetExceededErrorError(StrEnum):
    QUERY_CANDIDATE_BUDGET_EXCEEDED = "query_candidate_budget_exceeded"

    def __str__(self) -> str:
        return str(self.value)
