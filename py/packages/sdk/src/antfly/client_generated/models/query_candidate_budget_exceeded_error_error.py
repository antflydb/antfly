from enum import Enum


class QueryCandidateBudgetExceededErrorError(str, Enum):
    QUERY_CANDIDATE_BUDGET_EXCEEDED = "query_candidate_budget_exceeded"

    def __str__(self) -> str:
        return str(self.value)
