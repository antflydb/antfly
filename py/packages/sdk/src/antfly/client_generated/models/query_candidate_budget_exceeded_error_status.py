from enum import IntEnum


class QueryCandidateBudgetExceededErrorStatus(IntEnum):
    VALUE_422 = 422

    def __str__(self) -> str:
        return str(self.value)
