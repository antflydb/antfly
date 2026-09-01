from enum import IntEnum


class GraphDistinctBudgetExceededErrorStatus(IntEnum):
    VALUE_422 = 422

    def __str__(self) -> str:
        return str(self.value)
