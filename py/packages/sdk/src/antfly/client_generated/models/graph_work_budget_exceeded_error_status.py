from enum import IntEnum


class GraphWorkBudgetExceededErrorStatus(IntEnum):
    VALUE_422 = 422

    def __str__(self) -> str:
        return str(self.value)
