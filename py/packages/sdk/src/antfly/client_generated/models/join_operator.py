from enum import StrEnum


class JoinOperator(StrEnum):
    EQ = "eq"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    NEQ = "neq"

    def __str__(self) -> str:
        return str(self.value)
