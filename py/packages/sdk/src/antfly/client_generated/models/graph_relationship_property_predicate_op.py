from enum import StrEnum


class GraphRelationshipPropertyPredicateOp(StrEnum):
    EQ = "eq"
    GT = "gt"
    GTE = "gte"
    IS_NOT_NULL = "is_not_null"
    IS_NULL = "is_null"
    LT = "lt"
    LTE = "lte"
    NE = "ne"

    def __str__(self) -> str:
        return str(self.value)
