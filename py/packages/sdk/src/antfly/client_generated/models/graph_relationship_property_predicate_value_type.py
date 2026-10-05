from enum import StrEnum


class GraphRelationshipPropertyPredicateValueType(StrEnum):
    DATETIME = "datetime"
    SCALAR = "scalar"

    def __str__(self) -> str:
        return str(self.value)
