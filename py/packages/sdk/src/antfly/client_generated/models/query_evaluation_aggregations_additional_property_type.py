from enum import StrEnum


class QueryEvaluationAggregationsAdditionalPropertyType(StrEnum):
    AVG = "avg"
    COUNT = "count"
    SUM = "sum"
    TERMS = "terms"

    def __str__(self) -> str:
        return str(self.value)
