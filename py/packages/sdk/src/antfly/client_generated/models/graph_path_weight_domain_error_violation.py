from enum import Enum


class GraphPathWeightDomainErrorViolation(str, Enum):
    EDGE_WEIGHT_ABOVE_ONE = "edge_weight_above_one"
    NEGATIVE_EDGE_WEIGHT = "negative_edge_weight"
    PATH_SUM_OVERFLOW = "path_sum_overflow"

    def __str__(self) -> str:
        return str(self.value)
