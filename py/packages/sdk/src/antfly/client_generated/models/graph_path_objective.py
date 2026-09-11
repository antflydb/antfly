from enum import StrEnum


class GraphPathObjective(StrEnum):
    MAX_WEIGHT_PRODUCT = "max_weight_product"
    MIN_HOPS = "min_hops"
    MIN_WEIGHT_SUM = "min_weight_sum"

    def __str__(self) -> str:
        return str(self.value)
