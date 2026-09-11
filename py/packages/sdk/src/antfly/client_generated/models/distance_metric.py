from enum import StrEnum


class DistanceMetric(StrEnum):
    COSINE = "cosine"
    INNER_PRODUCT = "inner_product"
    L2_SQUARED = "l2_squared"

    def __str__(self) -> str:
        return str(self.value)
