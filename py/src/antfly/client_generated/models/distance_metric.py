from enum import Enum


class DistanceMetric(str, Enum):
    COSINE = "cosine"
    INNER_PRODUCT = "inner_product"
    L2_SQUARED = "l2_squared"

    def __str__(self) -> str:
        return str(self.value)
