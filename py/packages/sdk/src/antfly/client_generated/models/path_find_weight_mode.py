from enum import Enum


class PathFindWeightMode(str, Enum):
    MAX_WEIGHT = "max_weight"
    MIN_HOPS = "min_hops"
    MIN_WEIGHT = "min_weight"

    def __str__(self) -> str:
        return str(self.value)
