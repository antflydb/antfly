from enum import Enum


class GraphPathWeightDomainErrorMode(str, Enum):
    MAX_WEIGHT = "max_weight"
    MIN_WEIGHT = "min_weight"
    WEIGHTED_PATH = "weighted_path"

    def __str__(self) -> str:
        return str(self.value)
