from enum import Enum


class GraphResolverConfigFusionCombine(str, Enum):
    MAX = "max"
    MEAN = "mean"
    NOISY_OR = "noisy_or"
    VALUE_0 = ""

    def __str__(self) -> str:
        return str(self.value)
