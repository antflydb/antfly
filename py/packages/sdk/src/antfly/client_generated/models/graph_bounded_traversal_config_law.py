from enum import Enum


class GraphBoundedTraversalConfigLaw(str, Enum):
    PROVENANCE_SEMIRING = "provenance_semiring"

    def __str__(self) -> str:
        return str(self.value)
