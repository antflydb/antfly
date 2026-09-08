from enum import StrEnum


class GraphBoundedTraversalConfigLaw(StrEnum):
    PROVENANCE_SEMIRING = "provenance_semiring"

    def __str__(self) -> str:
        return str(self.value)
